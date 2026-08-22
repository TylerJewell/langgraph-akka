package io.akka.langgraph.domain;

import static io.akka.langgraph.domain.CheckpointStoreTest.chain;
import static io.akka.langgraph.domain.CheckpointStoreTest.checkpoint;
import static io.akka.langgraph.domain.CheckpointStoreTest.meta;
import static org.assertj.core.api.Assertions.assertThat;

import io.akka.langgraph.domain.CheckpointStore.ChannelWrite;
import io.akka.langgraph.domain.CheckpointStore.PruneStrategy;
import io.akka.langgraph.domain.CheckpointStore.PutResult;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 R20–R22 and R24.
 *
 * <p>The seven assertions below are transcribed one for one from the source's own conformance
 * suite, `langgraph-src/libs/checkpoint-conformance/langgraph/checkpoint/conformance/spec/
 * test_prune.py:34-185`, which is where the rule lives: no saver in the source tree implements
 * `prune`, so there is no running implementation to compare against (question-log rows 14, 18).
 * The eighth test is the port's own retention trigger, which the source does not have.
 */
class PruneTest {

  @Test
  void keepLatestLeavesOnlyTheLatestCheckpoint() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> ids = chain(state, "", 4);

    ThreadState pruned = CheckpointStore.prune(state[0], PruneStrategy.KEEP_LATEST);

    List<CheckpointTuple> listed = CheckpointStore.list(pruned, ListQuery.all(""));
    assertThat(listed).hasSize(1);
    assertThat(listed.get(0).checkpoint().id()).isEqualTo(ids.get(3));
  }

  @Test
  void keepLatestLeavesEachNamespaceItsOwnLatest() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> root = chain(state, "", 3);
    List<String> child = chain(state, "child:1", 2);

    ThreadState pruned = CheckpointStore.prune(state[0], PruneStrategy.KEEP_LATEST);

    for (Map.Entry<String, String> expected :
        Map.of("", root.get(2), "child:1", child.get(1)).entrySet()) {
      List<CheckpointTuple> listed =
          CheckpointStore.list(pruned, ListQuery.all(expected.getKey()));
      assertThat(listed).as("namespace [%s]", expected.getKey()).hasSize(1);
      assertThat(listed.get(0).checkpoint().id()).isEqualTo(expected.getValue());
    }
  }

  @Test
  void keepLatestPreservesTheKeptCheckpointsWrites() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> ids = chain(state, "", 3);
    state[0] =
        CheckpointStore.putWrites(
            state[0], "", ids.get(2), "task-1", "", List.of(new ChannelWrite("ch", "val")));

    ThreadState pruned = CheckpointStore.prune(state[0], PruneStrategy.KEEP_LATEST);

    CheckpointTuple latest = CheckpointStore.get(pruned, "", null);
    assertThat(latest.pendingWrites()).hasSize(1);
    assertThat(latest.pendingWrites().get(0).channel()).isEqualTo("ch");
    assertThat(latest.pendingWrites().get(0).value()).isEqualTo("val");
  }

  @Test
  void keepLatestDropsAPrunedCheckpointsWrites() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> ids = chain(state, "", 3);
    state[0] =
        CheckpointStore.putWrites(
            state[0], "", ids.get(0), "task-1", "", List.of(new ChannelWrite("ch", "gone")));

    ThreadState pruned = CheckpointStore.prune(state[0], PruneStrategy.KEEP_LATEST);

    assertThat(pruned.namespace("").writes()).doesNotContainKey(ids.get(0));
  }

  @Test
  void theKeptCheckpointKeepsItsOwnChannelValuesAndLosesTheRest() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    chain(state, "", 3);
    Map<String, Object> before = CheckpointStore.get(state[0], "", null).channelValues();

    ThreadState pruned = CheckpointStore.prune(state[0], PruneStrategy.KEEP_LATEST);

    assertThat(CheckpointStore.get(pruned, "", null).channelValues()).isEqualTo(before);
    assertThat(pruned.namespace("").values().get("a")).hasSize(1);
  }

  @Test
  void theKeptCheckpointBecomesTheStartOfItsChain() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    chain(state, "", 3);
    assertThat(CheckpointStore.get(state[0], "", null).parentId()).isNotNull();

    ThreadState pruned = CheckpointStore.prune(state[0], PruneStrategy.KEEP_LATEST);

    assertThat(CheckpointStore.get(pruned, "", null).parentId()).isNull();
  }

  @Test
  void deleteRemovesEverything() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    chain(state, "", 3);
    chain(state, "child", 2);

    ThreadState pruned = CheckpointStore.prune(state[0], PruneStrategy.DELETE);

    assertThat(pruned.isEmpty()).isTrue();
    assertThat(CheckpointStore.get(pruned, "", null)).isNull();
    assertThat(CheckpointStore.get(pruned, "child", null)).isNull();
  }

  @Test
  void pruningAThreadThatHoldsNothingIsANoOp() {
    ThreadState empty = new ThreadState("t", Map.of());

    assertThat(CheckpointStore.prune(empty, PruneStrategy.KEEP_LATEST).isEmpty()).isTrue();
    assertThat(CheckpointStore.prune(empty, PruneStrategy.DELETE).isEmpty()).isTrue();
    assertThat(CheckpointStore.prune(ThreadState.empty(), PruneStrategy.KEEP_LATEST).isEmpty())
        .isTrue();
  }

  @Test
  void crossingTheThresholdPrunesAutomatically() {
    // The port's own trigger, not the source's: the source specifies `prune` but never calls it,
    // and the target stops an entity at 10 MiB of accumulated events (question-log rows 18, T4).
    // SPEC-001 §4 OD3.
    ThreadState state = new ThreadState("t", Map.of());
    String parent = null;
    List<String> ids = new java.util.ArrayList<>();
    for (int step = 0; step < 6; step++) {
      String version = ChannelVersion.first();
      PutResult result =
          CheckpointStore.put(
              state,
              "",
              checkpoint(Map.of("a", version)),
              meta("loop", step),
              parent,
              Map.of("a", version),
              Map.of("a", "value-" + step),
              3);
      state = result.state();
      parent = result.checkpointId();
      ids.add(parent);
      assertThat(state.namespace("").checkpoints().size())
          .as("after step %s", step)
          .isLessThanOrEqualTo(3);
    }

    // Pruning fires on the write that crosses the threshold, so what is left afterwards is that
    // write's own checkpoint plus whatever has been added since — never more than the threshold,
    // and always with the newest checkpoint among them.
    assertThat(CheckpointStore.list(state, ListQuery.all(""))).hasSize(3);
    assertThat(CheckpointStore.get(state, "", null).checkpoint().id()).isEqualTo(ids.get(5));
    assertThat(idsOf(CheckpointStore.list(state, ListQuery.all(""))))
        .containsExactly(ids.get(5), ids.get(4), ids.get(3));
  }

  private static List<String> idsOf(List<CheckpointTuple> tuples) {
    return tuples.stream().map(t -> t.checkpoint().id()).toList();
  }

  @Test
  void stayingUnderTheThresholdPrunesNothing() {
    ThreadState state = new ThreadState("t", Map.of());
    String parent = null;
    for (int step = 0; step < 3; step++) {
      String version = ChannelVersion.first();
      PutResult result =
          CheckpointStore.put(
              state,
              "",
              checkpoint(Map.of("a", version)),
              meta("loop", step),
              parent,
              Map.of("a", version),
              Map.of("a", "value-" + step),
              3);
      state = result.state();
      parent = result.checkpointId();
    }
    assertThat(CheckpointStore.list(state, ListQuery.all(""))).hasSize(3);
  }
}
