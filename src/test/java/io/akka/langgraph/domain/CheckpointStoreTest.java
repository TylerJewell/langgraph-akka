package io.akka.langgraph.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.akka.langgraph.domain.CheckpointStore.PutResult;
import java.time.Instant;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 R1–R5, R11–R17, R23. */
class CheckpointStoreTest {

  private static final int NO_PRUNING = Integer.MAX_VALUE;

  static Checkpoint checkpoint(Map<String, String> channelVersions) {
    return new Checkpoint(
        Checkpoint.FORMAT_VERSION,
        CheckpointId.next(),
        Instant.EPOCH.toString(),
        channelVersions,
        Map.of(),
        null);
  }

  static CheckpointMetadata meta(String source, int step) {
    return new CheckpointMetadata(source, step, Map.of());
  }

  /** Writes {@code steps} checkpoints in a chain and returns their ids, oldest first. */
  static List<String> chain(ThreadState[] holder, String namespace, int steps) {
    List<String> ids = new ArrayList<>();
    String parent = null;
    for (int step = 0; step < steps; step++) {
      String version = ChannelVersion.first();
      PutResult result =
          CheckpointStore.put(
              holder[0],
              namespace,
              checkpoint(Map.of("a", version)),
              meta(step == 0 ? "input" : "loop", step),
              parent,
              Map.of("a", version),
              Map.of("a", "value-" + step),
              NO_PRUNING);
      holder[0] = result.state();
      parent = result.checkpointId();
      ids.add(parent);
    }
    return ids;
  }

  @Test
  void parentIsTheIdTheCallerSupplied() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> ids = chain(state, "", 3);

    assertThat(CheckpointStore.get(state[0], "", ids.get(0)).parentId()).isNull();
    assertThat(CheckpointStore.get(state[0], "", ids.get(1)).parentId()).isEqualTo(ids.get(0));
    assertThat(CheckpointStore.get(state[0], "", ids.get(2)).parentId()).isEqualTo(ids.get(1));
  }

  @Test
  void channelValuesAreStoredOncePerVersion() {
    ThreadState state = new ThreadState("t", Map.of());

    String v1 = ChannelVersion.first();
    PutResult first =
        CheckpointStore.put(
            state,
            "",
            checkpoint(Map.of("a", v1, "b", v1)),
            meta("loop", 0),
            null,
            Map.of("a", v1, "b", v1),
            Map.of("a", "A1", "b", "B1"),
            NO_PRUNING);

    // second step bumps only `a`; `b` keeps its version and its one stored value
    String v2 = ChannelVersion.next(v1);
    PutResult second =
        CheckpointStore.put(
            first.state(),
            "",
            checkpoint(Map.of("a", v2, "b", v1)),
            meta("loop", 1),
            first.checkpointId(),
            Map.of("a", v2),
            Map.of("a", "A2", "b", "B1"),
            NO_PRUNING);

    CheckpointTuple tuple = CheckpointStore.get(second.state(), "", null);
    assertThat(tuple.channelValues()).isEqualTo(Map.of("a", "A2", "b", "B1"));

    Namespace ns = second.state().namespace("");
    assertThat(ns.values().get("a")).hasSize(2);
    assertThat(ns.values().get("b")).hasSize(1);
  }

  @Test
  void aVersionDeclaredWithNoValueIsOmittedOnRead() {
    ThreadState state = new ThreadState("t", Map.of());
    String v = ChannelVersion.first();
    PutResult result =
        CheckpointStore.put(
            state,
            "",
            checkpoint(Map.of("a", v, "gone", v)),
            meta("loop", 0),
            null,
            Map.of("a", v, "gone", v),
            Map.of("a", "A"),
            NO_PRUNING);

    CheckpointTuple tuple = CheckpointStore.get(result.state(), "", null);
    assertThat(tuple.channelValues()).containsOnlyKeys("a");
    assertThat(tuple.checkpoint().channelVersions()).containsKeys("a", "gone");
    assertThat(result.state().namespace("").values().get("gone").get(v).empty()).isTrue();
  }

  @Test
  void rewritingAChannelVersionReplacesTheValueInPlace() {
    ThreadState state = new ThreadState("t", Map.of());
    String v = ChannelVersion.first();
    PutResult first =
        CheckpointStore.put(
            state, "", checkpoint(Map.of("a", v)), meta("loop", 0), null,
            Map.of("a", v), Map.of("a", "first"), NO_PRUNING);
    PutResult second =
        CheckpointStore.put(
            first.state(), "", checkpoint(Map.of("a", v)), meta("loop", 1), first.checkpointId(),
            Map.of("a", v), Map.of("a", "second"), NO_PRUNING);

    assertThat(second.state().namespace("").values().get("a")).hasSize(1);
    assertThat(CheckpointStore.get(second.state(), "", null).channelValues())
        .isEqualTo(Map.of("a", "second"));
  }

  @Test
  void writingTheSameIdTwiceReplacesIt() {
    ThreadState state = new ThreadState("t", Map.of());
    String v = ChannelVersion.first();
    Checkpoint sameCheckpoint = checkpoint(Map.of("a", v));

    PutResult first =
        CheckpointStore.put(
            state, "", sameCheckpoint, meta("loop", 0), null, Map.of("a", v),
            Map.of("a", "A"), NO_PRUNING);
    PutResult second =
        CheckpointStore.put(
            first.state(), "", sameCheckpoint, meta("loop", 7), null, Map.of("a", v),
            Map.of("a", "A"), NO_PRUNING);

    assertThat(second.state().namespace("").checkpoints()).hasSize(1);
    assertThat(CheckpointStore.get(second.state(), "", null).metadata().step()).isEqualTo(7);
  }

  @Test
  void readingTheLatestAndReadingByIdAndReadingNothing() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> ids = chain(state, "", 3);

    assertThat(CheckpointStore.get(state[0], "", null).checkpoint().id()).isEqualTo(ids.get(2));
    assertThat(CheckpointStore.get(state[0], "", ids.get(1)).checkpoint().id())
        .isEqualTo(ids.get(1));
    assertThat(CheckpointStore.get(state[0], "", "no-such-id")).isNull();
    assertThat(CheckpointStore.get(state[0], "other-namespace", null)).isNull();
    assertThat(CheckpointStore.get(ThreadState.empty(), "", null)).isNull();
  }

  @Test
  void listingIsNewestFirstAndFiltersConjunctively() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> ids = chain(state, "", 5);
    List<String> newestFirst = new ArrayList<>(ids);
    java.util.Collections.reverse(newestFirst);

    assertThat(idsOf(CheckpointStore.list(state[0], ListQuery.all(""))))
        .containsExactlyElementsOf(newestFirst);

    // `before` is strict: the named checkpoint is excluded along with everything after it
    assertThat(idsOf(CheckpointStore.list(state[0], new ListQuery("", ids.get(2), null, null))))
        .containsExactly(ids.get(1), ids.get(0));

    // every filter key must match
    assertThat(
            CheckpointStore.list(state[0], new ListQuery("", null, Map.of("source", "loop"), null))
                .stream()
                .map(t -> t.metadata().step())
                .toList())
        .containsExactly(4, 3, 2, 1);
    assertThat(
            CheckpointStore.list(
                state[0], new ListQuery("", null, Map.of("source", "loop", "step", 99), null)))
        .isEmpty();

    // a key the metadata does not carry excludes the row
    assertThat(
            CheckpointStore.list(
                state[0], new ListQuery("", null, Map.of("run_id", "anything"), null)))
        .isEmpty();

    // the limit caps after filtering, not before
    assertThat(idsOf(CheckpointStore.list(state[0], new ListQuery("", null, null, 2))))
        .containsExactly(ids.get(4), ids.get(3));
    assertThat(
            CheckpointStore.list(
                    state[0], new ListQuery("", null, Map.of("source", "loop"), 2))
                .stream()
                .map(t -> t.metadata().step())
                .toList())
        .containsExactly(4, 3);

    assertThat(CheckpointStore.list(state[0], ListQuery.all("unknown-namespace"))).isEmpty();
    assertThat(CheckpointStore.list(ThreadState.empty(), ListQuery.all(""))).isEmpty();
  }

  @Test
  void namespacesAreIndependentChainsOfOneThread() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> root = chain(state, "", 3);
    List<String> child = chain(state, "child:1", 2);

    assertThat(CheckpointStore.get(state[0], "", null).checkpoint().id()).isEqualTo(root.get(2));
    assertThat(CheckpointStore.get(state[0], "child:1", null).checkpoint().id())
        .isEqualTo(child.get(1));
    assertThat(idsOf(CheckpointStore.list(state[0], ListQuery.all("")))).hasSize(3);
    assertThat(idsOf(CheckpointStore.list(state[0], ListQuery.all("child:1")))).hasSize(2);
  }

  @Test
  void deletingAThreadTakesOnlyThatThread() {
    ThreadState[] one = {new ThreadState("t1", Map.of())};
    ThreadState[] two = {new ThreadState("t2", Map.of())};
    chain(one, "", 3);
    chain(one, "child", 1);
    chain(two, "", 2);

    ThreadState emptied = CheckpointStore.deleteThread(one[0]);
    assertThat(emptied.isEmpty()).isTrue();
    assertThat(CheckpointStore.get(emptied, "", null)).isNull();
    assertThat(CheckpointStore.get(emptied, "child", null)).isNull();
    assertThat(CheckpointStore.get(two[0], "", null)).isNotNull();
  }

  @Test
  void aReadCarriesItsPendingWrites() {
    ThreadState[] state = {new ThreadState("t", Map.of())};
    List<String> ids = chain(state, "", 2);
    state[0] =
        CheckpointStore.putWrites(
            state[0],
            "",
            ids.get(1),
            "task-1",
            "",
            List.of(new CheckpointStore.ChannelWrite("ch", "val")));

    assertThat(CheckpointStore.get(state[0], "", null).pendingWrites())
        .containsExactly(new PendingWrite("task-1", 0, "ch", "val", ""));
    assertThat(CheckpointStore.get(state[0], "", ids.get(0)).pendingWrites()).isEmpty();
  }

  @Test
  void aWriteReturnsTheIdToThreadIntoTheNext() {
    ThreadState state = new ThreadState("t", Map.of());
    Checkpoint c = checkpoint(Map.of());
    PutResult result =
        CheckpointStore.put(
            state, "", c, meta("loop", 0), null, Map.of(), Map.of(), NO_PRUNING);
    assertThat(result.checkpointId()).isEqualTo(c.id());
  }

  @Test
  void aCheckpointIdIsTimeOrderedFixedWidthAndDistinct() {
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      ids.add(CheckpointId.next());
    }
    List<String> sorted = new ArrayList<>(ids);
    java.util.Collections.sort(sorted);

    assertThat(ids).isEqualTo(sorted);
    assertThat(ids).doesNotHaveDuplicates();
    assertThat(ids.stream().map(String::length).distinct().toList()).containsExactly(36);
  }

  private static List<String> idsOf(List<CheckpointTuple> tuples) {
    return tuples.stream().map(t -> t.checkpoint().id()).toList();
  }
}
