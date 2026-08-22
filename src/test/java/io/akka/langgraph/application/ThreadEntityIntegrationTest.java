package io.akka.langgraph.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.langgraph.domain.ChannelVersion;
import io.akka.langgraph.domain.Checkpoint;
import io.akka.langgraph.domain.CheckpointId;
import io.akka.langgraph.domain.CheckpointMetadata;
import io.akka.langgraph.domain.CheckpointStore.ChannelWrite;
import io.akka.langgraph.domain.CheckpointTuple;
import io.akka.langgraph.domain.ControlChannels;
import io.akka.langgraph.domain.ListQuery;
import io.akka.langgraph.domain.ResumePlan;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The same rules as the unit tests, driven through a started runtime: state that only replays
 * correctly in memory is state that has not been checked, because the entity's own path from
 * command to event to state is where a shared mutable collection would show up and a unit test
 * calling {@link io.akka.langgraph.domain.CheckpointStore} directly would not.
 */
public class ThreadEntityIntegrationTest extends TestKitSupport {

  private String newThread() {
    return "thread-" + UUID.randomUUID();
  }

  private String put(String threadId, String namespace, String parentId, int step, Object value) {
    String version = ChannelVersion.first();
    Checkpoint checkpoint =
        new Checkpoint(
            Checkpoint.FORMAT_VERSION,
            CheckpointId.next(),
            Instant.now().toString(),
            Map.of("a", version),
            Map.of(),
            null);
    return componentClient
        .forEventSourcedEntity(threadId)
        .method(ThreadEntity::put)
        .invoke(
            new ThreadEntity.PutCommand(
                namespace,
                checkpoint,
                new CheckpointMetadata(step == 0 ? "input" : "loop", step, Map.of()),
                parentId,
                Map.of("a", version),
                Map.of("a", value)));
  }

  private List<String> chain(String threadId, String namespace, int steps) {
    List<String> ids = new ArrayList<>();
    String parent = null;
    for (int step = 0; step < steps; step++) {
      parent = put(threadId, namespace, parent, step, "value-" + step);
      ids.add(parent);
    }
    return ids;
  }

  private ThreadEntity.MaybeCheckpoint find(
      String threadId, String namespace, String checkpointId) {
    return componentClient
        .forEventSourcedEntity(threadId)
        .method(ThreadEntity::get)
        .invoke(new ThreadEntity.GetCommand(namespace, checkpointId));
  }

  private CheckpointTuple get(String threadId, String namespace, String checkpointId) {
    ThreadEntity.MaybeCheckpoint found = find(threadId, namespace, checkpointId);
    assertThat(found.found()).isTrue();
    return found.tuple();
  }

  @Test
  void aChainSurvivesAndReadsBackNewestFirst() {
    String threadId = newThread();
    List<String> ids = chain(threadId, "", 4);

    assertThat(get(threadId, "", null).checkpoint().id()).isEqualTo(ids.get(3));
    assertThat(get(threadId, "", null).parentId()).isEqualTo(ids.get(2));
    assertThat(get(threadId, "", ids.get(0)).parentId()).isNull();

    List<CheckpointTuple> listed =
        componentClient
            .forEventSourcedEntity(threadId)
            .method(ThreadEntity::list)
            .invoke(ListQuery.all(""));
    assertThat(listed.stream().map(t -> t.checkpoint().id()).toList())
        .containsExactly(ids.get(3), ids.get(2), ids.get(1), ids.get(0));
  }

  @Test
  void arbitraryJsonSurvivesAsAChannelValue() {
    String threadId = newThread();
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("str", "hello");
    nested.put("int", 42);
    nested.put("bool", true);
    nested.put("list", List.of(1, "two", false));
    nested.put("map", Map.of("inner", List.of(Map.of("deep", 1))));

    put(threadId, "", null, 0, nested);

    Object back = get(threadId, "", null).channelValues().get("a");
    assertThat(back).isInstanceOf(Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> asMap = (Map<String, Object>) back;
    assertThat(asMap.get("str")).isEqualTo("hello");
    assertThat(asMap.get("int")).isEqualTo(42);
    assertThat(asMap.get("bool")).isEqualTo(true);
    assertThat(asMap.get("list")).isEqualTo(List.of(1, "two", false));
  }

  @Test
  void theRepeatWriteRuleHoldsThroughTheRuntime() {
    String threadId = newThread();
    String checkpointId = chain(threadId, "", 1).get(0);

    record(threadId, checkpointId, "task-1", new ChannelWrite("chan", "first"));
    record(threadId, checkpointId, "task-1", new ChannelWrite("chan", "second"));
    assertThat(get(threadId, "", checkpointId).pendingWrites().get(0).value()).isEqualTo("first");

    for (String control :
        List.of(
            ControlChannels.ERROR,
            ControlChannels.SCHEDULED,
            ControlChannels.INTERRUPT,
            ControlChannels.RESUME)) {
      record(threadId, checkpointId, "task-2", new ChannelWrite(control, "first"));
      record(threadId, checkpointId, "task-2", new ChannelWrite(control, "second"));
      assertThat(
              get(threadId, "", checkpointId).pendingWrites().stream()
                  .filter(w -> w.taskId().equals("task-2") && w.channel().equals(control))
                  .map(w -> w.value())
                  .toList())
          .as("control channel %s", control)
          .containsExactly("second");
    }
  }

  @Test
  void writesAgainstAnAbsentCheckpointAreRefusedAndLeaveNothingBehind() {
    String threadId = newThread();
    String checkpointId = chain(threadId, "", 1).get(0);

    assertThatThrownBy(
            () -> record(threadId, "no-such-id", "task-1", new ChannelWrite("chan", "v")))
        .isInstanceOf(RuntimeException.class);

    // the refusal persisted no event, so the checkpoint that does exist is untouched
    assertThat(get(threadId, "", checkpointId).pendingWrites()).isEmpty();
  }

  @Test
  void aResumePlanIsComputedFromWhatTheThreadHolds() {
    String threadId = newThread();
    String checkpointId = chain(threadId, "", 1).get(0);
    record(threadId, checkpointId, "a", new ChannelWrite("log", "a"));
    record(threadId, checkpointId, "b", new ChannelWrite(ControlChannels.INTERRUPT, "need input"));

    ResumePlan plan =
        componentClient
            .forEventSourcedEntity(threadId)
            .method(ThreadEntity::resumePlan)
            .invoke(new ThreadEntity.ResumeCommand("", checkpointId, List.of("a", "b", "c")));

    assertThat(plan.restored()).containsOnlyKeys("a");
    assertThat(plan.rerun()).containsExactly("b", "c");
  }

  @Test
  void namespacesAreIndependentInsideOneEntity() {
    String threadId = newThread();
    List<String> root = chain(threadId, "", 3);
    List<String> child = chain(threadId, "child:1", 2);

    assertThat(get(threadId, "", null).checkpoint().id()).isEqualTo(root.get(2));
    assertThat(get(threadId, "child:1", null).checkpoint().id()).isEqualTo(child.get(1));
  }

  @Test
  void pruningKeepsTheLatestOfEachNamespace() {
    String threadId = newThread();
    List<String> root = chain(threadId, "", 3);
    List<String> child = chain(threadId, "child:1", 2);

    componentClient
        .forEventSourcedEntity(threadId)
        .method(ThreadEntity::prune)
        .invoke("KEEP_LATEST");

    assertThat(get(threadId, "", null).checkpoint().id()).isEqualTo(root.get(2));
    assertThat(get(threadId, "child:1", null).checkpoint().id()).isEqualTo(child.get(1));
    assertThat(
            componentClient
                .forEventSourcedEntity(threadId)
                .method(ThreadEntity::list)
                .invoke(ListQuery.all("")))
        .hasSize(1);
  }

  @Test
  void deletingAThreadEmptiesItAndLeavesOthersAlone() {
    String one = newThread();
    String two = newThread();
    chain(one, "", 2);
    chain(two, "", 2);

    componentClient.forEventSourcedEntity(one).method(ThreadEntity::delete).invoke();

    assertThat(find(one, "", null).found()).isFalse();
    assertThat(find(two, "", null).found()).isTrue();
  }

  @Test
  void concurrentSaversToOneThreadAllSurviveAndTheHistoryStaysOrdered() {
    // The published README states that this port orders concurrent saves to one thread and
    // langgraph's own store does not promise to. Both halves are run rather than reasoned
    // about; the other half is probes/probe_03_concurrency.py in the harness.
    int callers = 4;
    int savesEach = 25;
    String threadId = newThread();

    ExecutorService pool = Executors.newFixedThreadPool(callers);
    CountDownLatch ready = new CountDownLatch(callers);
    CountDownLatch go = new CountDownLatch(1);
    try {
      for (int caller = 0; caller < callers; caller++) {
        int c = caller;
        pool.submit(
            () -> {
              ready.countDown();
              go.await();
              for (int n = 0; n < savesEach; n++) {
                put(threadId, "", null, n, "caller-" + c + "-save-" + n);
              }
              return null;
            });
      }
      ready.await(30, TimeUnit.SECONDS);
      go.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }

    List<CheckpointTuple> listed =
        componentClient
            .forEventSourcedEntity(threadId)
            .method(ThreadEntity::list)
            .invoke(ListQuery.all(""));

    List<String> ids = listed.stream().map(t -> t.checkpoint().id()).toList();
    assertThat(ids).hasSize(callers * savesEach);
    assertThat(ids).doesNotHaveDuplicates();
    assertThat(ids).isSortedAccordingTo(Comparator.reverseOrder());
    assertThat(listed).allSatisfy(t -> assertThat(t.channelValues()).containsKey("a"));
  }

  @Test
  void aThreadReplaysToTheSameStateFromItsEventsAlone() {
    // A second entity id fed the identical commands must reach the identical state; if the
    // handlers shared a collection with the state they built, the two would diverge here and
    // nowhere else.
    String threadId = newThread();
    List<String> ids = chain(threadId, "", 5);
    record(threadId, ids.get(4), "task-1", new ChannelWrite("chan", "v"));

    var before =
        componentClient.forEventSourcedEntity(threadId).method(ThreadEntity::state).invoke();
    var after =
        componentClient.forEventSourcedEntity(threadId).method(ThreadEntity::state).invoke();
    assertThat(after).isEqualTo(before);
    assertThat(after.checkpointCount()).isEqualTo(5);
  }

  private void record(String threadId, String checkpointId, String taskId, ChannelWrite... writes) {
    componentClient
        .forEventSourcedEntity(threadId)
        .method(ThreadEntity::putWrites)
        .invoke(
            new ThreadEntity.PutWritesCommand(
                "", checkpointId, taskId, "", List.of(writes)));
  }
}
