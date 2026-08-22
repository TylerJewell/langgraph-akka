package io.akka.langgraph.domain;

import static io.akka.langgraph.domain.CheckpointStoreTest.chain;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.akka.langgraph.domain.CheckpointStore.ChannelWrite;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SPEC-001 R6–R10. */
class PendingWritesTest {

  private ThreadState[] state;
  private String checkpointId;

  private void aThreadWithOneCheckpoint() {
    state = new ThreadState[] {new ThreadState("t", Map.of())};
    checkpointId = chain(state, "", 1).get(0);
  }

  private void record(String taskId, ChannelWrite... writes) {
    state[0] =
        CheckpointStore.putWrites(state[0], "", checkpointId, taskId, "", List.of(writes));
  }

  private List<PendingWrite> stored() {
    return CheckpointStore.get(state[0], "", checkpointId).pendingWrites();
  }

  @Test
  void anOrdinaryRepeatWriteKeepsWhatIsAlreadyThere() {
    aThreadWithOneCheckpoint();
    record("task-1", new ChannelWrite("chan", "first"));
    record("task-1", new ChannelWrite("chan", "second"));

    assertThat(stored()).containsExactly(new PendingWrite("task-1", 0, "chan", "first", ""));
  }

  @Test
  void everyControlChannelRepeatOverwrites() {
    // Enumerated rather than sampled: the rule is about a class of four, and the guard that
    // implements it is one comparison, so a single example would not distinguish "all four"
    // from "the one that happened to be tried".
    for (String control :
        List.of(
            ControlChannels.ERROR,
            ControlChannels.SCHEDULED,
            ControlChannels.INTERRUPT,
            ControlChannels.RESUME)) {
      aThreadWithOneCheckpoint();
      record("task-1", new ChannelWrite(control, "first"));
      record("task-1", new ChannelWrite(control, "second"));

      assertThat(stored())
          .as("control channel %s", control)
          .containsExactly(
              new PendingWrite("task-1", ControlChannels.indexFor(control, 0), control, "second", ""));
    }
  }

  @Test
  void controlChannelsTakeTheirFixedNegativeIndex() {
    assertThat(ControlChannels.indexFor(ControlChannels.ERROR, 7)).isEqualTo(-1);
    assertThat(ControlChannels.indexFor(ControlChannels.SCHEDULED, 7)).isEqualTo(-2);
    assertThat(ControlChannels.indexFor(ControlChannels.INTERRUPT, 7)).isEqualTo(-3);
    assertThat(ControlChannels.indexFor(ControlChannels.RESUME, 7)).isEqualTo(-4);
    assertThat(ControlChannels.indexFor("anything-else", 7)).isEqualTo(7);
  }

  @Test
  void twoTasksAtTheSameIndexAreBothKept() {
    aThreadWithOneCheckpoint();
    record("task-1", new ChannelWrite("chan", "a"));
    record("task-2", new ChannelWrite("chan", "b"));

    assertThat(stored())
        .containsExactly(
            new PendingWrite("task-1", 0, "chan", "a", ""),
            new PendingWrite("task-2", 0, "chan", "b", ""));
  }

  @Test
  void theIndexIsThePositionInTheCall() {
    aThreadWithOneCheckpoint();
    record("task-1", new ChannelWrite("x", 1));
    // position 0 is taken and keeps its value; position 1 is free and is added
    record("task-1", new ChannelWrite("x", 9), new ChannelWrite("y", 2));

    assertThat(stored())
        .containsExactly(
            new PendingWrite("task-1", 0, "x", 1, ""),
            new PendingWrite("task-1", 1, "y", 2, ""));
  }

  @Test
  void aControlWriteAndAnOrdinaryWriteInOneCallDoNotShareAnIndex() {
    aThreadWithOneCheckpoint();
    record("task-1", new ChannelWrite(ControlChannels.ERROR, "boom"), new ChannelWrite("x", 1));

    assertThat(stored())
        .containsExactly(
            new PendingWrite("task-1", -1, ControlChannels.ERROR, "boom", ""),
            new PendingWrite("task-1", 1, "x", 1, ""));
  }

  @Test
  void writesAgainstAnAbsentCheckpointAreRefused() {
    aThreadWithOneCheckpoint();

    assertThatThrownBy(
            () ->
                CheckpointStore.putWrites(
                    state[0], "", "no-such-id", "task-1", "", List.of(new ChannelWrite("c", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no-such-id");

    assertThatThrownBy(
            () ->
                CheckpointStore.putWrites(
                    state[0],
                    "other-namespace",
                    checkpointId,
                    "task-1",
                    "",
                    List.of(new ChannelWrite("c", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("other-namespace");
  }

  @Test
  void theTaskPathIsCarriedThrough() {
    aThreadWithOneCheckpoint();
    state[0] =
        CheckpointStore.putWrites(
            state[0], "", checkpointId, "task-1", "PULL/node-a", List.of(new ChannelWrite("c", 1)));

    assertThat(stored()).singleElement().extracting(PendingWrite::taskPath).isEqualTo("PULL/node-a");
  }
}
