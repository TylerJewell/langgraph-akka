package io.akka.langgraph.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 R25–R29. */
class ResumePlanTest {

  private static PendingWrite write(String taskId, String channel, Object value) {
    return new PendingWrite(taskId, 0, channel, value, "");
  }

  @Test
  void aTaskWithOrdinaryWritesIsRestoredAndNotRerun() {
    ResumePlan plan =
        ResumePlan.from(
            List.of("a", "b"), List.of(write("a", "log", "done"), write("a", "count", 1)));

    assertThat(plan.restored()).containsOnlyKeys("a");
    assertThat(plan.restored().get("a")).hasSize(2);
    assertThat(plan.rerun()).containsExactly("b");
  }

  @Test
  void everyControlChannelAloneLeavesTheTaskToRerun() {
    // Enumerated, not sampled: the rule names four channels and skipping one would be invisible
    // in the state a run ends at, which is the same either way.
    for (String control :
        List.of(
            ControlChannels.ERROR,
            ControlChannels.ERROR_SOURCE_NODE,
            ControlChannels.INTERRUPT,
            ControlChannels.RESUME)) {
      ResumePlan plan = ResumePlan.from(List.of("a"), List.of(write("a", control, "x")));

      assertThat(plan.rerun()).as("control channel %s", control).containsExactly("a");
      assertThat(plan.restored()).as("control channel %s", control).isEmpty();
    }
  }

  @Test
  void controlChannelsAreNotHandedBackAlongsideOrdinaryOnes() {
    ResumePlan plan =
        ResumePlan.from(
            List.of("a"),
            List.of(
                write("a", "log", "done"),
                write("a", ControlChannels.INTERRUPT, "asked"),
                write("a", ControlChannels.RESUME, "answered")));

    assertThat(plan.rerun()).isEmpty();
    assertThat(plan.restored().get("a"))
        .containsExactly(new PendingWrite("a", 0, "log", "done", ""));
  }

  @Test
  void aTaskTheWritesDoNotNameIsRerun() {
    ResumePlan plan = ResumePlan.from(List.of("a", "b", "c"), List.of(write("a", "log", "x")));

    assertThat(plan.rerun()).containsExactly("b", "c");
  }

  @Test
  void aCheckpointWithNoWritesRerunsEverything() {
    ResumePlan plan = ResumePlan.from(List.of("a", "b", "c"), List.of());

    assertThat(plan.rerun()).containsExactly("a", "b", "c");
    assertThat(plan.restored()).isEmpty();
  }

  @Test
  void writesNamingATaskTheCallerIsNotRunningAreIgnored() {
    ResumePlan plan = ResumePlan.from(List.of("a"), List.of(write("z", "log", "stale")));

    assertThat(plan.rerun()).containsExactly("a");
    assertThat(plan.restored()).isEmpty();
  }

  @Test
  void rerunOrderIsTheOrderTheCallerGave() {
    ResumePlan plan = ResumePlan.from(List.of("c", "a", "b"), List.of());

    assertThat(plan.rerun()).containsExactly("c", "a", "b");
  }

  @Test
  void restoredWritesKeepTheOrderTheyWereRecordedIn() {
    ResumePlan plan =
        ResumePlan.from(
            List.of("a"), List.of(write("a", "one", 1), write("a", "two", 2), write("a", "three", 3)));

    assertThat(plan.restored().get("a").stream().map(PendingWrite::channel).toList())
        .containsExactly("one", "two", "three");
  }

  @Test
  void theThreeNodeChainOfQuestionLogRow9() {
    // First pass: `a` finished and wrote; `b` interrupted. The plan for the resume must skip `a`,
    // run `b` from its beginning, and run `c`, which has never run. This is the trace the source
    // produces — `b-enter, b-resumed, c` — expressed as a decision rather than as an outcome,
    // because the state both passes end at is identical and would not distinguish them.
    List<PendingWrite> pending =
        List.of(write("a", "log", "a"), write("b", ControlChannels.INTERRUPT, "need input"));

    ResumePlan plan = ResumePlan.from(List.of("a", "b", "c"), pending);

    assertThat(plan.restored()).containsOnlyKeys("a");
    assertThat(plan.rerun()).containsExactly("b", "c");
  }
}
