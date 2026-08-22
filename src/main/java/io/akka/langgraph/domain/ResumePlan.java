package io.akka.langgraph.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Which tasks a resumed run replays and which it skips (SPEC-001 R25–R29).
 *
 * <p>The decision is made entirely from the writes a checkpoint holds. A task whose ordinary
 * channel writes are there finished, so its writes are handed back and it is not run again; a task
 * named only by an error, an interrupt or a resume marker did not finish, so it is run — from its
 * beginning, because that is the only thing the record supports (R28). A task the writes do not
 * mention at all has never run.
 *
 * <p>The distinction is invisible in the state a run ends at: both a replayed and a skipped task
 * leave the same values behind. What separates them is which node bodies executed, which is why the
 * conformance test for this compares a trace rather than a result.
 *
 * @param restored task id to the writes handed back to it, in the order they were recorded
 * @param rerun task ids to execute, in the order they were given
 */
public record ResumePlan(Map<String, List<PendingWrite>> restored, List<String> rerun) {

  public ResumePlan {
    restored = Map.copyOf(restored);
    rerun = List.copyOf(rerun);
  }

  /**
   * @param taskIds the tasks the caller is about to run, in the order it would run them
   * @param pendingWrites everything recorded against the checkpoint being resumed from
   */
  public static ResumePlan from(List<String> taskIds, List<PendingWrite> pendingWrites) {
    Map<String, List<PendingWrite>> restorable = new LinkedHashMap<>();
    for (PendingWrite write : pendingWrites) {
      if (ControlChannels.isRestoredOnResume(write.channel())) {
        restorable.computeIfAbsent(write.taskId(), k -> new ArrayList<>()).add(write);
      }
    }

    Map<String, List<PendingWrite>> restored = new LinkedHashMap<>();
    List<String> rerun = new ArrayList<>();
    for (String taskId : taskIds) {
      List<PendingWrite> writes = restorable.get(taskId);
      if (writes == null || writes.isEmpty()) {
        rerun.add(taskId);
      } else {
        restored.put(taskId, List.copyOf(writes));
      }
    }
    return new ResumePlan(restored, rerun);
  }
}
