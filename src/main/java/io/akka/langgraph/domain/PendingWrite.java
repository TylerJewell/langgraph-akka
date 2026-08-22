package io.akka.langgraph.domain;

/**
 * One channel write a task produced, held against the checkpoint it was produced under
 * (SPEC-001 §2).
 *
 * @param taskId the task that produced it
 * @param index the write's position in the call that recorded it, or the control channel's fixed
 *     negative index — see {@link ControlChannels#indexFor}
 * @param channel the channel written
 * @param value the value written, JSON
 * @param taskPath where the task sat in the run, empty where the caller gave none
 */
public record PendingWrite(String taskId, int index, String channel, Object value, String taskPath) {

  public PendingWrite {
    taskPath = taskPath == null ? "" : taskPath;
  }

  /** Two writes are the same write when they share a task and an index (R6). */
  public boolean sameKeyAs(PendingWrite other) {
    return taskId.equals(other.taskId) && index == other.index;
  }
}
