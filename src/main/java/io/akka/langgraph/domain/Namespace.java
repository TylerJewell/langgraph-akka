package io.akka.langgraph.domain;

import java.util.List;
import java.util.Map;

/**
 * One chain of checkpoints under a thread (SPEC-001 §2). Two namespaces of the same thread are
 * independent: they have their own latest checkpoint and neither is visible from a listing of the
 * other (R17).
 *
 * @param checkpoints checkpoint id to what is stored under it; unordered here, sorted on read,
 *     because the id is what carries the order
 * @param writes checkpoint id to the writes recorded against it, in the order they were recorded
 * @param values channel name to version to the value stored at that version
 */
public record Namespace(
    Map<String, StoredCheckpoint> checkpoints,
    Map<String, List<PendingWrite>> writes,
    Map<String, Map<String, ChannelValue>> values) {

  public static Namespace empty() {
    return new Namespace(Map.of(), Map.of(), Map.of());
  }

  public Namespace {
    checkpoints = checkpoints == null ? Map.of() : Map.copyOf(checkpoints);
    writes = writes == null ? Map.of() : Map.copyOf(writes);
    // Copied a level down: a caller keeping a reference to one channel's version map could
    // otherwise change what a stored namespace holds.
    if (values == null) {
      values = Map.of();
    } else {
      Map<String, Map<String, ChannelValue>> frozen = new java.util.LinkedHashMap<>();
      values.forEach((channel, byVersion) -> frozen.put(channel, Map.copyOf(byVersion)));
      values = Map.copyOf(frozen);
    }
  }

  /** The greatest checkpoint id stored here, or null where nothing is (R11). */
  public String latestId() {
    String latest = null;
    for (String id : checkpoints.keySet()) {
      if (latest == null || id.compareTo(latest) > 0) {
        latest = id;
      }
    }
    return latest;
  }

  public List<PendingWrite> writesFor(String checkpointId) {
    return writes.getOrDefault(checkpointId, List.of());
  }
}
