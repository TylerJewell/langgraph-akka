package io.akka.langgraph.domain;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The whole of SPEC-001 §3 as functions over a {@link ThreadState}. Nothing here touches a runtime,
 * a clock it did not receive, or a random source other than the two id mints — so every rule is
 * checkable in a unit test and the benchmark drives the same code the entity does.
 *
 * <p>Every method that changes something returns the new state rather than editing the old one: the
 * entity applies these inside {@code applyEvent}, where a state that shares structure with the one
 * before it is a state that replays differently on the second pass.
 */
public final class CheckpointStore {

  /**
   * How many checkpoints one namespace may hold before a write prunes it (R24, §4 OD3).
   *
   * <p>Sized against the target's 1 MB replication ceiling for a single entity state rather than
   * against the 10 MiB at which the runtime stops the entity: a state that crosses the smaller
   * figure stops replicating between regions while continuing to answer locally, which is the
   * failure that says nothing at the boundary.
   */
  public static final int DEFAULT_RETENTION_THRESHOLD = 128;

  public enum PruneStrategy {
    /** Retain, per namespace, only its greatest checkpoint id and that checkpoint's writes (R20). */
    KEEP_LATEST,
    /** Remove every checkpoint, write and stored value of the thread (R21). */
    DELETE
  }

  /** The outcome of a write: the new state and the id the caller threads into the next one (R2). */
  public record PutResult(ThreadState state, String checkpointId) {}

  private CheckpointStore() {}

  // ------------------------------------------------------------------ writing

  /**
   * Stores {@code checkpoint} under {@code namespace}, recording {@code parentId} as what it came
   * after, and writes a value for each channel named in {@code newVersions} (R1, R3, R5).
   *
   * @param parentId the checkpoint id the caller was working from, or null for the first
   * @param channelValues the values the caller holds; a channel named in {@code newVersions} but
   *     absent here is stored as the empty marker
   */
  public static PutResult put(
      ThreadState state,
      String namespace,
      Checkpoint checkpoint,
      CheckpointMetadata metadata,
      String parentId,
      Map<String, String> newVersions,
      Map<String, Object> channelValues,
      int retentionThreshold) {

    Namespace ns = state.namespace(namespace);

    // Only the channels this put names get a fresh inner map; the rest are carried across by
    // reference, which is safe because every inner map here is already immutable.
    Map<String, Map<String, ChannelValue>> values = new LinkedHashMap<>(ns.values());
    for (Map.Entry<String, String> entry : newVersions.entrySet()) {
      String channel = entry.getKey();
      String version = entry.getValue();
      ChannelValue stored =
          channelValues != null && channelValues.containsKey(channel)
              ? ChannelValue.of(channelValues.get(channel))
              : ChannelValue.emptyMarker();
      Map<String, ChannelValue> byVersion =
          new LinkedHashMap<>(values.getOrDefault(channel, Map.of()));
      byVersion.put(version, stored);
      values.put(channel, byVersion);
    }

    Map<String, StoredCheckpoint> checkpoints = new LinkedHashMap<>(ns.checkpoints());
    checkpoints.put(
        checkpoint.id(),
        new StoredCheckpoint(
            checkpoint, metadata == null ? CheckpointMetadata.empty() : metadata, parentId));

    ThreadState next =
        state.withNamespace(namespace, new Namespace(checkpoints, ns.writes(), values));

    if (checkpoints.size() > retentionThreshold) {
      next = prune(next, PruneStrategy.KEEP_LATEST);
    }
    return new PutResult(next, checkpoint.id());
  }

  /**
   * Records {@code writes} against {@code checkpointId}, applying the repeat rule (R6–R9).
   *
   * <p>A write's key is its task and its index, where the index is its position in this call except
   * for the four control channels, which take fixed negative ones. At or above zero, a key already
   * present keeps what is there; below zero, it is overwritten. That asymmetry is the whole of the
   * rule: it lets a task's ordinary output be recorded twice without doubling, while letting the
   * same task's error, interrupt or resume marker be replaced by a later one.
   *
   * @throws IllegalArgumentException where {@code checkpointId} names no stored checkpoint (R10)
   */
  public static ThreadState putWrites(
      ThreadState state,
      String namespace,
      String checkpointId,
      String taskId,
      String taskPath,
      List<ChannelWrite> writes) {

    checkWritesTarget(state, namespace, checkpointId);
    return applyWrites(state, namespace, checkpointId, taskId, taskPath, writes);
  }

  /**
   * Whether {@code checkpointId} is somewhere writes may be recorded (R10).
   *
   * @throws IllegalArgumentException where it is not
   */
  public static void checkWritesTarget(ThreadState state, String namespace, String checkpointId) {
    if (!state.namespace(namespace).checkpoints().containsKey(checkpointId)) {
      throw new IllegalArgumentException(
          "no checkpoint ["
              + checkpointId
              + "] in namespace ["
              + namespace
              + "] to record writes against");
    }
  }

  /**
   * Records the writes without checking where they are going. Separated from {@link #putWrites} so
   * that an entity can decide the refusal while handling the command and apply the event with
   * something that cannot throw: an event handler that can fail is an entity that cannot replay.
   */
  public static ThreadState applyWrites(
      ThreadState state,
      String namespace,
      String checkpointId,
      String taskId,
      String taskPath,
      List<ChannelWrite> writes) {

    Namespace ns = state.namespace(namespace);
    List<PendingWrite> existing = new ArrayList<>(ns.writesFor(checkpointId));
    for (int position = 0; position < writes.size(); position++) {
      ChannelWrite write = writes.get(position);
      int index = ControlChannels.indexFor(write.channel(), position);
      PendingWrite candidate =
          new PendingWrite(taskId, index, write.channel(), write.value(), taskPath);

      int at = indexOfSameKey(existing, candidate);
      if (at < 0) {
        existing.add(candidate);
      } else if (index < 0) {
        existing.set(at, candidate);
      }
      // at >= 0 with a non-negative index: keep what is already there (R7)
    }

    Map<String, List<PendingWrite>> allWrites = new LinkedHashMap<>(ns.writes());
    allWrites.put(checkpointId, List.copyOf(existing));
    return state.withNamespace(
        namespace, new Namespace(ns.checkpoints(), allWrites, ns.values()));
  }

  /** One channel write as a caller states it, before the store decides its index. */
  public record ChannelWrite(String channel, Object value) {}

  // ------------------------------------------------------------------ reading

  /**
   * The checkpoint {@code checkpointId} names, or the namespace's greatest id where it is null
   * (R11, R12). Returns null where there is nothing to return — an unknown thread, namespace or id
   * all answer the same way.
   */
  public static CheckpointTuple get(ThreadState state, String namespace, String checkpointId) {
    Namespace ns = state.namespace(namespace);
    String id = checkpointId != null ? checkpointId : ns.latestId();
    if (id == null) {
      return null;
    }
    StoredCheckpoint stored = ns.checkpoints().get(id);
    if (stored == null) {
      return null;
    }
    return toTuple(state.threadId(), namespace, ns, id, stored);
  }

  /**
   * The namespace's checkpoints, newest first, after {@code before}, the metadata filter and the
   * limit have been applied in that order (R13–R17).
   */
  public static List<CheckpointTuple> list(ThreadState state, ListQuery query) {
    Namespace ns = state.namespace(query.namespace());

    List<String> ids = new ArrayList<>(ns.checkpoints().keySet());
    ids.sort(Comparator.reverseOrder());

    List<CheckpointTuple> result = new ArrayList<>();
    for (String id : ids) {
      if (query.before() != null && id.compareTo(query.before()) >= 0) {
        continue;
      }
      StoredCheckpoint stored = ns.checkpoints().get(id);
      if (!matches(stored.metadata(), query.filter())) {
        continue;
      }
      if (query.limit() != null && result.size() >= query.limit()) {
        break;
      }
      result.add(toTuple(state.threadId(), query.namespace(), ns, id, stored));
    }
    return List.copyOf(result);
  }

  private static boolean matches(CheckpointMetadata metadata, Map<String, Object> filter) {
    for (Map.Entry<String, Object> entry : filter.entrySet()) {
      if (!Objects.equals(metadata.get(entry.getKey()), entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  private static CheckpointTuple toTuple(
      String threadId, String namespace, Namespace ns, String id, StoredCheckpoint stored) {

    Map<String, Object> channelValues = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : stored.checkpoint().channelVersions().entrySet()) {
      ChannelValue value =
          ns.values().getOrDefault(entry.getKey(), Map.of()).get(entry.getValue());
      if (value != null && !value.empty()) {
        channelValues.put(entry.getKey(), value.value());
      }
    }
    return new CheckpointTuple(
        threadId,
        namespace == null ? "" : namespace,
        stored.checkpoint(),
        Map.copyOf(channelValues),
        stored.metadata(),
        stored.parentId(),
        ns.writesFor(id));
  }

  // ---------------------------------------------------------------- retention

  /** Applies a retention strategy to the whole thread (R20–R22). */
  public static ThreadState prune(ThreadState state, PruneStrategy strategy) {
    if (strategy == PruneStrategy.DELETE) {
      return new ThreadState(state.threadId(), Map.of());
    }

    Map<String, Namespace> next = new LinkedHashMap<>();
    for (Map.Entry<String, Namespace> entry : state.namespaces().entrySet()) {
      Namespace ns = entry.getValue();
      String keep = ns.latestId();
      if (keep == null) {
        next.put(entry.getKey(), ns);
        continue;
      }
      StoredCheckpoint stored = ns.checkpoints().get(keep);
      // The kept checkpoint is now the start of its chain, so it has nothing before it.
      Map<String, StoredCheckpoint> checkpoints =
          Map.of(keep, new StoredCheckpoint(stored.checkpoint(), stored.metadata(), null));

      Map<String, List<PendingWrite>> writes =
          ns.writes().containsKey(keep)
              ? Map.of(keep, ns.writesFor(keep))
              : Map.of();

      // Only the versions the kept checkpoint still names have a value anyone can reach.
      Map<String, Map<String, ChannelValue>> values = new LinkedHashMap<>();
      for (Map.Entry<String, String> cv : stored.checkpoint().channelVersions().entrySet()) {
        ChannelValue value = ns.values().getOrDefault(cv.getKey(), Map.of()).get(cv.getValue());
        if (value != null) {
          values.put(cv.getKey(), Map.of(cv.getValue(), value));
        }
      }
      next.put(entry.getKey(), new Namespace(checkpoints, writes, values));
    }
    return new ThreadState(state.threadId(), next);
  }

  /** Removes everything the thread holds (R23). */
  public static ThreadState deleteThread(ThreadState state) {
    return new ThreadState(state.threadId(), Map.of());
  }

  // ------------------------------------------------------------------ helpers

  private static int indexOfSameKey(List<PendingWrite> writes, PendingWrite candidate) {
    for (int i = 0; i < writes.size(); i++) {
      if (writes.get(i).sameKeyAs(candidate)) {
        return i;
      }
    }
    return -1;
  }
}
