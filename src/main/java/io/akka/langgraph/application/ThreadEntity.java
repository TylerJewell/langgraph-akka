package io.akka.langgraph.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.TypeName;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.langgraph.domain.Checkpoint;
import io.akka.langgraph.domain.CheckpointMetadata;
import io.akka.langgraph.domain.CheckpointStore;
import io.akka.langgraph.domain.CheckpointStore.ChannelWrite;
import io.akka.langgraph.domain.CheckpointStore.PruneStrategy;
import io.akka.langgraph.domain.CheckpointTuple;
import io.akka.langgraph.domain.ListQuery;
import io.akka.langgraph.domain.PendingWrite;
import io.akka.langgraph.domain.ResumePlan;
import io.akka.langgraph.domain.ThreadState;
import java.util.List;
import java.util.Map;

/**
 * One langgraph thread, durable. The entity id is the thread id and nothing else — SPEC-001 §4 OD1
 * records why the namespace is held inside the state instead of in the key.
 *
 * <p>Every decision lives in {@link CheckpointStore}; this class only turns commands into events and
 * events into state, so the same rules the unit tests drive are the rules a running runtime applies.
 */
@Component(id = "thread")
public class ThreadEntity extends EventSourcedEntity<ThreadState, ThreadEntity.Event> {

  private final String threadId;
  private final int retentionThreshold;

  public sealed interface Event {

    @TypeName("checkpoint-put")
    record CheckpointPut(
        String namespace,
        Checkpoint checkpoint,
        CheckpointMetadata metadata,
        String parentId,
        Map<String, String> newVersions,
        Map<String, Object> channelValues)
        implements Event {}

    @TypeName("writes-recorded")
    record WritesRecorded(
        String namespace,
        String checkpointId,
        String taskId,
        String taskPath,
        List<ChannelWrite> writes)
        implements Event {}

    @TypeName("pruned")
    record Pruned(PruneStrategy strategy) implements Event {}

    @TypeName("thread-deleted")
    record ThreadDeleted() implements Event {}
  }

  // ---- commands

  public record PutCommand(
      String namespace,
      Checkpoint checkpoint,
      CheckpointMetadata metadata,
      String parentId,
      Map<String, String> newVersions,
      Map<String, Object> channelValues) {}

  public record PutWritesCommand(
      String namespace,
      String checkpointId,
      String taskId,
      String taskPath,
      List<ChannelWrite> writes) {}

  public record GetCommand(String namespace, String checkpointId) {}

  public record ResumeCommand(String namespace, String checkpointId, List<String> taskIds) {}

  /** A read that may find nothing. Absence is an answer here, not a failure (R11). */
  public record MaybeCheckpoint(boolean found, CheckpointTuple tuple) {}

  public ThreadEntity(EventSourcedEntityContext context) {
    this.threadId = context.entityId();
    this.retentionThreshold = CheckpointStore.DEFAULT_RETENTION_THRESHOLD;
  }

  @Override
  public ThreadState emptyState() {
    return new ThreadState(threadId, Map.of());
  }

  /**
   * Applies an event, and cannot fail while doing so. Every refusal the store can make is decided
   * in the command handler that persists the event, so nothing reachable from here throws: an event
   * handler that can throw is an entity that stops replaying and never comes back.
   */
  @Override
  public ThreadState applyEvent(Event event) {
    return switch (event) {
      case Event.CheckpointPut e ->
          CheckpointStore.put(
                  currentState(),
                  e.namespace(),
                  e.checkpoint(),
                  e.metadata(),
                  e.parentId(),
                  e.newVersions(),
                  e.channelValues(),
                  retentionThreshold)
              .state();
      case Event.WritesRecorded e ->
          CheckpointStore.applyWrites(
              currentState(),
              e.namespace(),
              e.checkpointId(),
              e.taskId(),
              e.taskPath(),
              e.writes());
      case Event.Pruned e -> CheckpointStore.prune(currentState(), e.strategy());
      case Event.ThreadDeleted ignored -> CheckpointStore.deleteThread(currentState());
    };
  }

  /** Stores a checkpoint and answers the id to thread into the next one (SPEC-001 R1, R2). */
  public Effect<String> put(PutCommand command) {
    return effects()
        .persist(
            new Event.CheckpointPut(
                command.namespace(),
                command.checkpoint(),
                command.metadata(),
                command.parentId(),
                command.newVersions() == null ? Map.of() : command.newVersions(),
                command.channelValues() == null ? Map.of() : command.channelValues()))
        .thenReply(state -> command.checkpoint().id());
  }

  /**
   * Records a task's writes. The refusal is decided before anything is persisted, so a write
   * against a checkpoint that is not there leaves no event behind (SPEC-001 R10).
   */
  public Effect<Done> putWrites(PutWritesCommand command) {
    try {
      CheckpointStore.checkWritesTarget(
          currentState(), command.namespace(), command.checkpointId());
    } catch (IllegalArgumentException e) {
      return effects().error(e.getMessage());
    }
    return effects()
        .persist(
            new Event.WritesRecorded(
                command.namespace(),
                command.checkpointId(),
                command.taskId(),
                command.taskPath(),
                command.writes()))
        .thenReply(state -> Done.getInstance());
  }

  /** The named checkpoint, or the namespace's latest, or nothing (SPEC-001 R11, R12). */
  public ReadOnlyEffect<MaybeCheckpoint> get(GetCommand command) {
    CheckpointTuple tuple =
        CheckpointStore.get(currentState(), command.namespace(), command.checkpointId());
    return effects().reply(new MaybeCheckpoint(tuple != null, tuple));
  }

  public ReadOnlyEffect<List<CheckpointTuple>> list(ListQuery query) {
    return effects().reply(CheckpointStore.list(currentState(), query));
  }

  /**
   * What a caller picking this thread up should replay and what it should skip (SPEC-001 R25–R29).
   * The thread holds the record; the caller holds the tasks, so the plan is computed here and
   * carried out there.
   */
  public ReadOnlyEffect<ResumePlan> resumePlan(ResumeCommand command) {
    CheckpointTuple tuple =
        CheckpointStore.get(currentState(), command.namespace(), command.checkpointId());
    List<PendingWrite> pending = tuple == null ? List.of() : tuple.pendingWrites();
    return effects().reply(ResumePlan.from(command.taskIds(), pending));
  }

  public Effect<Done> prune(String strategy) {
    PruneStrategy parsed;
    try {
      parsed = PruneStrategy.valueOf(strategy);
    } catch (IllegalArgumentException e) {
      return effects().error("unknown prune strategy [" + strategy + "]");
    }
    return effects().persist(new Event.Pruned(parsed)).thenReply(state -> Done.getInstance());
  }

  public Effect<Done> delete() {
    return effects().persist(new Event.ThreadDeleted()).thenReply(state -> Done.getInstance());
  }

  public ReadOnlyEffect<ThreadState> state() {
    return effects().reply(currentState());
  }
}
