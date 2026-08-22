package io.akka.langgraph.domain;

import java.util.List;
import java.util.Map;

/**
 * What a read gives back: the checkpoint with its channel values reassembled, what it says about
 * itself, where it sits in the chain, and the writes recorded against it (SPEC-001 R11, R12).
 *
 * @param channelValues rebuilt by looking each channel version up; a channel stored as the empty
 *     marker is absent from this map rather than present and null
 * @param parentId null for the first checkpoint of a namespace
 */
public record CheckpointTuple(
    String threadId,
    String namespace,
    Checkpoint checkpoint,
    Map<String, Object> channelValues,
    CheckpointMetadata metadata,
    String parentId,
    List<PendingWrite> pendingWrites) {}
