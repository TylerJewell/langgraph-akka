package io.akka.langgraph.domain;

/**
 * A checkpoint as the store holds it: the checkpoint, what it says about itself, and the id of the
 * checkpoint it came after (SPEC-001 R1).
 *
 * @param parentId null exactly for the first checkpoint of a namespace
 */
public record StoredCheckpoint(Checkpoint checkpoint, CheckpointMetadata metadata, String parentId) {}
