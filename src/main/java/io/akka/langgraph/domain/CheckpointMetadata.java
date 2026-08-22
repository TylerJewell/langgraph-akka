package io.akka.langgraph.domain;

import java.util.Map;

/**
 * What a checkpoint says about itself, and the only thing a listing filter reads (SPEC-001 R15).
 *
 * <p>Every field may be absent. A filter naming a field the metadata does not carry excludes the
 * row rather than matching it, which is why {@link #get} answers null for an absent field instead of
 * a default.
 *
 * @param source how the checkpoint came about — {@code input}, {@code loop}, {@code update} or
 *     {@code fork}
 * @param step -1 for the first, input checkpoint, counting up from there
 * @param parents checkpoint namespace to the parent checkpoint id in it
 */
public record CheckpointMetadata(String source, Integer step, Map<String, String> parents) {

  public CheckpointMetadata {
    parents = parents == null ? Map.of() : Map.copyOf(parents);
  }

  public static CheckpointMetadata empty() {
    return new CheckpointMetadata(null, null, Map.of());
  }

  /** The value a filter key names, or null where the metadata does not carry it. */
  public Object get(String key) {
    return switch (key) {
      case "source" -> source;
      case "step" -> step;
      case "parents" -> parents.isEmpty() ? null : parents;
      default -> null;
    };
  }
}
