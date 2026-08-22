package io.akka.langgraph.domain;

import java.util.Map;
import java.util.Set;

/**
 * The four channels a pending write may name that are not ordinary channel writes, and the fixed
 * index each takes instead of its position in the caller's list (SPEC-001 R6).
 *
 * <p>The negative indices are what make a control write overwrite where an ordinary write is
 * dropped: the repeat rule is guarded on the index being at or above zero (R7, R8).
 */
public final class ControlChannels {

  public static final String ERROR = "__error__";
  public static final String SCHEDULED = "__scheduled__";
  public static final String INTERRUPT = "__interrupt__";
  public static final String RESUME = "__resume__";

  /** Named by the loop when a task fails, alongside {@link #ERROR}; not itself indexed. */
  public static final String ERROR_SOURCE_NODE = "__error_source_node__";

  private static final Map<String, Integer> INDEX_BY_CHANNEL =
      Map.of(ERROR, -1, SCHEDULED, -2, INTERRUPT, -3, RESUME, -4);

  /** The channels whose writes are not restored onto a task on resume (SPEC-001 R25, R26). */
  private static final Set<String> NOT_RESTORED_ON_RESUME =
      Set.of(ERROR, ERROR_SOURCE_NODE, INTERRUPT, RESUME);

  private ControlChannels() {}

  /** The index a write on {@code channel} takes, or {@code positionInCall} for any other channel. */
  public static int indexFor(String channel, int positionInCall) {
    return INDEX_BY_CHANNEL.getOrDefault(channel, positionInCall);
  }

  public static boolean isRestoredOnResume(String channel) {
    return !NOT_RESTORED_ON_RESUME.contains(channel);
  }
}
