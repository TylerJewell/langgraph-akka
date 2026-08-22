package io.akka.langgraph.domain;

/**
 * A channel's value at one version, or a marker saying the version was declared without one
 * (SPEC-001 R3, R12).
 *
 * <p>The marker is a third thing, distinct from a missing entry and from a null value: a read omits
 * the channel entirely where it finds one, so a caller cannot tell it apart from a channel that was
 * never written — which is what the source does.
 */
public record ChannelValue(boolean empty, Object value) {

  public static ChannelValue of(Object value) {
    return new ChannelValue(false, value);
  }

  public static ChannelValue emptyMarker() {
    return new ChannelValue(true, null);
  }
}
