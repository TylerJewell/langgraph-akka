package io.akka.langgraph.domain;

import java.util.List;
import java.util.Map;

/**
 * One step of a run, as it is stored (SPEC-001 §2).
 *
 * <p>A channel's value is deliberately not here. The checkpoint records which version of each
 * channel was current, and the value itself is stored once per version alongside the chain, so a
 * step that leaves a channel alone stores no second copy of it (R3, R12).
 *
 * @param v the checkpoint format version
 * @param id a time-ordered id, so text order is chronological order
 * @param ts when it was written, ISO 8601
 * @param channelVersions channel name to the version current at this step
 * @param versionsSeen node name to the versions that node has recorded seeing
 * @param updatedChannels the channels this step wrote, or null where that was not recorded
 */
public record Checkpoint(
    int v,
    String id,
    String ts,
    Map<String, String> channelVersions,
    Map<String, Map<String, String>> versionsSeen,
    List<String> updatedChannels) {

  public static final int FORMAT_VERSION = 1;

  public Checkpoint {
    channelVersions = channelVersions == null ? Map.of() : Map.copyOf(channelVersions);
    versionsSeen = versionsSeen == null ? Map.of() : Map.copyOf(versionsSeen);
    updatedChannels = updatedChannels == null ? null : List.copyOf(updatedChannels);
  }
}
