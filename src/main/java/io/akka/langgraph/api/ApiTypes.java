package io.akka.langgraph.api;

import io.akka.langgraph.domain.CheckpointTuple;
import io.akka.langgraph.domain.PendingWrite;
import io.akka.langgraph.domain.ResumePlan;
import java.util.List;
import java.util.Map;

/**
 * What the HTTP surface answers with. These are the API's own shapes rather than the domain's, so
 * that a change to how a checkpoint is stored is not automatically a change to what callers see.
 */
public final class ApiTypes {

  public record WriteView(String taskId, int index, String channel, Object value, String taskPath) {
    static WriteView from(PendingWrite write) {
      return new WriteView(
          write.taskId(), write.index(), write.channel(), write.value(), write.taskPath());
    }
  }

  public record CheckpointView(
      String threadId,
      String namespace,
      String checkpointId,
      String parentId,
      String ts,
      Map<String, String> channelVersions,
      Map<String, Map<String, String>> versionsSeen,
      Map<String, Object> channelValues,
      String source,
      Integer step,
      List<WriteView> pendingWrites) {

    static CheckpointView from(CheckpointTuple tuple) {
      return new CheckpointView(
          tuple.threadId(),
          tuple.namespace(),
          tuple.checkpoint().id(),
          tuple.parentId(),
          tuple.checkpoint().ts(),
          tuple.checkpoint().channelVersions(),
          tuple.checkpoint().versionsSeen(),
          tuple.channelValues(),
          tuple.metadata().source(),
          tuple.metadata().step(),
          tuple.pendingWrites().stream().map(WriteView::from).toList());
    }
  }

  public record ResumePlanView(Map<String, List<WriteView>> restored, List<String> rerun) {

    static ResumePlanView from(ResumePlan plan) {
      Map<String, List<WriteView>> restored =
          plan.restored().entrySet().stream()
              .collect(
                  java.util.stream.Collectors.toMap(
                      Map.Entry::getKey,
                      e -> e.getValue().stream().map(WriteView::from).toList()));
      return new ResumePlanView(restored, plan.rerun());
    }
  }

  private ApiTypes() {}

  public static CheckpointView view(CheckpointTuple tuple) {
    return CheckpointView.from(tuple);
  }

  public static List<CheckpointView> views(List<CheckpointTuple> tuples) {
    return tuples.stream().map(CheckpointView::from).toList();
  }

  public static ResumePlanView view(ResumePlan plan) {
    return ResumePlanView.from(plan);
  }
}
