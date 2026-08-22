package io.akka.langgraph.api;

import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Delete;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.CommandException;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpException;
import akka.javasdk.http.HttpResponses;
import io.akka.langgraph.api.ApiTypes.CheckpointView;
import io.akka.langgraph.api.ApiTypes.ResumePlanView;
import io.akka.langgraph.application.ThreadEntity;
import io.akka.langgraph.domain.Checkpoint;
import io.akka.langgraph.domain.CheckpointId;
import io.akka.langgraph.domain.CheckpointMetadata;
import io.akka.langgraph.domain.CheckpointStore.ChannelWrite;
import io.akka.langgraph.domain.ListQuery;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The port's own reachable surface: a caller outside a test drives the checkpoint store through
 * here, which is what a checkpointer is — something a run calls into rather than something a run
 * contains.
 *
 * <p>Two refusals happen here rather than downstream, and SPEC-001 R30 and R31 say why: a thread id
 * the runtime cannot use as an entity id reaches a caller as a ten-second timeout naming nothing,
 * so the boundary refuses it immediately and names the limit.
 *
 * <p>The access rule is open because a checkpoint store's whole purpose is to be called from
 * outside, and because this port exists to be run and read rather than deployed against real
 * threads. A deployment holding anybody's runs would narrow it.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/threads")
public class CheckpointEndpoint {

  /** The runtime refuses an entity id at or past this length (question-log row T1). */
  private static final int MAX_THREAD_ID_LENGTH = 239;

  /** Reserved in the runtime's own replication id (question-log row T2). */
  private static final char RESERVED = '|';

  private final ComponentClient componentClient;

  public CheckpointEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  public record PutRequest(
      String namespace,
      String checkpointId,
      String parentId,
      Map<String, String> channelVersions,
      Map<String, Map<String, String>> versionsSeen,
      List<String> updatedChannels,
      Map<String, String> newVersions,
      Map<String, Object> channelValues,
      String source,
      Integer step) {}

  public record PutResponse(String checkpointId) {}

  public record WriteRequest(
      String namespace,
      String checkpointId,
      String taskId,
      String taskPath,
      List<ChannelWrite> writes) {}

  public record ListRequest(
      String namespace, String before, Map<String, Object> filter, Integer limit) {}

  public record ResumeRequest(String namespace, String checkpointId, List<String> taskIds) {}

  public record GetRequest(String namespace, String checkpointId) {}

  @Post("/{threadId}/checkpoints")
  public HttpResponse put(String threadId, PutRequest request) {
    checkThreadId(threadId);
    String checkpointId =
        request.checkpointId() == null ? CheckpointId.next() : request.checkpointId();

    Checkpoint checkpoint =
        new Checkpoint(
            Checkpoint.FORMAT_VERSION,
            checkpointId,
            Instant.now().toString(),
            request.channelVersions(),
            request.versionsSeen(),
            request.updatedChannels());

    String stored =
        componentClient
            .forEventSourcedEntity(threadId)
            .method(ThreadEntity::put)
            .invoke(
                new ThreadEntity.PutCommand(
                    namespaceOf(request.namespace()),
                    checkpoint,
                    new CheckpointMetadata(request.source(), request.step(), Map.of()),
                    request.parentId(),
                    request.newVersions(),
                    request.channelValues()));
    return HttpResponses.created(new PutResponse(stored));
  }

  @Post("/{threadId}/writes")
  public HttpResponse putWrites(String threadId, WriteRequest request) {
    checkThreadId(threadId);
    try {
      componentClient
          .forEventSourcedEntity(threadId)
          .method(ThreadEntity::putWrites)
          .invoke(
              new ThreadEntity.PutWritesCommand(
                  namespaceOf(request.namespace()),
                  request.checkpointId(),
                  request.taskId(),
                  request.taskPath() == null ? "" : request.taskPath(),
                  request.writes()));
    } catch (CommandException e) {
      throw HttpException.badRequest(e.getMessage());
    }
    return HttpResponses.created();
  }

  @Get("/{threadId}/checkpoints/latest")
  public CheckpointView latest(String threadId) {
    return get(threadId, "", null);
  }

  @Post("/{threadId}/checkpoints/read")
  public CheckpointView read(String threadId, GetRequest request) {
    return get(threadId, namespaceOf(request.namespace()), request.checkpointId());
  }

  @Post("/{threadId}/checkpoints/list")
  public List<CheckpointView> list(String threadId, ListRequest request) {
    checkThreadId(threadId);
    return ApiTypes.views(
        componentClient
            .forEventSourcedEntity(threadId)
            .method(ThreadEntity::list)
            .invoke(
                new ListQuery(
                    namespaceOf(request.namespace()),
                    request.before(),
                    request.filter(),
                    request.limit())));
  }

  @Post("/{threadId}/resume-plan")
  public ResumePlanView resumePlan(String threadId, ResumeRequest request) {
    checkThreadId(threadId);
    return ApiTypes.view(
        componentClient
            .forEventSourcedEntity(threadId)
            .method(ThreadEntity::resumePlan)
            .invoke(
                new ThreadEntity.ResumeCommand(
                    namespaceOf(request.namespace()), request.checkpointId(), request.taskIds())));
  }

  @Post("/{threadId}/prune/{strategy}")
  public HttpResponse prune(String threadId, String strategy) {
    checkThreadId(threadId);
    try {
      componentClient
          .forEventSourcedEntity(threadId)
          .method(ThreadEntity::prune)
          .invoke(strategy.toUpperCase());
    } catch (CommandException e) {
      throw HttpException.badRequest(e.getMessage());
    }
    return HttpResponses.ok();
  }

  @Delete("/{threadId}")
  public HttpResponse delete(String threadId) {
    checkThreadId(threadId);
    componentClient.forEventSourcedEntity(threadId).method(ThreadEntity::delete).invoke();
    return HttpResponses.ok();
  }

  /**
   * Absence answers as 404 because the entity reports it as an answer rather than as a failure —
   * so a genuine failure downstream is left to surface as itself instead of being flattened into
   * "not found".
   */
  private CheckpointView get(String threadId, String namespace, String checkpointId) {
    checkThreadId(threadId);
    ThreadEntity.MaybeCheckpoint found =
        componentClient
            .forEventSourcedEntity(threadId)
            .method(ThreadEntity::get)
            .invoke(new ThreadEntity.GetCommand(namespace, checkpointId));
    if (!found.found()) {
      throw HttpException.notFound();
    }
    return ApiTypes.view(found.tuple());
  }

  private static String namespaceOf(String namespace) {
    return namespace == null ? "" : namespace;
  }

  private static void checkThreadId(String threadId) {
    if (threadId == null || threadId.isEmpty()) {
      throw HttpException.badRequest("thread id must not be empty");
    }
    if (threadId.length() > MAX_THREAD_ID_LENGTH) {
      throw HttpException.badRequest(
          "thread id is "
              + threadId.length()
              + " characters; the limit is "
              + MAX_THREAD_ID_LENGTH);
    }
    if (threadId.indexOf(RESERVED) >= 0) {
      throw HttpException.badRequest("thread id must not contain [" + RESERVED + "]");
    }
  }
}
