package io.akka.langgraph.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.http.javadsl.model.StatusCodes;
import akka.javasdk.testkit.TestKitSupport;
import io.akka.langgraph.domain.ChannelVersion;
import io.akka.langgraph.domain.CheckpointStore.ChannelWrite;
import io.akka.langgraph.domain.ControlChannels;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The port's capability driven the way something outside a test drives it. This exists as its own
 * suite because "no screen" answers whether the port renders anything, not whether anything can
 * reach it: a capability only ever called from a test has no reachable surface at all.
 */
public class CheckpointEndpointIntegrationTest extends TestKitSupport {

  private String newThread() {
    return "thread-" + UUID.randomUUID();
  }

  private String put(String threadId, String parentId, int step, Object value) {
    String version = ChannelVersion.first();
    var request =
        new CheckpointEndpoint.PutRequest(
            "",
            null,
            parentId,
            Map.of("a", version),
            Map.of(),
            null,
            Map.of("a", version),
            Map.of("a", value),
            step == 0 ? "input" : "loop",
            step);
    var response =
        httpClient
            .POST("/threads/" + threadId + "/checkpoints")
            .withRequestBody(request)
            .responseBodyAs(CheckpointEndpoint.PutResponse.class)
            .invoke();
    assertThat(response.status()).isEqualTo(StatusCodes.CREATED);
    return response.body().checkpointId();
  }

  @Test
  void aRunIsWrittenReadBackAndListedThroughHttp() {
    String threadId = newThread();
    String first = put(threadId, null, 0, "value-0");
    String second = put(threadId, first, 1, "value-1");
    String third = put(threadId, second, 2, "value-2");

    var latest =
        httpClient
            .GET("/threads/" + threadId + "/checkpoints/latest")
            .responseBodyAs(ApiTypes.CheckpointView.class)
            .invoke();
    assertThat(latest.status()).isEqualTo(StatusCodes.OK);
    assertThat(latest.body().checkpointId()).isEqualTo(third);
    assertThat(latest.body().channelValues()).isEqualTo(Map.of("a", "value-2"));
    assertThat(latest.body().parentId()).isEqualTo(second);

    var listed =
        httpClient
            .POST("/threads/" + threadId + "/checkpoints/list")
            .withRequestBody(new CheckpointEndpoint.ListRequest("", null, Map.of("source", "loop"), null))
            .responseBodyAsListOf(ApiTypes.CheckpointView.class)
            .invoke();
    assertThat(listed.body().stream().map(t -> t.checkpointId()).toList())
        .containsExactly(third, second);
  }

  @Test
  void writesAndAResumePlanThroughHttp() {
    String threadId = newThread();
    String checkpointId = put(threadId, null, 0, "value-0");

    var recorded =
        httpClient
            .POST("/threads/" + threadId + "/writes")
            .withRequestBody(
                new CheckpointEndpoint.WriteRequest(
                    "", checkpointId, "a", "", List.of(new ChannelWrite("log", "done"))))
            .invoke();
    assertThat(recorded.status()).isEqualTo(StatusCodes.CREATED);

    httpClient
        .POST("/threads/" + threadId + "/writes")
        .withRequestBody(
            new CheckpointEndpoint.WriteRequest(
                "",
                checkpointId,
                "b",
                "",
                List.of(new ChannelWrite(ControlChannels.INTERRUPT, "need input"))))
        .invoke();

    var plan =
        httpClient
            .POST("/threads/" + threadId + "/resume-plan")
            .withRequestBody(
                new CheckpointEndpoint.ResumeRequest("", checkpointId, List.of("a", "b", "c")))
            .responseBodyAs(ApiTypes.ResumePlanView.class)
            .invoke();
    assertThat(plan.body().restored()).containsOnlyKeys("a");
    assertThat(plan.body().rerun()).containsExactly("b", "c");
  }

  @Test
  void badThreadIdsAreRefusedAtTheBoundary() {
    // Both of these reach the runtime as a ten-second timeout naming nothing if they are passed
    // through (question-log rows T1, T2), so the boundary refuses them and says which limit.
    var tooLong =
        httpClient
            .GET("/threads/" + "a".repeat(300) + "/checkpoints/latest")
            .invoke();
    assertThat(tooLong.status()).isEqualTo(StatusCodes.BAD_REQUEST);

    var reserved =
        httpClient.GET("/threads/thread%7Csubgraph/checkpoints/latest").invoke();
    assertThat(reserved.status()).isEqualTo(StatusCodes.BAD_REQUEST);

    // 239 characters is inside the limit and is accepted
    String longButAllowed = "a".repeat(239);
    put(longButAllowed, null, 0, "value-0");
  }

  @Test
  void readingAThreadThatHoldsNothingIsNotFound() {
    var missing =
        httpClient.GET("/threads/" + newThread() + "/checkpoints/latest").invoke();
    assertThat(missing.status()).isEqualTo(StatusCodes.NOT_FOUND);
  }

  @Test
  void aWriteAgainstAnAbsentCheckpointIsABadRequest() {
    String threadId = newThread();
    put(threadId, null, 0, "value-0");

    var refused =
        httpClient
            .POST("/threads/" + threadId + "/writes")
            .withRequestBody(
                new CheckpointEndpoint.WriteRequest(
                    "", "no-such-id", "a", "", List.of(new ChannelWrite("log", "x"))))
            .invoke();
    assertThat(refused.status()).isEqualTo(StatusCodes.BAD_REQUEST);
  }

  @Test
  void pruningAndDeletingThroughHttp() {
    String threadId = newThread();
    String first = put(threadId, null, 0, "value-0");
    String second = put(threadId, first, 1, "value-1");

    httpClient.POST("/threads/" + threadId + "/prune/keep_latest").invoke();
    var afterPrune =
        httpClient
            .POST("/threads/" + threadId + "/checkpoints/list")
            .withRequestBody(new CheckpointEndpoint.ListRequest("", null, null, null))
            .responseBodyAsListOf(ApiTypes.CheckpointView.class)
            .invoke();
    assertThat(afterPrune.body().stream().map(t -> t.checkpointId()).toList())
        .containsExactly(second);

    httpClient.DELETE("/threads/" + threadId).invoke();
    assertThat(httpClient.GET("/threads/" + threadId + "/checkpoints/latest").invoke().status())
        .isEqualTo(StatusCodes.NOT_FOUND);
  }

  @Test
  void anUnknownPruneStrategyIsABadRequest() {
    String threadId = newThread();
    put(threadId, null, 0, "value-0");

    assertThat(httpClient.POST("/threads/" + threadId + "/prune/whatever").invoke().status())
        .isEqualTo(StatusCodes.BAD_REQUEST);
  }
}
