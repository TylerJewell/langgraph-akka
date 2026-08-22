package io.akka.langgraph.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.langgraph.domain.Checkpoint;
import io.akka.langgraph.domain.CheckpointMetadata;
import io.akka.langgraph.domain.CheckpointStore;
import io.akka.langgraph.domain.CheckpointStore.ChannelWrite;
import io.akka.langgraph.domain.CheckpointTuple;
import io.akka.langgraph.domain.ListQuery;
import io.akka.langgraph.domain.PendingWrite;
import io.akka.langgraph.domain.ResumePlan;
import io.akka.langgraph.domain.ThreadState;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Runs the same {@code bench/workloads.json} the source side runs, and records the same shape of
 * answer, so section 1 of the report compares two sequences rather than two descriptions of one.
 *
 * <p>What is driven is {@link CheckpointStore}: every rule in SPEC-001 section 3 lives there, and
 * the entity only turns commands into events that call it, so this drives the deciding code rather
 * than a copy of it. The id mint is stood in for on both sides, for the same reason — two sets of
 * generated identifiers cannot be lined up against each other, and the mint is the subject of no
 * claim compared here.
 *
 * <p>Deliberately not a test. It writes files and reads a clock, and {@code mvn verify} should
 * depend on neither.
 */
public final class BenchRunner {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A window aims for tens of milliseconds; the figure is the median of several windows. */
  private static final long TARGET_WINDOW_NS = 50_000_000L;

  private static final int WINDOWS = 5;
  private static final int WARMUP_RUNS = 20_000;
  private static final int RETENTION = CheckpointStore.DEFAULT_RETENTION_THRESHOLD;

  private BenchRunner() {}

  /** Fixed, orderable, and the same width as a real UUIDv6, so text order is still step order. */
  private static String stepId(int n) {
    return String.format("00000000-0000-6000-8000-%012d", n);
  }

  private static String summariseWrites(List<PendingWrite> writes) {
    List<String> parts = new ArrayList<>();
    for (PendingWrite w : writes) {
      parts.add(w.taskId() + "/" + w.channel() + "=" + w.value());
    }
    return String.join(";", parts);
  }

  private static String joinSteps(List<CheckpointTuple> listed, Map<String, Integer> stepOf) {
    List<String> parts = new ArrayList<>();
    for (CheckpointTuple t : listed) {
      parts.add(String.valueOf(stepOf.get(t.checkpoint().id())));
    }
    return String.join(",", parts);
  }

  static List<String> runWorkload(JsonNode workload) {
    ThreadState state = new ThreadState("bench", Map.of());
    List<String> answers = new ArrayList<>();
    Map<String, String> latestByNs = new HashMap<>();
    Map<String, String> parentByNs = new HashMap<>();
    Map<String, Integer> stepOf = new LinkedHashMap<>();
    int putCount = 0;

    for (JsonNode step : workload.get("steps")) {
      String op = step.get("op").asText();
      String ns = step.hasNonNull("ns") ? step.get("ns").asText() : "";

      switch (op) {
        case "put" -> {
          putCount++;
          String cid = stepId(putCount);
          String version = String.format("%032d.0", putCount);

          Map<String, String> versions = new LinkedHashMap<>();
          Map<String, Object> values = new LinkedHashMap<>();
          step.get("channels")
              .fields()
              .forEachRemaining(
                  e -> {
                    versions.put(e.getKey(), version);
                    values.put(e.getKey(), e.getValue().asText());
                  });

          Checkpoint checkpoint =
              new Checkpoint(
                  Checkpoint.FORMAT_VERSION,
                  cid,
                  "2026-01-01T00:00:00+00:00",
                  versions,
                  Map.of(),
                  List.copyOf(versions.keySet()));
          CheckpointMetadata metadata =
              new CheckpointMetadata(
                  step.hasNonNull("source") ? step.get("source").asText() : "loop",
                  step.get("step").asInt(),
                  Map.of());

          state =
              CheckpointStore.put(
                      state, ns, checkpoint, metadata, parentByNs.get(ns), versions, values,
                      RETENTION)
                  .state();
          parentByNs.put(ns, cid);
          latestByNs.put(ns, cid);
          stepOf.put(cid, step.get("step").asInt());
          answers.add("put:" + step.get("step").asInt());
        }

        case "latest" -> {
          CheckpointTuple tuple = CheckpointStore.get(state, ns, null);
          answers.add(
              tuple == null ? "latest:none" : "latest:" + stepOf.get(tuple.checkpoint().id()));
        }

        case "writeBatches" -> {
          // The batching is the input this workload varies, so the calls are made from
          // `batches` rather than written out a second time in `steps`.
          int calls = 0;
          for (JsonNode batch : workload.get("batches")) {
            List<ChannelWrite> writes = new ArrayList<>();
            for (JsonNode w : batch) {
              writes.add(new ChannelWrite(w.get("channel").asText(), w.get("value").asText()));
            }
            state =
                CheckpointStore.putWrites(
                    state, ns, latestByNs.get(ns), step.get("task").asText(), "", writes);
            calls++;
          }
          answers.add("writeBatches:" + calls);
        }

        case "writeEachOrder" -> {
          // Each delivery order is run against its own state, so the answers are that many
          // independent runs of the same writes rather than one run of all of them.
          List<String> outcomes = new ArrayList<>();
          for (JsonNode order : workload.get("orders")) {
            ThreadState side = seedForOrders(ns);
            for (JsonNode position : order) {
              JsonNode row = workload.get("rows").get(position.asInt());
              side =
                  CheckpointStore.putWrites(
                      side,
                      ns,
                      stepId(1),
                      step.get("task").asText(),
                      "",
                      List.of(new ChannelWrite(row.get("channel").asText(), row.get("value").asText())));
            }
            CheckpointTuple tuple = CheckpointStore.get(side, ns, stepId(1));
            outcomes.add(summariseWrites(tuple.pendingWrites()));
          }
          if (workload.path("expectsDistinctAnswers").asBoolean()
              && new java.util.HashSet<>(outcomes).size() < 2) {
            throw new IllegalStateException(
                workload.get("name").asText()
                    + " declares that its answer moves with delivery order, and every one of the "
                    + outcomes.size()
                    + " orders answered "
                    + outcomes.get(0));
          }
          answers.add("orders:" + String.join("|", outcomes));
        }

        case "write" -> {
          List<ChannelWrite> writes = new ArrayList<>();
          for (JsonNode w : step.get("writes")) {
            writes.add(new ChannelWrite(w.get(0).asText(), w.get(1).asText()));
          }
          state =
              CheckpointStore.putWrites(
                  state, ns, latestByNs.get(ns), step.get("task").asText(), "", writes);
          answers.add("write:ok");
        }

        case "readWrites" -> {
          CheckpointTuple tuple = CheckpointStore.get(state, ns, latestByNs.get(ns));
          answers.add(
              tuple == null
                  ? "writes:absent"
                  : "writes:" + summariseWrites(tuple.pendingWrites()));
        }

        case "count" ->
            answers.add("count:" + CheckpointStore.list(state, ListQuery.all(ns)).size());

        case "list" -> {
          Map<String, Object> filter = new LinkedHashMap<>();
          if (step.hasNonNull("filter")) {
            step.get("filter")
                .fields()
                .forEachRemaining(e -> filter.put(e.getKey(), e.getValue().asText()));
          }
          Integer limit = step.hasNonNull("limit") ? step.get("limit").asInt() : null;
          List<CheckpointTuple> listed =
              CheckpointStore.list(state, new ListQuery(ns, null, filter, limit));
          answers.add("list:" + joinSteps(listed, stepOf));
        }

        case "listBefore" -> {
          int beforeStep = step.get("beforeStep").asInt();
          String before =
              stepOf.entrySet().stream()
                  .filter(e -> e.getValue() == beforeStep)
                  .map(Map.Entry::getKey)
                  .findFirst()
                  .orElseThrow();
          List<CheckpointTuple> listed =
              CheckpointStore.list(state, new ListQuery(ns, before, Map.of(), null));
          answers.add("listBefore:" + joinSteps(listed, stepOf));
        }

        case "resumePlan" -> {
          CheckpointTuple tuple = CheckpointStore.get(state, ns, latestByNs.get(ns));
          List<String> tasks = new ArrayList<>();
          step.get("tasks").forEach(t -> tasks.add(t.asText()));
          ResumePlan plan =
              ResumePlan.from(tasks, tuple == null ? List.of() : tuple.pendingWrites());
          answers.add(
              "resume:restored="
                  + String.join(",", new TreeSet<>(plan.restored().keySet()))
                  + "|rerun="
                  + String.join(",", plan.rerun()));
        }

        case "delete" -> {
          state = CheckpointStore.deleteThread(state);
          latestByNs.clear();
          parentByNs.clear();
          answers.add("delete:ok");
        }

        default -> throw new IllegalArgumentException("unknown op " + op);
      }
    }
    return answers;
  }

  /** The one checkpoint every delivery order of an arrival-order workload writes against. */
  private static ThreadState seedForOrders(String ns) {
    Map<String, String> versions = Map.of("a", String.format("%032d.0", 1));
    Checkpoint checkpoint =
        new Checkpoint(
            Checkpoint.FORMAT_VERSION,
            stepId(1),
            "2026-01-01T00:00:00+00:00",
            versions,
            Map.of(),
            List.of("a"));
    return CheckpointStore.put(
            new ThreadState("bench", Map.of()),
            ns,
            checkpoint,
            new CheckpointMetadata("loop", 0, Map.of()),
            null,
            versions,
            Map.of("a", "v0"),
            RETENTION)
        .state();
  }

  private static long windowRepetitions(JsonNode workload) {
    long start = System.nanoTime();
    runWorkload(workload);
    long pilot = System.nanoTime() - start;
    if (pilot <= 0) {
      throw new IllegalStateException(
          "pilot for " + workload.get("name").asText() + " measured nothing");
    }
    return Math.max(1, TARGET_WINDOW_NS / pilot);
  }

  private static ObjectNode timeWorkload(JsonNode workload) {
    // A cold JIT measures the compiler rather than the code it compiles.
    for (int i = 0; i < WARMUP_RUNS; i++) {
      runWorkload(workload);
    }
    long reps = windowRepetitions(workload);
    List<Double> readings = new ArrayList<>();
    for (int w = 0; w < WINDOWS; w++) {
      long start = System.nanoTime();
      for (long r = 0; r < reps; r++) {
        runWorkload(workload);
      }
      long elapsed = System.nanoTime() - start;
      if (elapsed <= 0) {
        throw new IllegalStateException(
            "window for " + workload.get("name").asText() + " measured nothing");
      }
      readings.add((double) elapsed / reps);
    }
    readings.sort(Comparator.naturalOrder());

    ObjectNode out = MAPPER.createObjectNode();
    out.put("workload", workload.get("name").asText());
    out.put("repetitions", reps);
    out.put("windows", WINDOWS);
    out.put("nanosPerRun", Math.round(readings.get(WINDOWS / 2)));
    return out;
  }

  public static void main(String[] args) throws Exception {
    Path bench = Path.of(args[0]);
    JsonNode workloads = MAPPER.readTree(Files.readString(bench.resolve("workloads.json")));

    // The shape toolkit/sequence_probe.py reads: one row per step, carrying the field the
    // workload names as the one that must move.
    ObjectNode answers = MAPPER.createObjectNode();
    for (JsonNode w : workloads) {
      ArrayNode rows = MAPPER.createArrayNode();
      for (String outcome : runWorkload(w)) {
        rows.add(MAPPER.createObjectNode().put("outcome", outcome));
      }
      answers.set(w.get("name").asText(), rows);
    }
    ObjectNode answerDocument = MAPPER.createObjectNode();
    answerDocument.set("answers", answers);
    Files.writeString(
        bench.resolve("port-answers.json"),
        MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(answerDocument) + System.lineSeparator());

    // The shape toolkit/timing_check.py reads.
    ObjectNode timings = MAPPER.createObjectNode();
    for (JsonNode w : workloads) {
      timings.set(w.get("name").asText(), timeWorkload(w));
    }
    ObjectNode timingDocument = MAPPER.createObjectNode();
    timingDocument.set("timing", timings);
    Files.writeString(
        bench.resolve("port-timings.json"),
        MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(timingDocument) + System.lineSeparator());

    System.out.println(answerDocument.toPrettyString());
    System.out.println(timingDocument.toPrettyString());
  }
}
