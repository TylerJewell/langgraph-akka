package io.akka.langgraph.domain;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything one thread holds (SPEC-001 §2). The thread is the durable unit and the namespace lives
 * inside it rather than in its name — §4 OD1 records why: the target reserves a character that a
 * composite key would need, and every character it does accept is one a caller-chosen thread id may
 * itself contain.
 */
public record ThreadState(String threadId, Map<String, Namespace> namespaces) {

  public static ThreadState empty() {
    return new ThreadState(null, Map.of());
  }

  public ThreadState {
    namespaces = namespaces == null ? Map.of() : Map.copyOf(namespaces);
  }

  public Namespace namespace(String name) {
    return namespaces.getOrDefault(name == null ? "" : name, Namespace.empty());
  }

  public ThreadState withNamespace(String name, Namespace replacement) {
    Map<String, Namespace> next = new LinkedHashMap<>(namespaces);
    next.put(name == null ? "" : name, replacement);
    return new ThreadState(threadId, next);
  }

  public boolean isEmpty() {
    return namespaces.values().stream().allMatch(ns -> ns.checkpoints().isEmpty());
  }

  public int checkpointCount() {
    return namespaces.values().stream().mapToInt(ns -> ns.checkpoints().size()).sum();
  }
}
