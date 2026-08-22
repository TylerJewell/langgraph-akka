package io.akka.langgraph.domain;

import java.util.Map;

/**
 * The three filters a listing takes (SPEC-001 R14, R15, R16).
 *
 * @param namespace which chain to list; never null, the root chain being the empty string
 * @param before exclude this checkpoint and everything after it — strict, so the named checkpoint
 *     is itself excluded
 * @param filter every entry must equal the metadata's value for that key; a key the metadata does
 *     not carry excludes the row
 * @param limit cap the result after filtering, or null for no cap
 */
public record ListQuery(String namespace, String before, Map<String, Object> filter, Integer limit) {

  public ListQuery {
    namespace = namespace == null ? "" : namespace;
    filter = filter == null ? Map.of() : Map.copyOf(filter);
  }

  public static ListQuery all(String namespace) {
    return new ListQuery(namespace, null, Map.of(), null);
  }
}
