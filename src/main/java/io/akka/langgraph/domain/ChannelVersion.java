package io.akka.langgraph.domain;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Mints the next version of a channel: a 32-digit zero-padded counter, a dot, and a suffix
 * (SPEC-001 R18, R19).
 *
 * <p>The counter is the whole of the ordering. It is fixed width, so comparing two versions as text
 * compares their counters; the suffix exists only so that two versions minted from the same current
 * value are distinct, and nothing reads it — so it is drawn from a plain random source rather
 * than a cryptographic one. The source's suffix is a Python float in Python's own
 * rendering, which no other language reproduces character for character — SPEC-001 §4 OD4 records
 * that this port renders a random long in hexadecimal instead.
 */
public final class ChannelVersion {

  private static final int COUNTER_DIGITS = 32;

  /** Built once: a version is minted once per channel per step, on the write path. */
  private static final String FORMAT = "%0" + COUNTER_DIGITS + "d.%016x";

  private ChannelVersion() {}

  /** The first version of a channel that has none. */
  public static String first() {
    return next(null);
  }

  public static String next(String current) {
    return format(counterOf(current) + 1);
  }

  /** The counter a version carries, or zero for no version at all. */
  public static long counterOf(String version) {
    if (version == null || version.isEmpty()) {
      return 0L;
    }
    int dot = version.indexOf('.');
    String digits = dot < 0 ? version : version.substring(0, dot);
    try {
      return Long.parseLong(digits);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("not a channel version: [" + version + "]", e);
    }
  }

  /** The greater of two versions, by counter. A tie returns the first, as the counter is all. */
  public static String greater(String a, String b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return counterOf(b) > counterOf(a) ? b : a;
  }

  private static String format(long counter) {
    return String.format(FORMAT, counter, ThreadLocalRandom.current().nextLong());
  }
}
