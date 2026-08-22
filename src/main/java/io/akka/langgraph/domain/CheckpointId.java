package io.akka.langgraph.domain;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Mints checkpoint ids the way the source does: a time-ordered UUID version 6, rendered as the
 * ordinary 36-character text form (SPEC-001 §2, row 1).
 *
 * <p>Two properties are load-bearing and both come from the layout rather than from luck. The
 * timestamp occupies the most significant bits, so text order is time order; and the generator
 * carries the last timestamp forward and adds one, so two ids minted inside the same clock tick
 * still differ and still order by the order they were minted in.
 */
public final class CheckpointId {

  /** 100-nanosecond intervals between the UUID epoch (1582-10-15) and the Unix epoch. */
  private static final long GREGORIAN_OFFSET_100NS = 0x01B21DD213814000L;

  private static final SecureRandom RANDOM = new SecureRandom();

  private static long lastTimestamp = Long.MIN_VALUE;

  private CheckpointId() {}

  public static synchronized String next() {
    long timestamp = System.currentTimeMillis() * 10_000L + GREGORIAN_OFFSET_100NS;
    if (timestamp <= lastTimestamp) {
      timestamp = lastTimestamp + 1;
    }
    lastTimestamp = timestamp;

    long timeHighAndMid = (timestamp >>> 12) & 0xFFFFFFFFFFFFL;
    long timeLow = timestamp & 0x0FFFL;
    long mostSignificant = (timeHighAndMid << 16) | (6L << 12) | timeLow;

    long clockSeq = RANDOM.nextInt(1 << 14);
    long node = RANDOM.nextLong() & 0xFFFFFFFFFFFFL;
    long leastSignificant = (2L << 62) | (clockSeq << 48) | node;

    return new UUID(mostSignificant, leastSignificant).toString();
  }
}
