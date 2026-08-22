package io.akka.langgraph.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 R18, R19. */
class ChannelVersionTest {

  @Test
  void nextVersionIncrementsTheCounterAndStaysDistinct() {
    String v1 = ChannelVersion.first();
    String v2 = ChannelVersion.next(v1);
    String v3 = ChannelVersion.next(v2);

    assertThat(counters(v1, v2, v3)).containsExactly("1", "2", "3");
    assertThat(prefixOf(v1)).hasSize(32);
    assertThat(v1).isLessThan(v2);
    assertThat(v2).isLessThan(v3);
  }

  @Test
  void anIntegerReadsAsTheCounter() {
    assertThat(ChannelVersion.counterOf("5")).isEqualTo(5L);
    assertThat(counters(ChannelVersion.next("5"))).containsExactly("6");
    assertThat(ChannelVersion.counterOf(null)).isZero();
    assertThat(ChannelVersion.counterOf("")).isZero();
  }

  @Test
  void twoVersionsFromTheSameCurrentShareACounterAndStillDiffer() {
    String current = ChannelVersion.first();
    String a = ChannelVersion.next(current);
    String b = ChannelVersion.next(current);

    assertThat(prefixOf(a)).isEqualTo(prefixOf(b));
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void theSuffixIsFixedWidthSoTextOrderIsCounterOrder() {
    // The source's suffix is a Python float in Python's own rendering and is not fixed width
    // (question-log row 12); this port renders a long in hexadecimal, so the whole string is.
    // SPEC-001 §4 OD4 records the divergence.
    List<Integer> widths = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      widths.add(ChannelVersion.first().length());
    }
    assertThat(widths.stream().distinct().toList()).containsExactly(32 + 1 + 16);
  }

  @Test
  void oneThousandSuccessiveVersionsOrderAsText() {
    String version = ChannelVersion.first();
    List<String> all = new ArrayList<>(List.of(version));
    for (int i = 0; i < 1000; i++) {
      version = ChannelVersion.next(version);
      all.add(version);
    }
    List<String> sorted = new ArrayList<>(all);
    java.util.Collections.sort(sorted);
    assertThat(all).isEqualTo(sorted);
  }

  @Test
  void greaterPicksTheHigherCounterAndToleratesAMissingSide() {
    String low = ChannelVersion.first();
    String high = ChannelVersion.next(low);

    assertThat(ChannelVersion.greater(low, high)).isEqualTo(high);
    assertThat(ChannelVersion.greater(high, low)).isEqualTo(high);
    assertThat(ChannelVersion.greater(null, low)).isEqualTo(low);
    assertThat(ChannelVersion.greater(low, null)).isEqualTo(low);
  }

  @Test
  void textThatIsNotAVersionIsRefusedRatherThanReadAsZero() {
    assertThatThrownBy(() -> ChannelVersion.counterOf("not-a-version"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not-a-version");
  }

  private static String prefixOf(String version) {
    return version.substring(0, version.indexOf('.'));
  }

  private static List<String> counters(String... versions) {
    return java.util.Arrays.stream(versions)
        .map(v -> String.valueOf(ChannelVersion.counterOf(v)))
        .toList();
  }
}
