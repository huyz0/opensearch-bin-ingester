// SPDX-License-Identifier: Apache-2.0
package binjava.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M9.3. The property under test is reproducibility: a workload that cannot be
 * replayed byte for byte makes every number taken over it unfalsifiable, because
 * a second run measures a different input.
 *
 * <p>⚠️ Both directions are pinned. "Same seed, same bytes" alone is satisfied by
 * a generator that ignores the seed entirely and emits one constant stream, so
 * the opposite case -- a different seed yielding different bytes -- is asserted
 * beside it.
 */
class LoadGeneratorTest {

  private static WorkloadSpec spec(long seed, SizeProfile profile) {
    return new WorkloadSpec(seed, 4, 2, profile, 3);
  }

  private static List<byte[]> bodies(WorkloadSpec spec, int batches) {
    LoadGenerator gen = new LoadGenerator(spec);
    List<byte[]> out = new ArrayList<>();
    for (int i = 0; i < batches; i++) {
      out.add(gen.nextBatch().body());
    }
    return out;
  }

  /** The source (odd-indexed) lines of a bulk body, as strings. */
  private static List<String> sourceLines(byte[] body) {
    String[] lines = new String(body, StandardCharsets.UTF_8).split("\n", -1);
    List<String> out = new ArrayList<>();
    // The body ends with a newline, so the split leaves a trailing empty entry.
    assertThat(lines[lines.length - 1]).isEmpty();
    for (int i = 1; i < lines.length - 1; i += 2) {
      out.add(lines[i]);
    }
    return out;
  }

  private static List<String> actionLines(byte[] body) {
    String[] lines = new String(body, StandardCharsets.UTF_8).split("\n", -1);
    List<String> out = new ArrayList<>();
    for (int i = 0; i < lines.length - 1; i += 2) {
      out.add(lines[i]);
    }
    return out;
  }

  @Test
  void theSameSeedYieldsTheSameBytes() {
    List<byte[]> first = bodies(spec(42L, SizeProfile.MIXED_LONG_TAIL), 25);
    List<byte[]> second = bodies(spec(42L, SizeProfile.MIXED_LONG_TAIL), 25);
    assertThat(first).hasSize(25);
    for (int i = 0; i < first.size(); i++) {
      assertThat(first.get(i))
          .as("batch %d of a replay must be byte-identical", i)
          .isEqualTo(second.get(i));
    }
  }

  @Test
  void aDifferentSeedYieldsDifferentBytes() {
    List<byte[]> first = bodies(spec(42L, SizeProfile.MIXED_LONG_TAIL), 25);
    List<byte[]> other = bodies(spec(43L, SizeProfile.MIXED_LONG_TAIL), 25);
    boolean anyDifferent = false;
    for (int i = 0; i < first.size(); i++) {
      anyDifferent |= !java.util.Arrays.equals(first.get(i), other.get(i));
    }
    assertThat(anyDifferent).as("seed 43 must not replay seed 42's bytes").isTrue();
  }

  @Test
  void theSameSeedYieldsTheSameStreams() {
    LoadGenerator a = new LoadGenerator(spec(7L, SizeProfile.MEDIUM_1KIB));
    LoadGenerator b = new LoadGenerator(spec(7L, SizeProfile.MEDIUM_1KIB));
    for (int i = 0; i < 20; i++) {
      assertThat(a.nextBatch().stream()).isEqualTo(b.nextBatch().stream());
    }
  }

  @Test
  void everyBodyIsBulkNdjsonWithOneActionLinePerDocument() {
    BulkBatch batch = new LoadGenerator(spec(1L, SizeProfile.SMALL_200B)).nextBatch();
    String text = new String(batch.body(), StandardCharsets.UTF_8);
    assertThat(text).endsWith("\n");
    assertThat(batch.documentCount()).isEqualTo(3);
    assertThat(actionLines(batch.body())).hasSize(3);
    assertThat(sourceLines(batch.body())).hasSize(3);
    for (String action : actionLines(batch.body())) {
      assertThat(action)
          .startsWith("{\"index\":{\"_index\":\"" + batch.stream().index() + "\"")
          .contains("\"_id\":\"")
          .endsWith("}}");
    }
    for (String source : sourceLines(batch.body())) {
      // A plausible document, not filler: the fields the real parse path walks.
      assertThat(source)
          .startsWith("{\"@timestamp\":\"")
          .contains("\"level\":\"")
          .contains("\"service\":\"")
          .contains("\"trace_id\":\"")
          .contains("\"message\":\"")
          .endsWith("\"}");
    }
  }

  @Test
  void eachFixedProfileEmitsSourceDocumentsOfExactlyItsSize() {
    assertExactSize(SizeProfile.SMALL_200B, 200);
    assertExactSize(SizeProfile.MEDIUM_1KIB, 1024);
    assertExactSize(SizeProfile.LARGE_10KIB, 10 * 1024);
  }

  private static void assertExactSize(SizeProfile profile, int expected) {
    LoadGenerator gen = new LoadGenerator(spec(11L, profile));
    for (int i = 0; i < 10; i++) {
      for (String source : sourceLines(gen.nextBatch().body())) {
        assertThat(source.getBytes(StandardCharsets.UTF_8).length)
            .as("%s document size", profile)
            .isEqualTo(expected);
      }
    }
  }

  @Test
  void theMixedProfileDrawsAllThreeNamedSizesAndALongTail() {
    List<Integer> sizes = mixedSizes(2026L, 4000);
    assertThat(sizes).hasSize(4000);
    assertThat(sizes).contains(200, 1024, 10 * 1024);
    assertThat(java.util.Collections.min(sizes)).isEqualTo(200);
    assertThat(java.util.Collections.max(sizes))
        .as("the tail is bounded at the 50 KiB stack trace research 40/03 names")
        .isLessThanOrEqualTo(50 * 1024);
    long tail = sizes.stream().filter(s -> s > 10 * 1024).count();
    assertThat(tail).as("the long tail must actually be drawn").isGreaterThan(0);
  }

  /**
   * ⚠️ The mixture's WEIGHTS, not merely its support. A generator drawing the
   * tail 50% of the time also "spans 200 B to 50 KiB", and would measure a
   * workload nothing in production resembles. Deterministic, because the seed is
   * fixed: these bands are a property of this seed and this mixture.
   */
  @Test
  void theMixedProfileHoldsItsStatedWeights() {
    List<Integer> sizes = mixedSizes(2026L, 4000);
    assertThat(fraction(sizes, s -> s == 200)).isBetween(0.66, 0.74);
    assertThat(fraction(sizes, s -> s == 1024)).isBetween(0.17, 0.23);
    assertThat(fraction(sizes, s -> s == 10 * 1024)).isBetween(0.06, 0.10);
    assertThat(fraction(sizes, s -> s > 10 * 1024)).isBetween(0.01, 0.035);
  }

  private static double fraction(List<Integer> sizes, java.util.function.IntPredicate p) {
    return sizes.stream().filter(s -> p.test(s)).count() / (double) sizes.size();
  }

  private static List<Integer> mixedSizes(long seed, int docs) {
    LoadGenerator gen =
        new LoadGenerator(new WorkloadSpec(seed, 4, 2, SizeProfile.MIXED_LONG_TAIL, 10));
    List<Integer> sizes = new ArrayList<>();
    while (sizes.size() < docs) {
      for (String source : sourceLines(gen.nextBatch().body())) {
        sizes.add(source.getBytes(StandardCharsets.UTF_8).length);
      }
    }
    return sizes.subList(0, docs);
  }

  @Test
  void batchesCoverEveryStreamTheSpecAsksFor() {
    LoadGenerator gen =
        new LoadGenerator(new WorkloadSpec(5L, 3, 2, SizeProfile.SMALL_200B, 1));
    List<StreamId> seen = new ArrayList<>();
    for (int i = 0; i < 60; i++) {
      StreamId s = gen.nextBatch().stream();
      if (!seen.contains(s)) {
        seen.add(s);
      }
    }
    assertThat(seen)
        .containsExactlyInAnyOrder(
            new StreamId("bench-index-0", 0),
            new StreamId("bench-index-0", 1),
            new StreamId("bench-index-1", 0),
            new StreamId("bench-index-1", 1),
            new StreamId("bench-index-2", 0),
            new StreamId("bench-index-2", 1));
  }

  @Test
  void aSpecWithNoStreamsIsRefused() {
    assertThatThrownBy(
            () -> new LoadGenerator(new WorkloadSpec(1L, 0, 1, SizeProfile.SMALL_200B, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("streamCount");
  }

  @Test
  void aSpecWithNoPartitionsIsRefused() {
    assertThatThrownBy(
            () -> new LoadGenerator(new WorkloadSpec(1L, 1, 0, SizeProfile.SMALL_200B, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partitionsPerIndex");
  }

  @Test
  void aSpecWithNoDocumentsPerBatchIsRefused() {
    assertThatThrownBy(
            () -> new LoadGenerator(new WorkloadSpec(1L, 1, 1, SizeProfile.SMALL_200B, 0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("docsPerBatch");
  }

  @Test
  void valueObjectsRetainValueSemantics() {
    StreamId stream = new StreamId("bench-index-0", 1);
    StreamId sameStream = new StreamId("bench-index-0", 1);
    assertThat(stream).isEqualTo(sameStream).hasSameHashCodeAs(sameStream);
    assertThat(stream.hashCode()).isNotZero();
    assertThat(stream.index()).isEqualTo("bench-index-0");
    assertThat(stream.partition()).isEqualTo(1);
    assertThat(stream).isNotEqualTo(new StreamId("bench-index-1", 1));
    assertThat(stream.toString()).contains("bench-index-0", "partition=1");

    WorkloadSpec spec = new WorkloadSpec(7L, 2, 3, SizeProfile.MEDIUM_1KIB, 4);
    WorkloadSpec sameSpec = new WorkloadSpec(7L, 2, 3, SizeProfile.MEDIUM_1KIB, 4);
    assertThat(spec).isEqualTo(sameSpec).hasSameHashCodeAs(sameSpec);
    assertThat(spec.hashCode()).isNotZero();
    assertThat(spec).isNotEqualTo(new WorkloadSpec(8L, 2, 3, SizeProfile.MEDIUM_1KIB, 4));
    assertThat(spec.toString()).contains("seed=7", "streamCount=2");

    BulkBatch batch = new BulkBatch(stream, 1, new byte[] {1, 2, 3});
    BulkBatch sameBatch = new BulkBatch(stream, 1, new byte[] {1, 2, 3});
    assertThat(batch).isEqualTo(sameBatch).hasSameHashCodeAs(sameBatch);
    assertThat(batch.hashCode()).isNotZero();
    assertThat(batch.equals(batch)).isTrue();
    assertThat(batch.equals("not a batch")).isFalse();
    assertThat(batch).isNotEqualTo(new BulkBatch(stream, 2, new byte[] {1, 2, 3}));
    assertThat(batch.toString()).contains("documentCount=1");
  }
}
