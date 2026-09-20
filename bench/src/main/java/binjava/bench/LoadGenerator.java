// SPDX-License-Identifier: Apache-2.0
package binjava.bench;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.SplittableRandom;

/** Produces deterministic bulk-NDJSON batches for a benchmark run. */
public final class LoadGenerator {
  private static final int SMALL_DOCUMENT_BYTES = 200;
  private static final int MEDIUM_DOCUMENT_BYTES = 1_024;
  private static final int LARGE_DOCUMENT_BYTES = 10 * 1_024;
  private static final int MAX_LONG_TAIL_BYTES = 50 * 1_024;

  private final WorkloadSpec spec;
  private final SplittableRandom random;
  private final int streamTotal;
  private long batchNumber;

  public LoadGenerator(WorkloadSpec spec) {
    this.spec = Objects.requireNonNull(spec, "spec");
    if (spec.streamCount() <= 0) {
      throw new IllegalArgumentException("streamCount must be positive");
    }
    if (spec.partitionsPerIndex() <= 0) {
      throw new IllegalArgumentException("partitionsPerIndex must be positive");
    }
    if (spec.docsPerBatch() <= 0) {
      throw new IllegalArgumentException("docsPerBatch must be positive");
    }
    Objects.requireNonNull(spec.sizeProfile(), "sizeProfile");
    streamTotal = Math.multiplyExact(spec.streamCount(), spec.partitionsPerIndex());
    random = new SplittableRandom(spec.seed());
  }

  public BulkBatch nextBatch() {
    int streamNumber = (int) (batchNumber++ % streamTotal);
    StreamId stream =
        new StreamId(
            "bench-index-" + (streamNumber / spec.partitionsPerIndex()),
            streamNumber % spec.partitionsPerIndex());
    StringBuilder body = new StringBuilder();
    for (int document = 0; document < spec.docsPerBatch(); document++) {
      String id = Long.toUnsignedString(random.nextLong(), 16);
      body.append(actionLine(stream, id)).append('\n');
      body.append(document(stream, id, documentSize())).append('\n');
    }
    return new BulkBatch(stream, spec.docsPerBatch(), body.toString().getBytes(StandardCharsets.UTF_8));
  }

  private int documentSize() {
    return switch (spec.sizeProfile()) {
      case SMALL_200B -> SMALL_DOCUMENT_BYTES;
      case MEDIUM_1KIB -> MEDIUM_DOCUMENT_BYTES;
      case LARGE_10KIB -> LARGE_DOCUMENT_BYTES;
      case MIXED_LONG_TAIL -> mixedDocumentSize();
    };
  }

  private int mixedDocumentSize() {
    return switch (random.nextInt(100)) {
      case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
          23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42,
          43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62,
          63, 64, 65, 66, 67, 68, 69 -> SMALL_DOCUMENT_BYTES;
      case 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89 ->
          MEDIUM_DOCUMENT_BYTES;
      case 90, 91, 92, 93, 94, 95, 96, 97 -> LARGE_DOCUMENT_BYTES;
      default -> LARGE_DOCUMENT_BYTES + random.nextInt(MAX_LONG_TAIL_BYTES - LARGE_DOCUMENT_BYTES + 1);
    };
  }

  private static String actionLine(StreamId stream, String id) {
    return "{\"index\":{\"_index\":\""
        + stream.index()
        + "\",\"_id\":\""
        + id
        + "\"}}";
  }

  private static String document(StreamId stream, String id, int size) {
    String prefix =
        "{\"@timestamp\":\"2026-09-20T00:00:00Z\",\"level\":\"INFO\",\"service\":\""
            + stream.index()
            + "\",\"trace_id\":\""
            + id
            + "\",\"message\":\"";
    String suffix = "\"}";
    int messageBytes = size - prefix.length() - suffix.length();
    return prefix + "x".repeat(messageBytes) + suffix;
  }
}
