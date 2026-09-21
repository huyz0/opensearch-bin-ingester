// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.bench;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.HdrHistogram.Histogram;

/** Writes the single JSON artifact produced by one assembled macro run. */
public record MacroRunResults(
    String runId, Map<String, ProcessStoreCounts> storeCounts, Histogram latencyHistogram) {

  public MacroRunResults {
    Objects.requireNonNull(runId, "runId");
    if (runId.isBlank()) {
      throw new IllegalArgumentException("runId must not be blank");
    }
    storeCounts = Map.copyOf(Objects.requireNonNull(storeCounts, "storeCounts"));
    if (storeCounts.isEmpty()) {
      throw new IllegalArgumentException("storeCounts must not be empty");
    }
    Objects.requireNonNull(latencyHistogram, "latencyHistogram");
  }

  /** Returns a stable, self-contained JSON summary without an I/O dependency. */
  public String toJson() {
    String counts =
        storeCounts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> countJson(entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(","));
    ByteBuffer encoded = ByteBuffer.allocate(latencyHistogram.getNeededByteBufferCapacity());
    int encodedLength = latencyHistogram.encodeIntoCompressedByteBuffer(encoded);
    String encodedHistogram = Base64.getEncoder().encodeToString(
        java.util.Arrays.copyOf(encoded.array(), encodedLength));
    return "{\"runId\":\""
        + escape(runId)
        + "\",\"storeCounts\":["
        + counts
        + "],\"latencyHistogram\":{\"type\":\"HdrHistogram\",\"encoding\":\"base64-compressed\",\"encoded\":\""
        + encodedHistogram
        + "\",\"totalCount\":"
        + latencyHistogram.getTotalCount()
        + ",\"minNanos\":"
        + latencyHistogram.getMinValue()
        + ",\"maxNanos\":"
        + latencyHistogram.getMaxValue()
        + ",\"p50Nanos\":"
        + latencyHistogram.getValueAtPercentile(50.0)
        + ",\"p95Nanos\":"
        + latencyHistogram.getValueAtPercentile(95.0)
        + ",\"p99Nanos\":"
        + latencyHistogram.getValueAtPercentile(99.0)
        + "}}\n";
  }

  private static String countJson(String podId, ProcessStoreCounts counts) {
    return "{\"podId\":\""
        + escape(podId)
        + "\",\"puts\":"
        + counts.puts()
        + ",\"gets\":"
        + counts.gets()
        + ",\"lists\":"
        + counts.lists()
        + ",\"stats\":"
        + counts.stats()
        + ",\"deletes\":"
        + counts.deletes()
        + "}";
  }

  private static String escape(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char character = value.charAt(i);
      switch (character) {
        case '\\' -> escaped.append("\\\\");
        case '\"' -> escaped.append("\\\"");
        case '\b' -> escaped.append("\\b");
        case '\f' -> escaped.append("\\f");
        case '\n' -> escaped.append("\\n");
        case '\r' -> escaped.append("\\r");
        case '\t' -> escaped.append("\\t");
        default -> {
          if (character < 0x20) {
            escaped.append(String.format("\\u%04x", (int) character));
          } else {
            escaped.append(character);
          }
        }
      }
    }
    return escaped.toString();
  }
}
