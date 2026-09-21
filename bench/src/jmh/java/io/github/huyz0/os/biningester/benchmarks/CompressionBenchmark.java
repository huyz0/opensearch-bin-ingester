// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.benchmarks;

import com.github.luben.zstd.Zstd;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** B3's real-document compression matrix (M9.7). */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(2)
@State(Scope.Benchmark)
public class CompressionBenchmark {
  @Param({"ZSTD_1", "ZSTD_3", "ZSTD_6", "LZ4", "NONE"})
  public String codec;

  @Param({"64KiB", "256KiB", "1MiB"})
  public String blockSize;

  private byte[] input;
  private LZ4Compressor lz4;

  @AuxCounters(AuxCounters.Type.OPERATIONS)
  @State(Scope.Thread)
  public static class Counters {
    /** Compressed bytes per benchmark operation, used to derive ratio. */
    public long compressedBytes;
  }

  @Setup
  public void setup() {
    input = new byte[parseBlockSize(blockSize)];
    int offset = 0;
    for (int document = 0; offset < input.length; document++) {
      int targetSize = switch (document % 10) {
        case 0, 1, 2, 3, 4, 5, 6 -> 200;
        case 7, 8 -> 1024;
        default -> 10 * 1024;
      };
      byte[] record = document(document, targetSize);
      int copied = Math.min(record.length, input.length - offset);
      System.arraycopy(record, 0, input, offset, copied);
      offset += copied;
    }
    lz4 = LZ4Factory.fastestInstance().fastCompressor();
  }

  @Benchmark
  public void compressBlock(Counters counters, Blackhole blackhole) {
    byte[] output;
    switch (codec) {
      case "NONE" -> output = input.clone();
      case "ZSTD_1" -> output = Zstd.compress(input, 1);
      case "ZSTD_3" -> output = Zstd.compress(input, 3);
      case "ZSTD_6" -> output = Zstd.compress(input, 6);
      case "LZ4" -> output = lz4.compress(input);
      default -> throw new IllegalStateException("unknown codec " + codec);
    }
    counters.compressedBytes += output.length;
    blackhole.consume(output);
  }

  private static int parseBlockSize(String value) {
    return switch (value) {
      case "64KiB" -> 64 * 1024;
      case "256KiB" -> 256 * 1024;
      case "1MiB" -> 1024 * 1024;
      default -> throw new IllegalArgumentException("unknown block size " + value);
    };
  }

  private static byte[] document(int sequence, int targetSize) {
    StringBuilder json = new StringBuilder(targetSize + 64)
        .append("{\"_index\":\"logs-")
        .append(2026 + sequence % 3)
        .append("\",\"_id\":\"")
        .append(String.format(Locale.ROOT, "%08x", sequence * 2654435761L))
        .append("\",\"level\":\"")
        .append(sequence % 7 == 0 ? "error" : "info")
        .append("\",\"message\":\"");
    String alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 _-";
    while (json.length() < targetSize - 3) {
      json.append(alphabet.charAt((sequence + json.length() * 17) % alphabet.length()));
    }
    return json.append("\"}\n").toString().getBytes(StandardCharsets.UTF_8);
  }
}
