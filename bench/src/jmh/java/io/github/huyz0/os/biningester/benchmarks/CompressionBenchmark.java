// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.benchmarks;

import com.github.luben.zstd.Zstd;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

/** B3's real-document compression shape; the complete matrix is M9.7. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(2)
@State(Scope.Benchmark)
public class CompressionBenchmark {
  // M9.6 gates one representative B3 codec. M9.7 expands the manual matrix
  // to zstd 1/3/6, lz4 and none across all three block sizes.
  @Param({"ZSTD"})
  public String codec;

  private byte[] input;
  private LZ4Compressor lz4;

  @Setup
  public void setup() {
    input = new byte[256 * 1024];
    byte[] line = "{\"_index\":\"logs-2026\",\"message\":\"mixed-document-tail\",\"level\":\"info\"}\n"
        .getBytes(StandardCharsets.UTF_8);
    for (int offset = 0; offset < input.length; offset += line.length)
      System.arraycopy(line, 0, input, offset, Math.min(line.length, input.length - offset));
    lz4 = LZ4Factory.fastestInstance().fastCompressor();
  }

  @Benchmark
  public void compress256KiB(Blackhole blackhole) {
    byte[] output;
    switch (codec) {
      case "NONE" -> output = input.clone();
      case "ZSTD" -> output = Zstd.compress(input, 3);
      case "LZ4" -> output = lz4.compress(input);
      default -> throw new IllegalStateException("unknown codec " + codec);
    }
    blackhole.consume(output);
  }
}
