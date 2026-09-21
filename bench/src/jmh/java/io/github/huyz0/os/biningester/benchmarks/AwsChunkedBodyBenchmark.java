// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.benchmarks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import software.amazon.awssdk.http.auth.aws.internal.signer.chunkedencoding.ChunkedEncodedInputStream;

/** JFR's AWS chunk parser allocation-pressure hotspot (M9.5, M9.6). */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(2)
@State(Scope.Benchmark)
public class AwsChunkedBodyBenchmark {
  private final byte[] payload = payload();
  private final byte[] sink = new byte[16 * 1024];

  @Benchmark
  public void decodeChunkedBody(Blackhole blackhole) throws IOException {
    InputStream decoded = ChunkedEncodedInputStream.builder()
        .inputStream(new ByteArrayInputStream(payload))
        .chunkSize(16 * 1024)
        .header(this::chunkHeader)
        .build();
    int bytes = 0;
    int read;
    while ((read = decoded.read(sink)) != -1) bytes += read;
    blackhole.consume(bytes);
  }

  private byte[] chunkHeader(ByteBuffer body) {
    return (Integer.toHexString(body.remaining()) + "\r\n").getBytes(StandardCharsets.US_ASCII);
  }

  private static byte[] payload() {
    byte[] bytes = new byte[256 * 1024];
    byte[] line = "{\"_index\":\"logs-2026\",\"message\":\"allocation-profile\"}\n"
        .getBytes(StandardCharsets.UTF_8);
    for (int offset = 0; offset < bytes.length; offset += line.length)
      System.arraycopy(line, 0, bytes, offset, Math.min(line.length, bytes.length - offset));
    return bytes;
  }
}
