// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.benchmarks;

import java.util.HashMap;
import java.util.Map;
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

/** JFR's {@code HashMap.resize()} allocation-pressure hotspot. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(2)
@State(Scope.Benchmark)
public class HashMapResizeBenchmark {
  @Benchmark
  public void resizeAcrossRealisticStreamCardinality(Blackhole blackhole) {
    Map<Integer, byte[]> map = new HashMap<>();
    for (int stream = 0; stream < 256; stream++) map.put(stream, new byte[64]);
    blackhole.consume(map);
  }
}
