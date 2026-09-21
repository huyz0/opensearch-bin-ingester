// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.benchmarks;

import java.util.concurrent.ConcurrentHashMap;
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

/** JFR's {@code ConcurrentHashMap.initTable()} allocation-pressure hotspot. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(2)
@State(Scope.Benchmark)
public class ConcurrentHashMapInitBenchmark {
  @Benchmark
  public void initializeAndSeed(Blackhole blackhole) {
    ConcurrentHashMap<Integer, byte[]> map = new ConcurrentHashMap<>();
    map.put(1, new byte[256]);
    blackhole.consume(map);
  }
}
