// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The allocation gate reads the recorded JMH allocation metric (M9.6). */
class AllocationGateTest {

  @Test
  void passesTheRecordedAllocationMetricWithinItsDerivedThreshold(@TempDir Path dir)
      throws Exception {
    Path baseline = write(dir, "baseline.json", baselineJson());
    Path result = write(dir, "result.json", jmhResult(10.0, 104.0));

    Result checked = run(baseline, result);

    assertThat(checked.exit()).as(checked.output()).isZero();
    assertThat(checked.output()).contains("1 benchmark(s) passed");
  }

  @Test
  void failsWhenAllocationExceedsTheDerivedThresholdEvenIfThroughputLooksGood(
      @TempDir Path dir) throws Exception {
    Path baseline = write(dir, "baseline.json", baselineJson());
    Path result = write(dir, "result.json", jmhResult(1.0, 106.0));

    Result checked = run(baseline, result);

    assertThat(checked.exit()).as(checked.output()).isOne();
    assertThat(checked.output()).contains("106").contains("105");
  }

  @Test
  void refusesAResultThatHasNoAllocationMetric(@TempDir Path dir) throws Exception {
    Path baseline = write(dir, "baseline.json", baselineJson());
    Path result = write(dir, "result.json", """
        [{
          "benchmark": "example.Benchmark.work",
          "primaryMetric": {"score": 1.0, "scoreError": 0.1, "scoreUnit": "ops/s"},
          "secondaryMetrics": {}
        }]
        """);

    Result checked = run(baseline, result);

    assertThat(checked.exit()).as(checked.output()).isOne();
    assertThat(checked.output()).contains("gc.alloc.rate.norm");
  }

  @Test
  void refusesABaselineWhoseThresholdWasNotDerivedFromItsNoiseFloor(@TempDir Path dir)
      throws Exception {
    Path baseline = write(dir, "baseline.json", baselineJson().replace("5.0", "20.0"));
    Path result = write(dir, "result.json", jmhResult(10.0, 104.0));

    Result checked = run(baseline, result);

    assertThat(checked.exit()).as(checked.output()).isOne();
    assertThat(checked.output()).contains("thresholdPct");
  }

  @Test
  void acceptsABenchmarkDroppedBecauseThreeTimesNoiseExceedsTheCap(@TempDir Path dir)
      throws Exception {
    Path baseline = write(dir, "baseline.json", """
        {
          "schema": 1,
          "benchmarks": {
            "example.Benchmark.work": {
              "baselineBop": 100.0,
              "variancePct": 10.0,
              "thresholdPct": 25.0,
              "samples": 10,
              "status": "dropped-too-noisy"
            }
          }
        }
        """);
    Path result = write(dir, "result.json", "[]");

    Result checked = run(baseline, result);

    assertThat(checked.exit()).as(checked.output()).isZero();
    assertThat(checked.output()).contains("dropped");
  }

  @Test
  void ignoresCurrentResultsForABenchmarkDroppedBecauseItIsTooNoisy(@TempDir Path dir)
      throws Exception {
    Path baseline = write(dir, "baseline.json", """
        {
          "schema": 1,
          "benchmarks": {
            "example.Benchmark.work": {
              "baselineBop": 100.0,
              "variancePct": 10.0,
              "thresholdPct": 25.0,
              "samples": 10,
              "status": "dropped-too-noisy"
            }
          }
        }
        """);
    Path result = write(dir, "result.json", jmhResult(10.0, 104.0));

    Result checked = run(baseline, result);

    assertThat(checked.exit()).as(checked.output()).isZero();
    assertThat(checked.output()).contains("dropped");
  }

  private static String baselineJson() {
    return """
        {
          "schema": 1,
          "benchmarks": {
            "example.Benchmark.work": {
              "baselineBop": 100.0,
              "variancePct": 1.0,
              "thresholdPct": 5.0,
              "samples": 10,
              "status": "gated"
            }
          }
        }
        """;
  }

  private static String jmhResult(double throughput, double allocation) {
    return """
        [{
          "benchmark": "example.Benchmark.work",
          "primaryMetric": {"score": %s, "scoreError": 0.1, "scoreUnit": "ops/s"},
          "secondaryMetrics": {
            "gc.alloc.rate.norm": {"score": %s, "scoreError": 0.1, "scoreUnit": "B/op"}
          }
        }]
        """.formatted(throughput, allocation);
  }

  private static Path write(Path dir, String name, String content) throws Exception {
    Path path = dir.resolve(name);
    Files.writeString(path, content);
    return path;
  }

  private static Result run(Path baseline, Path result) throws Exception {
    Path repo = Path.of("..").toAbsolutePath().normalize();
    Process process = new ProcessBuilder(
            "python", "scripts/check-allocation-gate.py",
            "--baseline", baseline.toString(), "--results", result.toString())
        .directory(repo.toFile())
        .redirectErrorStream(true)
        .start();
    String output = new String(process.getInputStream().readAllBytes());
    return new Result(process.waitFor(), output);
  }

  private record Result(int exit, String output) {}
}
