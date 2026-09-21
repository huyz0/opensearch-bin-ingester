#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check JMH allocation results against a noise-derived committed baseline.

The only metric this gate reads is JMH's ``gc.alloc.rate.norm`` secondary
metric.  Throughput remains a trend and cannot make an allocation regression
green (M9.6, criterion 10).
"""

import argparse
import json
import math
import sys
from pathlib import Path


METRIC = "gc.alloc.rate.norm"
CAP_PCT = 25.0
FLOOR_PCT = 5.0
MIN_SAMPLES = 10


def fail(message):
    print(f"FAIL allocation gate: {message}")
    return 1


def finite_number(value, label):
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite number")
    return float(value)


def load(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc


def benchmark_name(result):
    name = result.get("benchmark")
    params = result.get("params")
    if isinstance(params, dict) and params:
        suffix = ",".join(f"{key}={params[key]}" for key in sorted(params))
        return f"{name}({suffix})"
    return name


def check(baseline_path, results_path):
    try:
        baseline = load(baseline_path)
        results = load(results_path)
        if baseline.get("schema") != 1:
            raise ValueError("baseline schema must be 1")
        specs = baseline.get("benchmarks")
        if not isinstance(specs, dict) or not specs:
            raise ValueError("baseline benchmarks must be a non-empty object")
        if not isinstance(results, list):
            raise ValueError("JMH results must be an array")

        result_by_name = {}
        for result in results:
            name = benchmark_name(result)
            if not isinstance(name, str) or not name:
                raise ValueError("every JMH result needs a benchmark name")
            if name in result_by_name:
                raise ValueError(f"duplicate JMH result for {name}")
            result_by_name[name] = result

        passed = 0
        dropped = 0
        for name, spec in specs.items():
            if not isinstance(spec, dict):
                raise ValueError(f"{name}: baseline entry must be an object")
            samples = spec.get("samples")
            if not isinstance(samples, int) or isinstance(samples, bool) or samples < MIN_SAMPLES:
                raise ValueError(f"{name}: samples must be at least {MIN_SAMPLES}")
            variance = finite_number(spec.get("variancePct"), f"{name}.variancePct")
            if variance < 0:
                raise ValueError(f"{name}: variancePct cannot be negative")
            derived = min(CAP_PCT, max(FLOOR_PCT, 3.0 * variance))
            threshold = finite_number(spec.get("thresholdPct"), f"{name}.thresholdPct")
            if abs(threshold - derived) > 1e-9:
                raise ValueError(
                    f"{name}: thresholdPct {threshold:g} is not derived from variancePct "
                    f"{variance:g} (expected {derived:g})"
                )

            status = spec.get("status")
            too_noisy = 3.0 * variance > CAP_PCT
            if status == "dropped-too-noisy":
                if not too_noisy:
                    raise ValueError(f"{name}: dropped benchmark is not too noisy")
                result_by_name.pop(name, None)
                dropped += 1
                continue
            if status != "gated":
                raise ValueError(f"{name}: status must be gated or dropped-too-noisy")
            if too_noisy:
                raise ValueError(f"{name}: noisy benchmark must be dropped, not gated")

            result = result_by_name.pop(name, None)
            if result is None:
                raise ValueError(f"{name}: no current JMH result")
            secondary = result.get("secondaryMetrics", {})
            metric = secondary.get(METRIC) if isinstance(secondary, dict) else None
            if not isinstance(metric, dict) or "score" not in metric:
                raise ValueError(f"{name}: missing secondary metric {METRIC}")
            actual = finite_number(metric["score"], f"{name}.{METRIC}.score")
            baseline_bop = finite_number(spec.get("baselineBop"), f"{name}.baselineBop")
            if baseline_bop <= 0:
                raise ValueError(f"{name}: baselineBop must be positive")
            allowed = baseline_bop * (1.0 + threshold / 100.0)
            if actual > allowed:
                raise ValueError(
                    f"{name}: {METRIC} {actual:g} B/op exceeds {allowed:g} B/op "
                    f"(baseline {baseline_bop:g}, threshold {threshold:g}%)"
                )
            passed += 1

        if result_by_name:
            raise ValueError("current results contain unlisted benchmark(s): "
                             + ", ".join(sorted(result_by_name)))
        print(f"OK allocation gate: {passed} benchmark(s) passed, {dropped} dropped as too noisy")
        return 0
    except ValueError as exc:
        return fail(str(exc))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--results", required=True, type=Path)
    args = parser.parse_args()
    return check(args.baseline, args.results)


if __name__ == "__main__":
    sys.exit(main())
