# M9 curve inputs

These files are the committed inputs to the generated `cost-latency-curve.md`
and `cost-latency-curve.svg`. Regenerate them with
`./gradlew generateCostLatencyCurve`; `./gradlew checkCostLatencyCurve` compares
both generated artifacts byte-for-byte and is part of the `gates` pre-commit
path.

| File | Provenance and interpretation |
|---|---|
| `size-triggered.csv` | M9.8 RustFS points at 40, 80 and 160 MiB/s. Status and duration identify smoke versus full five-minute measurements; the configured interval is a 1-second floor and 5-second ceiling. Requests/MiB uses aggregate PUTs; USD/TiB is derived from that count and the AWS PUT price. |
| `low-rate.csv` | M9.56 RustFS points at 250 ms and 5 s with aggregate PUTs partitioned into data, commit delta/seal, checkpoint, lease, and other. The renderer derives elapsed ceiling windows and the data+commit-delta PUTs per interval. Every purpose remains included in aggregate PUTs; requests/MiB and USD/TiB are intentionally not computed for this regime. |
| `visibility.csv` | M9.11's 10,000-record RustFS latency run at 250 ms, 1 s and 5 s. These local p99s are not S3 latency. |
| `codec.csv` | All 15 measured M9.7 B3 block/codec results. |
| `direct-threshold.csv` | The latest passing M9.14 RustFS JUnit output: GET counts, ingester bytes, process CPU ns/delivery and the configured mode for fan-outs 1–16. The M9.14 narrative table is aligned to this run. |
| `rig.properties` | Observed host/JDK/CPU, Docker engine resources, RustFS container limit and image digest, CPU-governor limitation, and the AWS S3 Standard request prices from the research cost model. |

All costs in the generated curve are modelled, not billed. S3 TTFB and a billed
AWS dollar result are NOT-RUN. Container counts and bytes are measurements;
RustFS does not stand in for S3 latency.

The `CountingBinStore` PUT classifier adds one bounded-key-category
`LongAdder` update per PUT, with no additional object-store operation or
per-PUT string construction. M9.5's reproducible JFR allocation hotspots were
AWS chunk encoding and map allocation rather than the counter; this task makes
no throughput-optimization claim, and the full macro measurement remains the
request-cost evidence rather than a microbenchmark of counter overhead.
