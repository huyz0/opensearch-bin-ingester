# M9 curve inputs

These files are the committed inputs to the generated `cost-latency-curve.md`
and `cost-latency-curve.svg`. Regenerate them with
`./gradlew generateCostLatencyCurve`; `./gradlew checkCostLatencyCurve` compares
both generated artifacts byte-for-byte and is part of the `gates` pre-commit
path.

| File | Provenance and interpretation |
|---|---|
| `size-triggered.csv` | M9.8 RustFS points at 40, 80 and 160 MiB/s. These are the recorded 5-second smoke observations, with a 1-second floor and 5-second configured ceiling. The full five-minute NFR-1 run remains outstanding. Requests/MiB are the measured rounded values; USD/TiB is derived from them and the AWS PUT price. |
| `low-rate.csv` | M9.18 RustFS smoke points at 250 ms and 5 s, as recorded in ADR-0062. The renderer derives elapsed ceiling windows and PUTs per interval. Requests/MiB and USD/TiB are intentionally not computed for this regime. |
| `visibility.csv` | M9.11's 10,000-record RustFS latency run at 250 ms, 1 s and 5 s. These local p99s are not S3 latency. |
| `codec.csv` | All 15 measured M9.7 B3 block/codec results. |
| `direct-threshold.csv` | The latest passing M9.14 RustFS JUnit output: GET counts, ingester bytes, process CPU ns/delivery and the configured mode for fan-outs 1–16. The M9.14 narrative table is aligned to this run. |
| `rig.properties` | Observed host/JDK/CPU, Docker engine resources, RustFS container limit and image digest, CPU-governor limitation, and the AWS S3 Standard request prices from the research cost model. |

All costs in the generated curve are modelled, not billed. S3 TTFB and a billed
AWS dollar result are NOT-RUN. Container counts and bytes are measurements;
RustFS does not stand in for S3 latency.
