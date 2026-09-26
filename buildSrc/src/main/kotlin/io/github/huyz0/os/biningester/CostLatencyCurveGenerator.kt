// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import java.math.BigDecimal
import java.math.RoundingMode
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import kotlin.math.ceil

/** Source-of-truth renderer for M9's cost and latency evidence. */
object CostLatencyCurveGenerator {

    private const val M914_SEGMENT_BYTES = 64L * 1024L

    private val sizeHeader = listOf(
        "rate_mib_s", "puts", "requests_per_mib", "duration_seconds",
        "interval_floor_ms", "interval_ceiling_ms", "status")
    private val lowRateHeader = listOf(
        "ceiling_ms", "total_puts", "data_puts", "commit_puts", "checkpoint_puts",
        "lease_puts", "other_puts", "elapsed_seconds", "status")
    private val visibilityHeader = listOf(
        "ceiling_ms", "records", "p50_ms", "p99_ms", "max_ms", "bound_3x_ms", "status")
    private val codecHeader = listOf("block_bytes", "codec", "mib_per_s_core", "ratio", "bytes_per_op")
    private val directHeader = listOf(
        "fanout", "proxy_gets", "direct_gets", "proxy_ingester_bytes",
        "direct_ingester_bytes", "proxy_cpu_ns", "direct_cpu_ns", "default_mode")

    @JvmStatic
    fun render(results: Path): String {
        val size = csv(results.resolve("size-triggered.csv"), sizeHeader)
        val lowRate = csv(results.resolve("low-rate.csv"), lowRateHeader)
        val visibility = csv(results.resolve("visibility.csv"), visibilityHeader)
        val codecs = csv(results.resolve("codec.csv"), codecHeader)
        val direct = csv(results.resolve("direct-threshold.csv"), directHeader)
        val rig = Properties().also { properties ->
            Files.newInputStream(results.resolve("rig.properties")).use(properties::load)
        }
        validate(size, lowRate, visibility, codecs, direct, rig)

        return buildString {
            appendLine("# Cost and latency curve")
            appendLine()
            appendLine("Generated from the committed files in [results](results/); do not edit this document by hand.")
            appendLine("Counts are measured, dollar values are modelled from the AWS S3 Standard price table, and latency is measured on RustFS on this rig.")
            appendLine("![Measured and modelled M9 cost/latency plots](cost-latency-curve.svg)")
            appendLine()
            appendLine("## Size-triggered writes")
            appendLine()
            appendLine("| Target rate (MiB/s) | Configured interval floor / ceiling | PUTs | Requests/MiB (measured) | USD/TiB (modelled) | Duration / evidence |")
            appendLine("|---:|---:|---:|---:|---:|---|")
            size.forEach { row ->
                val rate = row.decimal("rate_mib_s")
                val requestsPerMib = row.decimal("requests_per_mib")
                val dollars = requestsPerMib
                    .multiply(BigDecimal("1048576"))
                    .multiply(rig.getProperty("aws_put_usd_per_1000").toBigDecimal())
                    .divide(BigDecimal("1000"), 8, RoundingMode.HALF_UP)
                appendLine("| ${fmt(rate)} | ${row.value("interval_floor_ms")} ms / ${row.value("interval_ceiling_ms")} ms | ${row.value("puts")} | ${fmt(requestsPerMib)} | \$${fmt(dollars, 4)} | ${row.value("duration_seconds")} s, ${row.value("status")} |")
            }
            appendLine()
            val sizeProfileComplete = size.all {
                it.value("status") == "measured"
                    && it.decimal("duration_seconds") >= BigDecimal("300")
            }
            appendLine("USD/TiB uses binary 1,048,576 MiB/TiB and the measured PUTs/MiB multiplied by the published AWS PUT price. It is not a billed result. " + if (sizeProfileComplete) "All M9.8 points are measured for at least five minutes." else "The M9.8 five-minute acceptance run remains outstanding for any point labelled smoke or shorter than five minutes.")
            appendLine("Source: M9.8's recorded RustFS counts and [ADR-0072](../decisions/0072-attribute-low-rate-lease-put-cost.md) for the low-rate interpretation.")
            appendLine()
            appendLine("## Low-rate write budget")
            appendLine()
            appendLine("The low-rate budget is measured segment-data plus commit-delta PUTs per interval, not requests per MiB or dollars per TiB; checkpoint and lease/control PUTs are separately cadence-bounded but remain in aggregate totals.")
            appendLine()
            appendLine("| Interval ceiling | Total PUTs | Data + commit PUTs | Checkpoint PUTs | Lease PUTs | Other PUTs | Elapsed | Ceiling windows | Data + commit / interval | Requests/MiB and USD/TiB |")
            appendLine("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
            lowRate.forEach { row ->
                val ceiling = row.long("ceiling_ms")
                val elapsed = row.decimal("elapsed_seconds")
                val windows = ceil(elapsed.toDouble() * 1000.0 / ceiling).toLong()
                val putsPerInterval = (row.long("data_puts") + row.long("commit_puts")).toBigDecimal()
                    .divide(windows.toBigDecimal(), 4, RoundingMode.HALF_UP)
                val dataAndCommit = row.long("data_puts") + row.long("commit_puts")
                appendLine("| ${fmt(ceiling.toBigDecimal().divide(BigDecimal("1000")))} s | ${row.value("total_puts")} | $dataAndCommit | ${row.value("checkpoint_puts")} | ${row.value("lease_puts")} | ${row.value("other_puts")} | ${fmt(elapsed)} s | $windows | ${fmt(putsPerInterval)} | N/A — low-rate requests/MiB is not a valid budget (${row.value("status")}) |")
            }
            appendLine()
            appendLine("## Visibility latency by interval ceiling")
            appendLine()
            appendLine("| Ceiling | Records | p50 (RustFS measured) | p99 (RustFS measured) | Max (RustFS measured) | 3× bound |")
            appendLine("|---:|---:|---:|---:|---:|---:|")
            visibility.forEach { row ->
                appendLine("| ${fmt(row.long("ceiling_ms").toBigDecimal().divide(BigDecimal("1000")))} s | ${row.value("records")} | ${fmt(row.decimal("p50_ms"))} ms | ${fmt(row.decimal("p99_ms"))} ms | ${fmt(row.decimal("max_ms"))} ms | ${fmt(row.long("bound_3x_ms").toBigDecimal().divide(BigDecimal("1000")))} s |")
            }
            appendLine()
            appendLine("RustFS loopback omits the production S3 PUT tail. S3 p99, S3 TTFB, and billed dollars are NOT-RUN; these p99s are local measurements and a lower bound for production visibility.")
            appendLine("Source: [M9.11 visibility latency](m9.11-visibility-latency.md).")
            appendLine()
            appendLine("## Codec trend (M2 / M9.7)")
            appendLine()
            appendLine("| Block | Codec | MiB/s per core (measured) | Ratio (measured) | B/op (measured) |")
            appendLine("|---:|---|---:|---:|---:|")
            codecs.forEach { row ->
                appendLine("| ${fmt(row.long("block_bytes").toBigDecimal().divide(BigDecimal("1024")))} KiB | ${row.value("codec")} | ${fmt(row.decimal("mib_per_s_core"))} | ${fmt(row.decimal("ratio"))}× | ${row.value("bytes_per_op")} |")
            }
            appendLine()
            appendLine("Source: [M9.7 codec comparison](m9.7-codec-comparison.md).")
            appendLine()
            appendLine("## Direct-mode threshold (M3 / M9.14)")
            appendLine()
            appendLine("| Fan-out | Proxy GETs/segment | Direct GETs/segment | Proxy bytes through ingester | Direct bytes through ingester | Proxy CPU ns/delivery | Direct CPU ns/delivery | Default |")
            appendLine("|---:|---:|---:|---:|---:|---:|---:|---|")
            direct.forEach { row ->
                appendLine("| ${row.value("fanout")} | ${row.value("proxy_gets")} | ${row.value("direct_gets")} | ${row.value("proxy_ingester_bytes")} | ${row.value("direct_ingester_bytes")} | ${fmt(row.decimal("proxy_cpu_ns"))} | ${fmt(row.decimal("direct_cpu_ns"))} | ${row.value("default_mode")} |")
            }
            appendLine()
            appendLine("Default direct threshold: **1**, the only fan-out with request-cost parity (one GET on either path). Above one, proxy stays at one cold ingester GET while direct costs N signed-URL GETs. CPU is coarse process CPU time, not a price input. Real S3 TTFB crossover: **NOT-RUN**. See [M9.14 details](m9.14-direct-threshold.md) and [ADR-0067](../decisions/0067-direct-threshold-cost-parity-boundary.md).")
            appendLine()
            appendLine("## Measurement rig")
            appendLine()
            appendLine("- Host OS: ${rig.getProperty("os")}")
            appendLine("- JDK: ${rig.getProperty("jdk")}")
            appendLine("- CPU: ${rig.getProperty("cpu")}")
            appendLine("- Container engine: ${rig.getProperty("container_engine")}")
            appendLine("- RustFS container limits: ${rig.getProperty("rustfs_limits")}")
            appendLine("- RustFS: ${rig.getProperty("rustfs")}")
            appendLine("- CPU governor: ${rig.getProperty("cpu_governor")}")
            appendLine("- Price source: AWS S3 Standard us-east-1, Aug 2026; PUT \$${rig.getProperty("aws_put_usd_per_1000")} / 1,000, GET \$${rig.getProperty("aws_get_usd_per_1000")} / 1,000; same-region S3-to-EC2 transfer \$0.")
            appendLine()
            appendLine("The repository cost-meter gate is not implemented yet; it did not run. This report's generator gate checks source/report consistency, not runtime request budgets.")
        }
    }

    @JvmStatic
    fun renderSvg(results: Path): String {
        val lowRate = csv(results.resolve("low-rate.csv"), lowRateHeader)
        val visibility = csv(results.resolve("visibility.csv"), visibilityHeader)
        val size = csv(results.resolve("size-triggered.csv"), sizeHeader)
        val putPrice = Properties().also { properties ->
            Files.newInputStream(results.resolve("rig.properties")).use(properties::load)
        }.getProperty("aws_put_usd_per_1000").toBigDecimal()
        val visibilityPoints = visibility.mapIndexed { index, row ->
            Pair(120.0 + index * 300.0,
                230.0 - row.decimal("p99_ms").toDouble() / 15.0 * 140.0)
        }
        val lowRatePoints = lowRate.sortedBy { it.long("ceiling_ms") }.mapIndexed { index, row ->
            val windows = ceil(row.decimal("elapsed_seconds").toDouble() * 1000.0
                / row.long("ceiling_ms")).toLong()
            val perInterval = (row.long("data_puts") + row.long("commit_puts")).toDouble() / windows
            Pair(120.0 + index * 600.0, 445.0 - perInterval / 2.5 * 95.0)
        }
        val costPoints = size.sortedBy { it.long("rate_mib_s") }.mapIndexed { index, row ->
            val rate = row.long("rate_mib_s")
            val requests = row.decimal("requests_per_mib")
            val dollars = requests.multiply(BigDecimal("1048576")).multiply(putPrice)
                .divide(BigDecimal("1000"), 8, RoundingMode.HALF_UP)
            Pair(120.0 + index * 300.0,
                670.0 - (requests.toDouble() - 0.25) / 0.05 * 110.0 to
                    670.0 - (dollars.toDouble() - 1.3) / 0.3 * 110.0)
        }
        return buildString {
            appendLine("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"900\" height=\"820\" viewBox=\"0 0 900 820\">")
            appendLine("<style>text{font:14px sans-serif;fill:#202124}.title{font-size:18px;font-weight:600}.grid{stroke:#d8dce2;stroke-width:1}.p99{fill:none;stroke:#2563eb;stroke-width:3}.low{fill:none;stroke:#d97706;stroke-width:3}.req{fill:none;stroke:#15803d;stroke-width:3}.cost{fill:none;stroke:#7c3aed;stroke-width:3}</style>")
            appendLine("<text x=\"32\" y=\"30\" class=\"title\">M9 cost and latency evidence — measured counts, modelled dollars</text>")
            appendLine("<text x=\"32\" y=\"58\" class=\"title\">Visibility p99 vs configured interval ceiling (RustFS measured, ms)</text>")
            appendLine("<path class=\"grid\" d=\"M100 90H820 M100 160H820 M100 230H820\"/>")
            appendLine("<path class=\"p99\" d=\"${polyline(visibilityPoints)}\"/>")
            visibilityPoints.forEachIndexed { index, point ->
                appendLine("<circle cx=\"${coord(point.first)}\" cy=\"${coord(point.second)}\" r=\"5\" fill=\"#2563eb\"/><text x=\"${coord(point.first - 30)}\" y=\"${coord(point.second - 10)}\">${visibility[index].value("p99_ms")} ms</text>")
            }
            visibility.forEachIndexed { index, row ->
                appendLine("<text x=\"${120 + index * 300}\" y=\"255\">${intervalLabel(row.long("ceiling_ms"))}</text>")
            }
            appendLine("<text x=\"32\" y=\"300\" class=\"title\">Low-rate data+commit PUTs per interval vs ceiling (measured)</text>")
            appendLine("<path class=\"grid\" d=\"M100 350H820 M100 397H820 M100 445H820\"/>")
            appendLine("<path class=\"low\" d=\"${polyline(lowRatePoints)}\"/>")
            lowRatePoints.forEachIndexed { index, point ->
                val row = lowRate.sortedBy { it.long("ceiling_ms") }[index]
                val windows = ceil(row.decimal("elapsed_seconds").toDouble() * 1000.0
                    / row.long("ceiling_ms")).toLong()
                val value = (row.long("data_puts") + row.long("commit_puts")).toDouble() / windows
                appendLine("<circle cx=\"${coord(point.first)}\" cy=\"${coord(point.second)}\" r=\"5\" fill=\"#d97706\"/><text x=\"${coord(point.first - 20)}\" y=\"${coord(point.second - 10)}\">${fmt(BigDecimal.valueOf(value), 2)}</text><text x=\"${coord(point.first - 25)}\" y=\"465\">${intervalLabel(row.long("ceiling_ms"))}</text>")
            }
            appendLine("<text x=\"32\" y=\"515\" class=\"title\">Size-triggered write cost at 5 s configured ceiling</text>")
            appendLine("<path class=\"grid\" d=\"M100 560H820 M100 615H820 M100 670H820\"/>")
            appendLine("<path class=\"req\" d=\"${polyline(costPoints.map { Pair(it.first, it.second.first) })}\"/>")
            appendLine("<path class=\"cost\" d=\"${polyline(costPoints.map { Pair(it.first, it.second.second) })}\"/>")
            costPoints.forEachIndexed { index, point ->
                val row = size.sortedBy { it.long("rate_mib_s") }[index]
                val requests = row.decimal("requests_per_mib")
                val dollars = requests.multiply(BigDecimal("1048576")).multiply(putPrice)
                    .divide(BigDecimal("1000"), 8, RoundingMode.HALF_UP)
                appendLine("<circle cx=\"${coord(point.first)}\" cy=\"${coord(point.second.first)}\" r=\"5\" fill=\"#15803d\"/><circle cx=\"${coord(point.first)}\" cy=\"${coord(point.second.second)}\" r=\"5\" fill=\"#7c3aed\"/><text x=\"${coord(point.first + 8)}\" y=\"${coord(point.second.first - 7)}\">${fmt(requests)} requests/MiB</text><text x=\"${coord(point.first + 8)}\" y=\"${coord(point.second.second + 15)}\">\$${fmt(dollars, 4)}/TiB</text><text x=\"${coord(point.first - 25)}\" y=\"705\">${row.value("rate_mib_s")} MiB/s</text>")
            }
            appendLine("<line x1=\"120\" y1=\"745\" x2=\"150\" y2=\"745\" class=\"req\"/><text x=\"158\" y=\"750\">requests/MiB measured</text><line x1=\"390\" y1=\"745\" x2=\"420\" y2=\"745\" class=\"cost\"/><text x=\"428\" y=\"750\">USD/TiB modelled</text>")
            appendLine("<text x=\"32\" y=\"780\">Low-rate requests/MiB: N/A. Billed AWS dollars: NOT-RUN.</text>")
            val fullSizeProfile = size.all {
                it.value("status") == "measured"
                    && it.decimal("duration_seconds") >= BigDecimal("300")
            }
            appendLine("<text x=\"32\" y=\"805\">Size-triggered points: 5 s configured ceiling, 1 s floor, ${if (fullSizeProfile) "five-minute measured runs" else "smoke runs"}.</text>")
            appendLine("</svg>")
        }
    }

    @JvmStatic
    fun check(results: Path, output: Path, svgOutput: Path) {
        val expected = render(results)
        if (!Files.exists(output) || normalizeNewlines(Files.readString(output)) != expected
            || !Files.exists(svgOutput)
            || normalizeNewlines(Files.readString(svgOutput)) != renderSvg(results)) {
            throw IllegalStateException("cost-latency curve is stale; run generateCostLatencyCurve")
        }
    }

    private fun normalizeNewlines(value: String): String = value.replace("\r\n", "\n")

    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 4 && (args[0] == "--check" || args[0] == "--write")) {
            "usage: CostLatencyCurveGenerator --check|--write <results-directory> <output-file> <svg-file>"
        }
        val results = Path.of(args[1]).toAbsolutePath().normalize()
        val output = Path.of(args[2]).toAbsolutePath().normalize()
        val svgOutput = Path.of(args[3]).toAbsolutePath().normalize()
        if (args[0] == "--check") {
            check(results, output, svgOutput)
        } else {
            Files.createDirectories(output.parent)
            Files.writeString(output, render(results))
            Files.writeString(svgOutput, renderSvg(results))
        }
    }

    private fun polyline(points: List<Pair<Double, Double>>): String =
        points.joinToString(" ") { "${coord(it.first)},${coord(it.second)}" }
            .let { "M$it" }

    private fun coord(value: Double): String = BigDecimal.valueOf(value)
        .setScale(2, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString()

    private fun intervalLabel(millis: Long): String =
        if (millis < 1000) "${millis} ms" else "${millis / 1000} s"

    private fun validate(
        size: List<Row>,
        lowRate: List<Row>,
        visibility: List<Row>,
        codecs: List<Row>,
        direct: List<Row>,
        rig: Properties,
    ) {
        require(size.map { it.long("rate_mib_s") }.toSet() == setOf(40L, 80L, 160L)) {
            "the committed M9.8 result set must contain the 40, 80, and 160 MiB/s points"
        }
        require(size.all { row ->
            row.long("puts") > 0L
                && row.decimal("rate_mib_s").signum() > 0
                && row.decimal("duration_seconds").signum() > 0
                && row.long("interval_floor_ms") > 0L
                && row.long("interval_ceiling_ms") >= row.long("interval_floor_ms")
                && row.decimal("requests_per_mib").signum() > 0
                && (row.value("status") == "smoke" || row.value("status") == "measured")
        }) { "size-triggered results must contain positive measured values and valid interval bounds" }
        require(lowRate.map { it.long("ceiling_ms") }.toSet().containsAll(setOf(250L, 5000L)))
        require(lowRate.all { row ->
            val ceilingMs = row.long("ceiling_ms")
            val totalPuts = row.long("total_puts")
            val dataPuts = row.long("data_puts")
            val commitPuts = row.long("commit_puts")
            val checkpointPuts = row.long("checkpoint_puts")
            val leasePuts = row.long("lease_puts")
            val otherPuts = row.long("other_puts")
            val elapsed = row.decimal("elapsed_seconds")
            if (ceilingMs <= 0L || totalPuts <= 0L || dataPuts < 0L || commitPuts < 0L
                || checkpointPuts < 0L || leasePuts < 0L || otherPuts < 0L
                || dataPuts + commitPuts + checkpointPuts + leasePuts + otherPuts != totalPuts
                || elapsed.signum() <= 0
                || row.value("status") !in setOf("smoke", "measured")) {
                false
            } else {
                val intervals = ceil(elapsed.toDouble() * 1000.0 / ceilingMs).toLong()
                val dataAndCommit = BigDecimal.valueOf(dataPuts + commitPuts)
                val maximumPuts = BigDecimal.valueOf(intervals)
                    .multiply(BigDecimal("2")).add(BigDecimal("2"))
                intervals > 0L && dataAndCommit.compareTo(maximumPuts) <= 0
            }
        }) { "low-rate PUT categories must reconcile and data+commit stay within the per-interval budget" }
        require(visibility.map { it.long("ceiling_ms") }.toSet().containsAll(setOf(250L, 1000L, 5000L)))
        require(visibility.all { row ->
            row.long("records") > 0L && row.decimal("p50_ms").signum() > 0
                && row.decimal("p50_ms") <= row.decimal("p99_ms")
                && row.decimal("p99_ms") <= row.decimal("max_ms")
                && row.decimal("p99_ms") < row.long("bound_3x_ms").toBigDecimal()
                && row.value("status") == "measured"
        })
        val codecPoints = codecs.map { "${it.long("block_bytes")}:${it.value("codec")}" }.toSet()
        val expectedCodecPoints = listOf(65_536L, 262_144L, 1_048_576L)
            .flatMap { block -> listOf("zstd-1", "zstd-3", "zstd-6", "lz4", "none")
                .map { codec -> "$block:$codec" } }.toSet()
        require(codecPoints == expectedCodecPoints) { "M9.7 requires each of the 15 block/codec measurements once" }
        require(codecs.all { row ->
            row.decimal("mib_per_s_core").signum() > 0
                && row.decimal("ratio").signum() > 0
                && row.long("bytes_per_op") > 0L
        }) { "codec results must be positive measurements" }
        require(direct.size == 16 && direct.map { it.long("fanout") }.toSet() == (1L..16L).toSet()) {
            "M9.14 results must contain exactly one row for each fan-out from 1 through 16"
        }
        require(direct.all { row ->
            row.long("proxy_gets") == 1L && row.long("direct_gets") == row.long("fanout")
        })
        require(direct.all { row ->
            val fanout = row.long("fanout")
            row.long("proxy_ingester_bytes") == fanout * M914_SEGMENT_BYTES
                && row.long("direct_ingester_bytes") == 0L
                && row.decimal("proxy_cpu_ns").signum() >= 0
                && row.decimal("direct_cpu_ns").signum() >= 0
        }) { "M9.14 byte counts must match the 64 KiB proxy/direct serving paths" }
        require(direct.single { it.long("fanout") == 1L }.value("default_mode") == "direct")
        require(direct.filter { it.long("fanout") > 1L }.all { it.value("default_mode") == "proxy" })
        listOf("os", "jdk", "cpu", "container_engine", "rustfs_limits", "rustfs", "cpu_governor",
            "aws_put_usd_per_1000", "aws_get_usd_per_1000").forEach { key ->
            require(!rig.getProperty(key).isNullOrBlank()) { "rig.properties missing $key" }
        }
        require(rig.getProperty("aws_put_usd_per_1000").toBigDecimal().signum() > 0
            && rig.getProperty("aws_get_usd_per_1000").toBigDecimal().signum() > 0) {
            "AWS request prices must be positive"
        }
    }

    private fun csv(path: Path, header: List<String>): List<Row> {
        val lines = Files.readAllLines(path).filter(String::isNotBlank)
        require(lines.isNotEmpty() && lines.first().split(',') == header) {
            "${path.fileName} has an unexpected header"
        }
        return lines.drop(1).mapIndexed { index, line ->
            val values = line.split(',')
            require(values.size == header.size) { "${path.fileName}:${index + 2} has wrong column count" }
            Row(header.zip(values).toMap(), path.fileName.toString(), index + 2)
        }
    }

    private data class Row(val values: Map<String, String>, val file: String, val line: Int) {
        fun value(key: String): String = values[key]?.takeIf(String::isNotBlank)
            ?: error("$file:$line missing $key")

        fun long(key: String): Long = value(key).toLongOrNull()
            ?: error("$file:$line invalid integer in $key")

        fun decimal(key: String): BigDecimal = value(key).toBigDecimalOrNull()
            ?: error("$file:$line invalid decimal in $key")
    }

    private fun fmt(value: BigDecimal, scale: Int? = null): String {
        val rounded = scale?.let { value.setScale(it, RoundingMode.HALF_UP) } ?: value.stripTrailingZeros()
        return rounded.toPlainString()
    }
}
