// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CostLatencyCurveGeneratorTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void generatedCurveLabelsMeasuredAndModelledValuesAndCheckRejectsStaleOutput()
            throws Exception {
        Path results = temporaryDirectory.resolve("docs/internal/product/measurements/results");
        Files.createDirectories(results);
        Files.writeString(results.resolve("size-triggered.csv"), """
                rate_mib_s,puts,requests_per_mib,duration_seconds,interval_floor_ms,interval_ceiling_ms,status
                40,35,0.2714,5,1000,5000,smoke
                80,35,0.2744,5,1000,5000,smoke
                160,35,0.2722,5,1000,5000,smoke
                """);
        Files.writeString(results.resolve("low-rate.csv"), """
                ceiling_ms,total_puts,data_puts,commit_puts,checkpoint_puts,lease_puts,other_puts,elapsed_seconds,status
                250,24,9,10,1,2,2,3.07,smoke
                5000,4,1,1,0,1,1,5.05,smoke
                """);
        Files.writeString(results.resolve("visibility.csv"), """
                ceiling_ms,records,p50_ms,p99_ms,max_ms,bound_3x_ms,status
                250,10000,0.942,11.395,11.428,750,measured
                1000,10000,0.864,13.681,13.705,3000,measured
                5000,10000,0.892,3.396,3.426,15000,measured
                """);
        StringBuilder codecs = new StringBuilder(
                "block_bytes,codec,mib_per_s_core,ratio,bytes_per_op\n");
        for (int block : new int[] {65_536, 262_144, 1_048_576}) {
            for (String codec : new String[] {"zstd-1", "zstd-3", "zstd-6", "lz4", "none"}) {
                codecs.append(block).append(',').append(codec).append(",1,1,1\n");
            }
        }
        Files.writeString(results.resolve("codec.csv"), codecs);
        StringBuilder direct = new StringBuilder(
                "fanout,proxy_gets,direct_gets,proxy_ingester_bytes,direct_ingester_bytes,proxy_cpu_ns,direct_cpu_ns,default_mode\n");
        for (int fanout = 1; fanout <= 16; fanout++) {
            direct.append(fanout).append(",1,").append(fanout).append(',')
                    .append(65_536L * fanout).append(",0,0,1,")
                    .append(fanout == 1 ? "direct" : "proxy").append('\n');
        }
        Files.writeString(results.resolve("direct-threshold.csv"), direct);
        Files.writeString(results.resolve("rig.properties"), """
                os=Windows 11 Pro build 26200
                jdk=Temurin 25.0.4.1+1-LTS
                cpu=Intel Core i5-13600KF; 14 physical / 20 logical cores
                container_engine=Linux Docker Desktop; 20 vCPU; 31.35 GiB engine memory
                rustfs_limits=256 MiB memory; no CPU quota or cpuset restriction
                rustfs=1.0.0; sha256:8cc9801755448b71a786705ce76692c77e14936cccd87fc2f31842e58f4d1ff
                cpu_governor=Not exposed by WSL2
                aws_put_usd_per_1000=0.005
                aws_get_usd_per_1000=0.0004
                """);

        String generated = CostLatencyCurveGenerator.render(results);
        String chart = CostLatencyCurveGenerator.renderSvg(results);

        assertThat(generated)
                .contains("| 40 | 1000 ms / 5000 ms | 35 | 0.2714 | $1.4229 |")
                .contains("| 0.25 s | 24 | 19 | 1 | 2 | 2 | 3.07 s | 13 | 1.4615 |")
                .contains("| 5 s | 4 | 2 | 0 | 1 | 1 | 5.05 s | 2 | 1 |")
                .contains("| 11.395 ms |")
                .contains("low-rate budget is measured segment-data plus commit-delta PUTs per interval")
                .contains("NOT-RUN")
                .contains("zstd-3")
                .contains("Default direct threshold: **1**")
                .contains("Temurin 25.0.4.1+1-LTS");
        assertThat(chart)
                .contains("<svg")
                .contains("Visibility p99 vs configured interval ceiling")
                .contains("Low-rate data+commit PUTs per interval")
                .contains("<path class=\"low\" d=\"M120,389.46 720,407\"/>")
                .contains("data+commit")
                .contains("USD/TiB modelled");

        Path generatedDocument = temporaryDirectory.resolve("cost-latency-curve.md");
        Path generatedSvg = temporaryDirectory.resolve("cost-latency-curve.svg");
        Files.writeString(generatedDocument, generated);
        Files.writeString(generatedSvg, chart);
        CostLatencyCurveGenerator.check(results, generatedDocument, generatedSvg);

        Files.writeString(generatedDocument, generated.replace("\n", "\r\n"));
        Files.writeString(generatedSvg, chart.replace("\n", "\r\n"));
        CostLatencyCurveGenerator.check(results, generatedDocument, generatedSvg);

        Files.writeString(generatedSvg, chart.replace("measured counts, modelled dollars",
                "mutated chart"));
        assertThatThrownBy(
                () -> CostLatencyCurveGenerator.check(results, generatedDocument, generatedSvg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale");
        Files.writeString(generatedSvg, chart);

        Path sizeResults = results.resolve("size-triggered.csv");
        String validSizeResults = Files.readString(sizeResults);
        Files.writeString(sizeResults, validSizeResults.replace("0.2714", "0.9000"));
        assertThatThrownBy(
                () -> CostLatencyCurveGenerator.check(results, generatedDocument, generatedSvg))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale");
        Files.writeString(sizeResults, validSizeResults);

        Files.writeString(sizeResults, validSizeResults.replace("40,35,", "40,-1,"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class);
        Files.writeString(sizeResults, validSizeResults);
        Files.writeString(results.resolve("direct-threshold.csv"), direct.toString());

        Files.writeString(results.resolve("direct-threshold.csv"),
                Files.readString(results.resolve("direct-threshold.csv"))
                        .replace("1,1,1,65536,0,0,1,direct", "1,1,1,65536,1,0,1,direct"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class);
        Files.writeString(results.resolve("direct-threshold.csv"), direct.toString());

        Files.writeString(results.resolve("direct-threshold.csv"),
                direct + "2,1,2,131072,0,0,1,proxy\n");
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class);
        Files.writeString(results.resolve("direct-threshold.csv"), direct.toString());

        Path rig = results.resolve("rig.properties");
        String validRig = Files.readString(rig);
        Files.writeString(rig, validRig.replace("aws_put_usd_per_1000=0.005",
                "aws_put_usd_per_1000=0"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class);
        Files.writeString(rig, validRig.replace("aws_put_usd_per_1000=0.005",
                "aws_put_usd_per_1000=-0.005"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class);
        // ⚠️ RESTORED HERE (M10.10), so every later case starts from a valid
        // rig. Left negative, the price check -- run last -- refused every
        // later fixture anyway, so a case below that asserts no message could
        // not notice its own check being deleted (measured: removing the
        // `proxy_ingester_bytes` clause stayed green), and the free-GET case
        // would be refused for the PUT price under the same message.
        Files.writeString(rig, validRig);

        Path lowRateResults = results.resolve("low-rate.csv");
        String validLowRateResults = Files.readString(lowRateResults);
        Files.writeString(lowRateResults, validLowRateResults.replace("250,24,", "250,-1,"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class);
        Files.writeString(lowRateResults, validLowRateResults);

        Files.writeString(lowRateResults,
                validLowRateResults.replace("250,24,9,10,1,2,2,", "250,24,9,10,0,2,2,"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("categories must reconcile");
        Files.writeString(lowRateResults, validLowRateResults);

        Files.writeString(lowRateResults,
                validLowRateResults.replace("250,24,9,10,1,2,2,",
                        "250,37,15,14,1,2,5,"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data+commit stay within the per-interval budget");
        Files.writeString(lowRateResults, validLowRateResults);

        Files.writeString(results.resolve("direct-threshold.csv"),
                Files.readString(results.resolve("direct-threshold.csv"))
                        .replace("1,1,1,65536,0,0,1,direct", "1,1,1,0,0,0,1,direct"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class);
        Files.writeString(results.resolve("direct-threshold.csv"), direct.toString());

        // ⚠️ M10.10: A DUPLICATED POINT IS REFUSED. The rate set compared as a
        // set, so a second 40 MiB/s row passed and the curve plotted two
        // values for one point.
        Files.writeString(sizeResults, validSizeResults + "40,36,0.2800,5,1000,5000,smoke\n");
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly once");
        Files.writeString(sizeResults, validSizeResults);

        // ⚠️ M10.10: A FREE GET IS REFUSED ON ITS OWN, not only beside a free
        // PUT -- every read-side dollar in the report multiplies by it.
        Files.writeString(rig, validRig.replace("aws_get_usd_per_1000=0.0004",
                "aws_get_usd_per_1000=0"));
        assertThatThrownBy(() -> CostLatencyCurveGenerator.render(results))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prices must be positive");
        Files.writeString(rig, validRig);
        CostLatencyCurveGenerator.render(results);
    }
}
