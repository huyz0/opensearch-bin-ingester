// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

class MacroRunResultsTest {

    private static final Pattern ENCODED = Pattern.compile("\\\"encoded\\\":\\\"([^\\\"]+)\\\"");

    @Test
    void resultFileCarriesStoreCountsAndAnHdrHistogram() throws Exception {
        Histogram latency = new Histogram(60_000_000_000L, 3);
        latency.recordValue(1_000_000L);

        String podId = "pod\"a" + (char) 0;
        String json = new MacroRunResults(
                "run\n1", Map.of(podId, new ProcessStoreCounts(3, 2, 1, 4, 0)), latency)
                .toJson();

        assertThat(json).contains("\"storeCounts\"")
                .contains("\"runId\":\"run\\n1\"")
                .contains("\"podId\":\"pod\\\"a\\u0000\"")
                .contains("\"puts\":3")
                .contains("\"gets\":2")
                .contains("\"lists\":1")
                .contains("\"stats\":4")
                .contains("\"deletes\":0")
                .contains("\"latencyHistogram\"")
                .contains("\"type\":\"HdrHistogram\"")
                .contains("\"encoding\":\"base64-compressed\"")
                .contains("\"encoded\":\"")
                .doesNotContain("\"encoded\":\"\"")
                .contains("\"totalCount\":1")
                .contains("\"minNanos\":" + latency.getMinValue())
                .contains("\"maxNanos\":" + latency.getMaxValue())
                .contains("\"p50Nanos\":" + latency.getValueAtPercentile(50.0));
        Matcher encoded = ENCODED.matcher(json);
        assertThat(encoded.find()).isTrue();
        Histogram decoded = Histogram.decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(Base64.getDecoder().decode(encoded.group(1))), 0);
        assertThat(decoded.getTotalCount()).isEqualTo(latency.getTotalCount());
        assertThat(decoded.getMinValue()).isEqualTo(latency.getMinValue());
        assertThat(decoded.getMaxValue()).isEqualTo(latency.getMaxValue());
        assertThat(decoded.getValueAtPercentile(95.0))
                .isEqualTo(latency.getValueAtPercentile(95.0));
        assertThat(decoded.getValueAtPercentile(99.0))
                .isEqualTo(latency.getValueAtPercentile(99.0));
    }
}
