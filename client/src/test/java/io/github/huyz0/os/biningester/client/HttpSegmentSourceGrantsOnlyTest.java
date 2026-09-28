// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * A source built without an ingester refuses a proxied fetch by name
 * (M11.14, H9), rather than sending a request to {@code "null/..."} and
 * reporting it as a transport failure a consumer would retry.
 */
class HttpSegmentSourceGrantsOnlyTest {

    private static final String KEY = "bins/cluster-a/data/some.bseg";

    @Test
    void aGrantsOnlySourceRefusesAProxiedFetchAsGrantsOnly() {
        for (HttpSegmentSource source : new HttpSegmentSource[] {
                new HttpSegmentSource(Duration.ofSeconds(1)),
                new HttpSegmentSource(Duration.ofSeconds(1), null, "az-a")}) {
            assertThatThrownBy(() -> source.fetchSegment(KEY))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("fetches only under grants")
                    .hasMessageContaining(KEY);
        }
    }
}
