// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.GoverningBinStore;
import java.time.Clock;

/**
 * The governing decorator runs the WHOLE store contract (M10.10, ADR-0075), over
 * the capable stand-in so {@code presign}'s forwarding is exercised -- the
 * reason {@code CountingBinStoreTest} does the same.
 *
 * <p>⚠️ THE BUCKET IS SIZED OUT OF THE WAY: this asserts the decorator is
 * transparent below its ceiling, and {@code GoverningBinStoreTest} asserts
 * the ceiling.
 */
class GoverningBinStoreConformanceTest extends io.github.huyz0.os.biningester.binstore.BinStoreConformance {

    @Override
    protected BinStore newStore() {
        return new GoverningBinStore(new CountingBinStoreTest.CapableStore(), new CostGovernor(
                new CostGovernor.Settings(1_000.0, 1_000_000, java.time.Duration.ofMinutes(1),
                        8 << 20), Clock.systemUTC(), () -> 250));
    }
}
