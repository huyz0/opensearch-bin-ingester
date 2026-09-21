// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.BinStoreConformance;

/** The in-memory backend against the shared contract. */
class MemoryBinStoreTest extends BinStoreConformance {
    @Override
    protected BinStore newStore() {
        return new MemoryBinStore();
    }
}
