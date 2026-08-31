// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import binjava.binstore.BinStore;
import binjava.binstore.BinStoreConformance;

/** The in-memory backend against the shared contract. */
class MemoryBinStoreTest extends BinStoreConformance {
    @Override
    protected BinStore newStore() {
        return new MemoryBinStore();
    }
}
