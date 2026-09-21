// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.BinStoreConformance;
import java.nio.file.Files;
import java.nio.file.Path;

/** The local-filesystem backend against the shared contract. */
class LocalFsBinStoreTest extends BinStoreConformance {
    @Override
    protected BinStore newStore() throws Exception {
        Path dir = Files.createTempDirectory("binstore-localfs");
        dir.toFile().deleteOnExit();
        return new LocalFsBinStore(dir);
    }
}
