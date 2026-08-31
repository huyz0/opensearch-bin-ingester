// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import binjava.binstore.BinStore;
import binjava.binstore.BinStoreConformance;
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
