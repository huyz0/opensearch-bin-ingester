// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

/**
 * Data-segment GETs are counted apart from other GETs, by the classifier the
 * PUT purposes use (M11.3, ADR-0077 decision 4a): the denominator the
 * read-side apportionment must sum to.
 */
class CountingBinStoreGetPurposeTest {

    private static final String DATA = "bins/c/data/2026/09/27/10/seg.bseg";
    private static final String CONTROL = "bins/c/ctl/log/0.delta";
    private static final String MISSING = "bins/c/data/2026/09/27/10/missing.bseg";

    private static BinStore backend() {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if ((method.getName().equals("get") || method.getName().equals("getRange"))
                            && args[0].equals(MISSING)) {
                        throw new IOException("no such key");
                    }
                    return switch (method.getName()) {
                        case "get", "getRange" -> InputStream.nullInputStream();
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    @Test
    void wholeAndRangedGetsOfADataSegmentAreCountedAndOthersAreNot() throws Exception {
        CountingBinStore counting = new CountingBinStore(backend());

        counting.get(DATA).close();
        counting.getRange(DATA, 0, 1).close();
        counting.get(CONTROL).close();
        counting.getRange(CONTROL, 0, 0).close();

        assertThat(counting.counts().gets()).as("every GET is still a GET").isEqualTo(4);
        assertThat(counting.dataSegmentGets())
                .as("⚠️ ONLY THE DATA SEGMENT's, ranged included").isEqualTo(2);
    }

    @Test
    void aGetThatFailsIsCountedLikeOneThatSucceeds() {
        CountingBinStore counting = new CountingBinStore(backend());

        assertThatThrownBy(() -> counting.get(MISSING)).isInstanceOf(IOException.class);

        assertThat(counting.dataSegmentGets())
                .as("a failed GET is billed, and the apportionment charges it")
                .isEqualTo(1);
    }
}
