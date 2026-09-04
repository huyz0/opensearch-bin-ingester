// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * The two fixtures the three {@code FaultInjectingStore} test classes share.
 *
 * <p>⚠️ SHARED RATHER THAN COPIED, and the reason is specific to these classes:
 * they exist to be compared AGAINST EACH OTHER. One asserts what the injector
 * does when faulting, one what it does when clean, one which stream it draws
 * from. If {@code body} drifted in the baseline class, the clean path would write
 * different bytes from the faulting path and every comparison between them would
 * still be green -- so a duplicated fixture here is not merely untidy, it
 * silently decouples the control run from the treated one.
 *
 * <p>⚠️ It was duplicated THREE times before this class existed, byte-identical,
 * which is exactly how such a drift starts.
 */
final class FaultFixtures {

    private FaultFixtures() {
    }

    /** A body carrying {@code s} as UTF-8, re-readable because the supplier is called per read. */
    static Body body(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return new Body(b.length, () -> new ByteArrayInputStream(b));
    }

    /** What is actually stored under {@code key}, read back through the DELEGATE. */
    static String read(MemoryBinStore store, String key) throws IOException {
        try (InputStream in = store.get(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
