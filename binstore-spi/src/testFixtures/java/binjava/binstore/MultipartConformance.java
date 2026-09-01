// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * The MULTIPART half of the store contract, run against every backend
 * (M2.2; research doc 01 §7). The root of the conformance chain --
 * {@link ConditionalWriteConformance} extends this, and {@link
 * BinStoreConformance} extends that -- so every backend test class still
 * needs only the one {@code extends} it already has. Split out for the same
 * code-structure.md rule 1 reason as the other two: one contract family, one
 * file, so no single file grows past the size that forced the original
 * split.
 */
public abstract class MultipartConformance {

    /** A fresh, empty store. Closed by the test. */
    protected abstract BinStore newStore() throws Exception;

    protected static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    protected static String read(java.io.InputStream in) throws Exception {
        try (in) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /** A body at least {@code minPartSize} long, filled with {@code fill}. */
    private static Body partSized(char fill, long minPartSize) {
        byte[] b = new byte[(int) minPartSize];
        java.util.Arrays.fill(b, (byte) fill);
        return Body.ofBytes(b);
    }

    @Test
    void multipartAssemblesPartsInPartNumberOrderByteIdentical() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            try (MultipartWriter w = s.multipart("k")) {
                // ⚠️ UPLOADED OUT OF ORDER. S3, GCS and Azure all allow this --
                // complete() must assemble by PART NUMBER, never arrival order.
                w.uploadPart(2, partSized('b', min));
                w.uploadPart(1, partSized('a', min));
                w.uploadPart(3, Body.ofBytes(bytes("tail"))); // final part, may be short
                Version v = w.complete();
                assertThat(v).isNotNull();
            }
            String expected = "a".repeat((int) min) + "b".repeat((int) min) + "tail";
            assertThat(read(s.get("k"))).isEqualTo(expected);
            assertThat(s.stat("k").orElseThrow().size()).isEqualTo(expected.length());
        }
    }

    @Test
    void aNonFinalPartBelowMinPartSizeIsRefused() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            try (MultipartWriter w = s.multipart("k")) {
                // ⚠️ part 1 is BELOW min and is NOT the highest-numbered part --
                // only complete() can know this, since part 2 might never arrive.
                w.uploadPart(1, Body.ofBytes(bytes("short")));
                w.uploadPart(2, partSized('b', min));
                assertThatThrownBy(w::complete).isInstanceOf(IOException.class);
            }
            // ⚠️ A refused complete() must not publish anything under the key.
            assertThat(s.stat("k")).as("a refused assembly creates nothing").isEmpty();
        }
    }

    @Test
    void exemptionFromMinPartSizeFollowsThePartNUMBERNotUploadOrder() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            try (MultipartWriter w = s.multipart("k")) {
                // ⚠️ Distinct from aNonFinalPartBelowMinPartSizeIsRefused: THERE
                // the undersized part also happened to be uploaded first, so a
                // backend that (wrongly) exempts whichever part arrived LAST in
                // wall-clock order, rather than the highest part NUMBER, would
                // still pass it. Here the highest-numbered part (2) is uploaded
                // FIRST and the undersized non-final part (1) SECOND -- the only
                // shape that tells "by number" and "by arrival" apart.
                w.uploadPart(2, partSized('b', min));
                w.uploadPart(1, Body.ofBytes(bytes("short")));
                assertThatThrownBy(w::complete).isInstanceOf(IOException.class);
            }
            assertThat(s.stat("k")).as("a refused assembly creates nothing").isEmpty();
        }
    }

    @Test
    void theHighestNumberedPartMayBeSmallerThanMinPartSize() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            try (MultipartWriter w = s.multipart("k")) {
                w.uploadPart(1, partSized('a', min));
                w.uploadPart(2, Body.ofBytes(bytes("x"))); // final part, well under min
                w.complete();
            }
            assertThat(read(s.get("k"))).isEqualTo("a".repeat((int) min) + "x");
        }
    }

    @Test
    void completingWithNoPartsIsRefused() throws Exception {
        try (BinStore s = newStore()) {
            try (MultipartWriter w = s.multipart("k")) {
                assertThatThrownBy(w::complete).isInstanceOf(IOException.class);
            }
            assertThat(s.stat("k")).isEmpty();
        }
    }

    @Test
    void abortDiscardsEverythingAndTheKeyIsNeverOccupied() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            try (MultipartWriter w = s.multipart("k")) {
                w.uploadPart(1, partSized('a', min));
                w.abort();
            }
            assertThat(s.stat("k")).as("an aborted upload creates nothing").isEmpty();
        }
    }

    @Test
    void abortLeavesAPreviouslyExistingObjectAtThatKeyUntouched() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            s.put("k", Body.ofBytes(bytes("original")));
            try (MultipartWriter w = s.multipart("k")) {
                w.uploadPart(1, partSized('z', min));
                w.abort();
            }
            // ⚠️ The whole point of NOT occupying the key until complete(): an
            // in-progress (or abandoned) multipart upload must never clobber
            // what was already there.
            assertThat(read(s.get("k"))).isEqualTo("original");
        }
    }

    @Test
    void closingWithoutCompletingBehavesLikeAnAbort() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            MultipartWriter w = s.multipart("k");
            w.uploadPart(1, partSized('a', min));
            w.close(); // no complete(), no explicit abort()
            // ⚠️ MultipartWriter's own contract: close() alone is NOT an
            // implicit complete. A caller that closes on an exception path
            // (try-with-resources unwinding) must never silently publish a
            // partial object.
            assertThat(s.stat("k")).as("close() without complete() publishes nothing").isEmpty();
        }
    }

    @Test
    void closingWithoutCompletingLeavesAPreviouslyExistingObjectAtThatKeyUntouched() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            // ⚠️ Distinct from BOTH closingWithoutCompletingBehavesLikeAnAbort
            // (checks close() only against an EMPTY key) and
            // abortLeavesAPreviouslyExistingObjectAtThatKeyUntouched (checks a
            // pre-existing object only against EXPLICIT abort()) -- the
            // acceptance criterion's own "aborted/never-completed" wording
            // covers this combination too, and a close()-as-implicit-abort path
            // that corrupts the existing object on this specific route would
            // pass both of those tests alone.
            s.put("k", Body.ofBytes(bytes("original")));
            MultipartWriter w = s.multipart("k");
            w.uploadPart(1, partSized('z', min));
            w.close(); // no complete(), no explicit abort()
            assertThat(read(s.get("k"))).isEqualTo("original");
        }
    }

    @Test
    void reUploadingAPartNumberReplacesWhatWasStagedForIt() throws Exception {
        try (BinStore s = newStore()) {
            long min = s.capabilities().minPartSize();
            try (MultipartWriter w = s.multipart("k")) {
                w.uploadPart(1, partSized('a', min));
                w.uploadPart(1, partSized('a', min)); // re-upload part 1
                w.uploadPart(2, Body.ofBytes(bytes("tail")));
                w.complete();
            }
            // ⚠️ Exactly ONE copy of part 1 -- a naive concatenation of every
            // uploadPart call, rather than the latest one per part number,
            // would double the first part's bytes.
            assertThat(read(s.get("k"))).isEqualTo("a".repeat((int) min) + "tail");
        }
    }
}
