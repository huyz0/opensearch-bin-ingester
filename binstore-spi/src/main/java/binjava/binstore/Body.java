// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * What a write sends, as a stream rather than an array.
 *
 * <p>⚠️ Never a {@code byte[]}. Constraint C8 — nothing materialises a whole
 * object in heap — is enforced at the SPI BOUNDARY rather than by convention:
 * if the interface cannot express a full-buffer write, nobody can accidentally
 * perform one. The supplier may be called more than once, because a retry has
 * to re-read the body from the start.
 *
 * @param length bytes the stream will yield; stores need it up front for
 *     content-length, and a mismatch is the caller's bug
 * @param open opens a fresh stream positioned at zero, once per attempt
 */
public record Body(long length, Supplier<InputStream> open) {

    public Body {
        Objects.requireNonNull(open, "open");
        if (length < 0) {
            throw new IllegalArgumentException("length is never negative: " + length);
        }
    }

    /**
     * A stream that REFUSES a body whose length disagrees with what it declared.
     * This is the sanctioned path for a streaming backend.
     *
     * <p>⚠️ {@link #readFully} bought length enforcement by making
     * materialisation the only way to check — which re-opened the exact hole
     * this class exists to close, since C8 budgets in-flight PUT buffers at
     * 50 × 8 MiB inside a pool and a full read allocates outside it. Counting
     * while streaming enforces the same rule at the same boundary without ever
     * holding the object.
     *
     * <p>⚠️ The length is checked in THREE places, because no one of them covers
     * every consumer:
     * <ul>
     *   <li><b>during an ordinary read</b>, as soon as a consumer that over-reads
     *       passes the declared length — so a streaming backend is stopped before
     *       it commits the request, not told afterwards;
     *   <li><b>at end of input</b>, which catches a short body;
     *   <li><b>on close</b>, which catches a consumer that stopped early — and
     *       which, when the consumer stopped EXACTLY at the declared length
     *       without seeing EOF, PROBES one more byte from the delegate. That
     *       consumer never over-reads, so nothing else can tell an honest body
     *       from a truncated one.
     * </ul>
     *
     * <p>⚠️ A refusal from {@code close()} may arrive AFTER the request body has
     * been sent. On the content-length path the object may already be committed,
     * so a caller must treat a failure from {@code close()} as a possibly-written
     * object and reconcile, not as a write that certainly did not happen.
     *
     * <p>⚠️ CONTRACT ON THE SUPPLIED STREAM: it must reach end of input promptly
     * once {@code length} bytes have been read, and must not block indefinitely
     * on a further read. The close-time probe issues one read, so a delegate that
     * parks after {@code length} bytes — a {@code PipedInputStream} whose writer
     * waits for the response, say — would deadlock in {@code close()}. The probe
     * also consumes one byte past {@code length} from the delegate, so the
     * delegate must not be reused afterwards; re-open the {@link Body} instead.
     */
    public InputStream checkedStream() {
        InputStream in = open.get();
        return new java.io.FilterInputStream(in) {
            private long seen;
            private boolean verified;
            private boolean sawEof;

            /** ⚠️ Over-long is checked EAGERLY: one byte past the declared
             * length is already a lie, and waiting for EOF lets a
             * content-length-driven backend commit the truncated object first. */
            private void count(long n) throws java.io.IOException {
                seen += n;
                if (seen > length) {
                    verified = true;
                    throw new java.io.IOException(
                            "body declared " + length + " bytes but yielded more");
                }
            }

            private void verify() throws java.io.IOException {
                if (verified) {
                    return;
                }
                verified = true;
                if (seen < length) {
                    throw new java.io.IOException(
                            "body declared " + length + " bytes but yielded " + seen);
                }
                // ⚠️ PROBE ONE MORE BYTE. count()'s eager check only fires when
                // the consumer VOLUNTARILY reads past the declared length. A
                // content-length-driven backend -- the shape this guard exists
                // for -- reads exactly `length` bytes and stops, so it never
                // over-reads, `seen == length`, and an over-long body was
                // ACCEPTED. S3 then stores a truncated object that reads back as
                // a valid short segment. Only asking for one more byte can tell
                // an honest body from a truncated one at that point.
                // ⚠️ EQUIVALENT MUTANT, disclosed: dropping `!sawEof` survives,
                // because a stream at end of input returns -1 to the probe
                // anyway. It is kept so the delegate is not TOUCHED at all after
                // EOF -- some throw on read-after-EOF and some block -- which is
                // a correctness guard against those delegates even though no
                // in-tree fake can express it.
                if (!sawEof && in.read() >= 0) {
                    throw new java.io.IOException(
                            "body declared " + length + " bytes but yielded more");
                }
            }

            @Override
            public int read() throws java.io.IOException {
                int b = super.read();
                if (b < 0) {
                    sawEof = true;
                    verify();
                } else {
                    count(1);
                }
                return b;
            }

            @Override
            public int read(byte[] buf, int off, int len) throws java.io.IOException {
                int n = super.read(buf, off, len);
                if (n < 0) {
                    sawEof = true;
                    verify();
                } else {
                    count(n);
                }
                return n;
            }

            /** ⚠️ Counted, or an honest body that is skipped over is refused. */
            @Override
            public long skip(long n) throws java.io.IOException {
                long skipped = super.skip(n);
                count(skipped);
                return skipped;
            }

            /**
             * ⚠️ EQUIVALENT MUTANT, disclosed: {@code super.mark(readlimit)}
             * survives, because reset() refuses unconditionally so nothing
             * observable separates a no-op from a forwarding override. Kept
             * because forwarding leaves the delegate marked for no reason.
             *
             * <p>A no-op, because FilterInputStream forwards mark() to the
             * delegate — a client could mark successfully and only discover the
             * refusal at reset(), which is worse than being told up front.
             */
            @Override
            public synchronized void mark(int readlimit) {
                // deliberately nothing; markSupported() is false
            }

            /**
             * ⚠️ REFUSED. An HTTP client that marks, streams and then resets to
             * retry a 500 would otherwise see seen == 2 x length at close and
             * fail a RETRYABLE upload permanently. Saying "no" makes the client
             * re-open the Body instead, which is the documented contract.
             */
            @Override
            public boolean markSupported() {
                return false;
            }

            @Override
            public synchronized void reset() throws java.io.IOException {
                throw new java.io.IOException("checkedStream is not resettable; re-open the Body");
            }

            @Override
            public void close() throws java.io.IOException {
                // ⚠️ The delegate is closed even when verify() throws, or a short
                // body leaks the descriptor it was opened with.
                try {
                    verify();
                } finally {
                    in.close();
                }
            }
        };
    }

    /**
     * Reads the body and REFUSES a length that does not match.
     *
     * <p>⚠️ ONLY for a backend that stores bytes in heap by its nature — which
     * today means the in-memory one. A streaming backend uses
     * {@link #checkedStream()}; using this instead materialises the whole object
     * outside the buffer pool and breaks constraint C8.
     *
     * <p>⚠️ {@code length} was decorative: nothing read it, no test constrained
     * it, and deleting the component left every test green. An unenforced length
     * is worse than none, because S3 is given it as content-length and turns a
     * mismatch into a TRUNCATED OBJECT that reads back as a valid short segment.
     */
    public byte[] readFully() throws java.io.IOException {
        byte[] bytes;
        try (InputStream in = open.get()) {
            bytes = in.readAllBytes();
        }
        if (bytes.length != length) {
            throw new java.io.IOException(
                    "body declared " + length + " bytes but yielded " + bytes.length);
        }
        return bytes;
    }

    /**
     * ⚠️ TEST AND SMALL-CONTROL-RECORD USE ONLY. A commit delta is tens of
     * bytes; a segment is megabytes and must never come through here.
     */
    public static Body ofBytes(byte[] bytes) {
        byte[] copy = bytes.clone();
        return new Body(copy.length, () -> new ByteArrayInputStream(copy));
    }
}
