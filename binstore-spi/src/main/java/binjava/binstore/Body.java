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
     * A stream that REFUSES, at end of input, a body whose length disagrees with
     * what it declared. This is the sanctioned path for a streaming backend.
     *
     * <p>⚠️ {@link #readFully} bought length enforcement by making
     * materialisation the only way to check — which re-opened the exact hole
     * this class exists to close, since C8 budgets in-flight PUT buffers at
     * 50 × 8 MiB inside a pool and a full read allocates outside it. Counting
     * while streaming enforces the same rule at the same boundary without ever
     * holding the object.
     *
     * <p>⚠️ The check fires at EOF and again on close, because a backend that
     * stops reading early would otherwise never learn the body was short.
     */
    public InputStream checkedStream() {
        InputStream in = open.get();
        return new java.io.FilterInputStream(in) {
            private long seen;
            private boolean verified;

            private void verify() throws java.io.IOException {
                if (verified) {
                    return;
                }
                verified = true;
                if (seen != length) {
                    throw new java.io.IOException(
                            "body declared " + length + " bytes but yielded " + seen);
                }
            }

            @Override
            public int read() throws java.io.IOException {
                int b = super.read();
                if (b < 0) {
                    verify();
                } else {
                    seen++;
                }
                return b;
            }

            @Override
            public int read(byte[] buf, int off, int len) throws java.io.IOException {
                int n = super.read(buf, off, len);
                if (n < 0) {
                    verify();
                } else {
                    seen += n;
                }
                return n;
            }

            @Override
            public void close() throws java.io.IOException {
                // ⚠️ Close the delegate even when verify() throws, or a short
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
