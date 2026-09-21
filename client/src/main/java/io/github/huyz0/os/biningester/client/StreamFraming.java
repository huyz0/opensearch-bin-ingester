// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * How one subscription stream is cut into events (M8.21).
 *
 * <p>⚠️ **FOUR BIG-ENDIAN BYTES, THEN THAT MANY BYTES.** HTTP chunking is a
 * transport framing that any proxy may redo, so a reader treating one chunk as
 * one event would split or merge events whenever something in the path
 * buffered differently — silently, and as a delivery gap rather than as a parse
 * error.
 *
 * <p>⚠️ **THERE IS NO HEARTBEAT FRAME, AND THE LONG POLL IS WHY.** An earlier
 * draft of this class said a zero-length frame was one, which was true of the
 * held-open stream it was written for: a consumer holding a response open for
 * hours cannot otherwise tell a quiet stream from a connection that died in the
 * path. A poll answers, with an EMPTY BODY, every {@code wait} at the latest —
 * so liveness is the answer itself and nothing writes a zero-length frame.
 *
 * <p>⚠️ **AN `IOException` RATHER THAN AN `EOFException`** on a truncated
 * frame, which is not a style choice: `check-io-seam.sh`'s allow-list carries
 * the `java.io` types this tree's business logic may name, and `EOFException`
 * is not one. Adding it would be widening a gate for a message, and every
 * caller here catches `IOException` anyway.
 *
 * <p>⚠️ **IN `client` RATHER THAN IN `http`**, because both ends need it and
 * `client` is the module both can depend on — the alternative is two copies of
 * a framing rule, which is the shape ADR-0053 refuses one layer up.
 */
public final class StreamFraming {

    /**
     * ⚠️ 8 MiB. An {@code inline} event carries a whole segment, and the write
     * path's own ceiling is 8 MiB (`IngestConfig.DEFAULT_MAX_SEGMENT_BYTES`),
     * so anything larger is not a frame this deployment produced. Bounded
     * BEFORE the allocation, because a length is four bytes an attacker
     * chooses.
     */
    public static final int MAX_FRAME_BYTES = 8 << 20;

    private StreamFraming() {
    }

    /**
     * Reads one frame, or returns {@code null} at a clean end of stream.
     *
     * @throws IOException on a torn frame, or one larger than
     *     {@link #MAX_FRAME_BYTES}
     */
    public static byte[] readFrame(InputStream in) throws IOException {
        int b0 = in.read();
        if (b0 < 0) {
            return null;
        }
        int length = (b0 << 24) | (readByte(in) << 16) | (readByte(in) << 8) | readByte(in);
        if (length < 0 || length > MAX_FRAME_BYTES) {
            throw new IOException("subscription frame claims " + length
                    + " bytes, which is not a frame this deployment writes");
        }
        // ⚠️ `readNBytes`, NOT A read()-UNTIL-FULL LOOP -- and NOT a
        // byte-at-a-time one, which an earlier draft wrote directly underneath
        // a comment saying it had not: 8.4M single-byte calls for a maximum
        // frame. ⚠️ THE STREAM THIS READS IS ALWAYS IN MEMORY (the poll answer
        // is drained first, by `readBounded`), so `readNBytes` returning short
        // means the frame is genuinely torn rather than merely not arrived yet.
        byte[] frame = in.readNBytes(length);
        int read = frame.length;
        if (read < length) {
            throw new IOException("subscription frame ended after " + read + " of "
                    + length + " bytes");
        }
        return frame;
    }

    /**
     * Drains {@code in} entirely, refusing at {@code max} bytes.
     *
     * <p>⚠️ **THE CAP IS APPLIED WHILE READING, NOT AFTER.** A peer answering
     * with two gigabytes must not be materialised and then measured: the
     * measurement never runs, because the allocation kills the reader first —
     * and an {@code OutOfMemoryError} is an {@code Error}, so it escapes the
     * {@code IOException | RuntimeException} the poll loop catches and the
     * subscription's thread dies silently, inside the OpenSearch node process.
     * ⚠️ This is the same bound the ingester's own {@code BoundedStream} applies
     * to a request body, in the module that may name a server type.
     *
     * @throws IOException as soon as more than {@code max} bytes have arrived
     */
    public static byte[] readBounded(InputStream in, int max) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] chunk = new byte[CHUNK_BYTES];
        int n;
        while ((n = in.read(chunk)) >= 0) {
            if (out.size() + n > max) {
                throw new IOException("a subscription answer exceeded " + max + " bytes, which "
                        + "is not an answer this deployment produces");
            }
            out.write(chunk, 0, n);
        }
        return out.toByteArray();
    }

    /** ⚠️ 8 KiB, the ordinary copy buffer; it bounds the overshoot to nothing. */
    private static final int CHUNK_BYTES = 8192;

    private static int readByte(InputStream in) throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new IOException("subscription stream ended inside a frame length");
        }
        return b;
    }
}
