// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The default-path commit answer when a request asked for it (ADR-0082 §7;
 * ADR-0081 §8; M13.34a): the deltas that committed a request's runs -- more
 * than one when a paused run followed in a later delta of the same segment --
 * and the runs redirected to the fast path because the catalog has their
 * stream in fast mode.
 *
 * <p>⚠️ A WRITER THAT DOES NOT ASK GETS THE BARE {@link CommitDelta} (the
 * request's Accept, M13.34e, is the HTTP layer's): an older binary, whose
 * indices are {@code wal=false}, for which one delta is always the whole
 * answer. This type is never sent to one.
 *
 * <p>⚠️ LAYOUT (big-endian), landing before anything writes it so a reader
 * ships first: {@code u32} magic {@code 0x4243414E} ("BCAN"), {@code u8}
 * version 1, {@code u16} delta count, each delta its {@link CommitDelta}
 * encoding behind a {@code u32} length, {@code u32} redirected count, each a
 * run key (16 bytes of index UUID, {@code i32} partition).
 *
 * @param deltas oldest first, sequences strictly ascending
 * @param redirected the runs the writer re-sends by COMMIT (§2), each once
 */
public record CommitAnswer(List<CommitDelta> deltas, List<RunKey> redirected) {

    /** {@code "BCAN"}, big-endian, so an operator sees it in a hex dump. */
    public static final int MAGIC = 0x4243414E;

    /** The only shape that has ever been written. */
    public static final int VERSION_1 = 1;

    /** What a {@code u16} count holds. */
    public static final int MAX_DELTAS = 0xFFFF;

    private static final int RUN_KEY_BYTES = 20;

    public CommitAnswer {
        Objects.requireNonNull(deltas, "deltas");
        Objects.requireNonNull(redirected, "redirected");
        deltas = List.copyOf(deltas);
        redirected = List.copyOf(redirected);
        if (deltas.isEmpty() && redirected.isEmpty()) {
            // ⚠️ AN ANSWER OF NOTHING ANSWERS NOTHING: a writer would wait on
            // it for ever, with every run neither committed nor redirected.
            throw new IllegalArgumentException("an answer carries a delta or a redirected run");
        }
        if (deltas.size() > MAX_DELTAS) {
            throw new IllegalArgumentException("an answer carries at most " + MAX_DELTAS
                    + " deltas: " + deltas.size());
        }
        for (int i = 1; i < deltas.size(); i++) {
            if (deltas.get(i).sequence() <= deltas.get(i - 1).sequence()) {
                throw new IllegalArgumentException("deltas ascend: "
                        + deltas.get(i - 1).sequence() + " then " + deltas.get(i).sequence());
            }
        }
        Set<RunKey> seen = new HashSet<>();
        for (RunKey run : redirected) {
            if (!seen.add(run)) {
                throw new IllegalArgumentException("a run is redirected once: " + run);
            }
        }
        // ⚠️ COMMITTED OR REDIRECTED, NEVER BOTH (review round 1, P3): a
        // writer re-sending a committed run by COMMIT would write it twice.
        for (CommitDelta delta : deltas) {
            for (RunCommit run : delta.runs()) {
                if (seen.contains(run.key())) {
                    throw new IllegalArgumentException("a run both committed and redirected: "
                            + run.key());
                }
            }
        }
    }

    /** The whole answer, the way ADR-0082 §7 lays it out. */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(4).putInt(MAGIC).array());
        out.write(VERSION_1);
        out.writeBytes(ByteBuffer.allocate(2).putShort((short) deltas.size()).array());
        for (CommitDelta delta : deltas) {
            byte[] bytes = delta.encode();
            out.writeBytes(ByteBuffer.allocate(4).putInt(bytes.length).array());
            out.writeBytes(bytes);
        }
        out.writeBytes(ByteBuffer.allocate(4).putInt(redirected.size()).array());
        for (RunKey run : redirected) {
            FastFrame.runKey(out, run);
        }
        return out.toByteArray();
    }

    /**
     * @throws IOException not an answer: a wrong magic or version, a torn
     *     count or delta, a delta that is not a delta, bytes left over, or an
     *     answer {@link #CommitAnswer} would refuse
     */
    public static CommitAnswer decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        // ⚠️ THE WHOLE PARSE (review round 1, P1): a nested delta's record
        // refuses what it cannot be as an IllegalArgumentException, and these
        // are a peer's bytes -- a refusal, never an unchecked throw.
        try {
            return parse(bytes);
        } catch (IllegalArgumentException refused) {
            throw new IOException("not a commit answer: " + refused.getMessage(), refused);
        }
    }

    private static CommitAnswer parse(byte[] bytes) throws IOException {
        Cursor c = new Cursor(bytes, 0, "commit answer");
        if (ByteBuffer.wrap(c.bytes(4)).getInt() != MAGIC) {
            throw new IOException("not a commit answer: wrong magic");
        }
        int version = c.bytes(1)[0] & 0xFF;
        if (version != VERSION_1) {
            throw new IOException("a commit answer of version " + version
                    + ", which this build does not read");
        }
        int count = ByteBuffer.wrap(c.bytes(2)).getShort() & 0xFFFF;
        List<CommitDelta> deltas = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            long length = FastFrame.u32(c);
            if (length > c.remaining()) {
                throw new IOException("a commit answer's delta claims " + length
                        + " bytes with only " + c.remaining() + " left");
            }
            deltas.add(CommitDelta.decode(c.bytes((int) length)));
        }
        long runs = FastFrame.count(c, RUN_KEY_BYTES);
        List<RunKey> redirected = new java.util.ArrayList<>();
        for (long i = 0; i < runs; i++) {
            redirected.add(FastFrame.runKey(c));
        }
        if (!c.atEnd()) {
            throw new IOException("a commit answer has " + c.remaining() + " bytes left over");
        }
        return new CommitAnswer(deltas, redirected);
    }
}
