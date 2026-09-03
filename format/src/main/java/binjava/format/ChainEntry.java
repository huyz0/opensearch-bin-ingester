// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * One object in a commit-log chain: a {@link CommitDelta}, a {@link Seal} that
 * ends an epoch's chain, or a {@link Continue} that opens the next one.
 *
 * <p>⚠️ A CLOSED SET, so a sealed interface rather than one record with a kind
 * field and mostly-absent columns (java-style.md rule 11). A reader that
 * switches over these three is exhaustive by compilation; a reader that
 * switches over a kind byte is exhaustive by hope.
 *
 * <p>⚠️ THE VERSION DISTINGUISHES LAYOUTS, NOT RELEASES. Version 0 <em>is</em>
 * "a delta": it carries no kind field, so a v0 object can only ever be one, and
 * every delta already in a bucket stays readable for the whole retention window
 * without being rewritten. Version 1 <em>is</em> "a kinded entry". A delta
 * therefore still writes v0 — nothing about a delta changed, and re-encoding
 * one would churn bytes and golden files to say the same thing. See ADR-0028.
 *
 * <p>⚠️ AN UNKNOWN KIND OR VERSION STOPS, it does not skip. The
 * {@code wire-format-change} skill requires this answer be stated rather than
 * left to whichever branch happens to run, and here the safe direction is
 * unambiguous: silently skipping an entry a reader does not understand is how a
 * chain loses a {@code SEAL} and a reader goes on applying a discarded suffix,
 * which is exactly what invariant I3 forbids.
 */
public sealed interface ChainEntry permits CommitDelta, Seal, Continue {

    /** ⚠️ 'BDLT'. Unchanged since v0 — the chain is still the chain. */
    int MAGIC = 0x42444C54;

    /** A bare delta, with no kind field. Still written, still read. */
    int VERSION_DELTA = 0;

    /** A kinded entry: the header is followed by a {@code KIND_*} uvarint. */
    int VERSION_KINDED = 1;

    // ⚠️ Kind 0 is deliberately UNASSIGNED. A v1-kinded delta would be a second
    // encoding of what v0 already says -- decode would accept it while `encode`
    // returned v0, so a round trip would not be byte-stable -- and any writer
    // emitting it would produce bytes every earlier reader rejects, for no
    // gain. It is left free so a FUTURE delta layout can claim it, at which
    // point v0 and it are genuinely different shapes.
    int KIND_SEAL = 1;

    int KIND_CONTINUE = 2;

    /** Where in its chain this entry sits. */
    long sequence();

    byte[] encode();

    /** ⚠️ The 8-byte header every shape shares. */
    static byte[] header(int version) {
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(MAGIC);
        head.putInt(version);
        return head.array();
    }

    static ByteArrayOutputStream kinded(int kind) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(header(VERSION_KINDED));
        SegmentWriter.putUvarint(out, kind);
        return out;
    }

    /**
     * Reads any chain entry.
     *
     * @throws IOException the bytes are not a chain entry, or are one this
     *     build does not understand — ⚠️ never silently skipped, see above
     */
    static ChainEntry decode(byte[] bytes) throws IOException {
        if (bytes.length < 8) {
            throw new IOException("chain entry is shorter than its own header");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (b.getInt(0) != MAGIC) {
            throw new IOException("not a chain entry: bad magic");
        }
        int version = b.getInt(4);
        Cursor c = new Cursor(bytes, 8);
        ChainEntry entry = switch (version) {
            case VERSION_DELTA -> CommitDelta.decodeBody(c);
            case VERSION_KINDED -> decodeKinded(c);
            default -> throw new IOException("unsupported chain entry version: " + version);
        };
        if (!c.atEnd()) {
            throw new IOException("chain entry has bytes after its last field");
        }
        return entry;
    }

    private static ChainEntry decodeKinded(Cursor c) throws IOException {
        long kind = c.uvarint();
        // ⚠️ A value the WIRE can carry but a record refuses is corrupt input,
        // not a programming error, so it leaves as an IOException like every
        // other malformed-object failure. Unwrapped, a torn or zero-filled
        // object would throw an unchecked exception straight past `recover`'s
        // own `throws IOException` and every caller catching it.
        try {
            if (kind == KIND_SEAL) {
                return new Seal(c.uvarint(), c.uvarint());
            }
            if (kind == KIND_CONTINUE) {
                return new Continue(c.uvarint(), c.uvarint(), c.uvarint());
            }
        } catch (IllegalArgumentException e) {
            throw new IOException("corrupt chain entry of kind " + kind + ": " + e.getMessage(), e);
        }
        throw new IOException("unsupported chain entry kind: " + kind);
    }
}
