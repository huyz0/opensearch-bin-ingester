// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * The adaptive tagged membership filter in the object key (ADR-0003; research
 * doc 02 §7): {@code A} (all), {@code N} (none/unknown -- must read the
 * header), {@code Z} (exact bitmap), {@code R} (run-length bitmap), {@code B}
 * (Bloom over index ordinals). One character tag, then a tag-specific
 * payload.
 *
 * <p>⚠️ M2.4 IS THE WIRE FORMAT, not the writer's candidate selection (M2.6)
 * or the Bloom's hash-based construction and query (M2.5, Kirsch-Mitzenmacher).
 * {@code Z} and {@code R} answer {@link #mightContain} directly from their own
 * bitmap -- no hashing involved, since an exact/RLE bitmap's definition IS the
 * membership set. {@link Bloom} exposes its raw bits and {@code k} but not yet
 * a hash-based query; M2.5 adds that on top of this same wire format.
 *
 * <p>⚠️ An unrecognised tag byte throws here. Treating it the same as {@code
 * N} ("must read the header", never "no match") is READER-side policy, wired
 * in M2.7 -- this class's job is only to parse the five tags it knows.
 */
public sealed interface MembershipFilter {

    /** The one-character tag this filter serializes under. */
    char tag();

    /** The full {@code <tag><payload>} string. */
    String encode();

    /** Parses a full {@code <tag><payload>} string into the matching variant. */
    static MembershipFilter decode(String s) throws IOException {
        Objects.requireNonNull(s, "s");
        if (s.isEmpty()) {
            throw new IOException("an empty string is not a membership filter");
        }
        char tag = s.charAt(0);
        String payload = s.substring(1);
        return switch (tag) {
            case 'A' -> All.decode(payload);
            case 'N' -> None.decode(payload);
            case 'Z' -> ExactBitmap.decode(payload);
            case 'R' -> RunLength.decode(payload);
            case 'B' -> Bloom.decode(payload);
            default -> throw new IOException("unrecognised membership filter tag: " + tag);
        };
    }

    /** Every registered index in scope is present. */
    record All() implements MembershipFilter {
        @Override public char tag() { return 'A'; }
        @Override public String encode() { return "A"; }
        public boolean mightContain(int ordinal) { return true; }

        static All decode(String payload) throws IOException {
            if (!payload.isEmpty()) {
                throw new IOException("tag A carries no payload: " + payload);
            }
            return new All();
        }
    }

    /**
     * No filter. ⚠️ Carries NO membership predicate on purpose: a caller that
     * wants an answer for {@code N} must read the segment header instead
     * (ADR-0003 -- {@code N} and an unrecognised tag are both "must read",
     * never "no match"). Exposing a {@code mightContain} here that always
     * returned {@code true} would be equivalent but easy to forget WHY it is
     * always true; not exposing one forces the caller to handle this case
     * explicitly.
     */
    record None() implements MembershipFilter {
        @Override public char tag() { return 'N'; }
        @Override public String encode() { return "N"; }

        static None decode(String payload) throws IOException {
            if (!payload.isEmpty()) {
                throw new IOException("tag N carries no payload: " + payload);
            }
            return new None();
        }
    }

    /**
     * An exact bitmap over a scoped ordinal space (the mega-index-prefix
     * case, research doc 02 §4). Zero false positives, zero false negatives.
     *
     * <p>⚠️ No stored "universe size": {@link BitSet#get} returns {@code
     * false} for any index at or past the highest set bit, which is exactly
     * the correct "not a member" answer for an ordinal never set -- there is
     * nothing a separate length field would add.
     */
    record ExactBitmap(BitSet bits) implements MembershipFilter {

        public ExactBitmap {
            Objects.requireNonNull(bits, "bits");
        }

        @Override public char tag() { return 'Z'; }

        @Override
        public String encode() {
            return "Z" + urlEncode(bits.toByteArray());
        }

        public boolean mightContain(int ordinal) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal is never negative: " + ordinal);
            }
            return bits.get(ordinal);
        }

        static ExactBitmap decode(String payload) throws IOException {
            return new ExactBitmap(BitSet.valueOf(urlDecode(payload, 'Z')));
        }
    }

    /**
     * A run-length encoded bitmap (dense with gaps): alternating uvarint
     * lengths, OFF then ON then OFF ..., starting with an OFF run (which may
     * be zero), base64url-encoded. Membership is identical to {@link
     * ExactBitmap} once expanded -- RLE is a smaller encoding of the exact
     * same set, not a different notion of membership.
     */
    record RunLength(BitSet bits) implements MembershipFilter {

        public RunLength {
            Objects.requireNonNull(bits, "bits");
        }

        @Override public char tag() { return 'R'; }

        @Override
        public String encode() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            boolean on = false;
            int pos = 0;
            int highest = bits.length(); // one past the highest set bit; 0 if empty
            while (pos < highest) {
                int next = on ? bits.nextClearBit(pos) : bits.nextSetBit(pos);
                if (next < 0 || next > highest) {
                    next = highest;
                }
                SegmentWriter.putUvarint(out, next - pos);
                pos = next;
                on = !on;
            }
            return "R" + urlEncode(out.toByteArray());
        }

        public boolean mightContain(int ordinal) {
            if (ordinal < 0) {
                throw new IllegalArgumentException("ordinal is never negative: " + ordinal);
            }
            return bits.get(ordinal);
        }

        static RunLength decode(String payload) throws IOException {
            byte[] raw = urlDecode(payload, 'R');
            BitSet bits = new BitSet();
            int pos = 0;
            boolean on = false;
            int i = 0;
            while (i < raw.length) {
                long run;
                int shift = 0;
                long value = 0;
                boolean more;
                do {
                    if (i >= raw.length) {
                        throw new IOException("truncated run-length uvarint in R payload");
                    }
                    int b = raw[i++] & 0xFF;
                    value |= (long) (b & 0x7F) << shift;
                    shift += 7;
                    more = (b & 0x80) != 0;
                } while (more);
                run = value;
                if (run < 0 || run > Integer.MAX_VALUE) {
                    throw new IOException("run length out of range in R payload: " + run);
                }
                if (on) {
                    bits.set(pos, pos + (int) run);
                }
                pos += (int) run;
                on = !on;
            }
            return new RunLength(bits);
        }
    }

    /**
     * A Bloom filter over index ordinals, {@code k} probes into an
     * {@code m}-bit array (Kirsch-Mitzenmacher construction and hash-based
     * {@code mightContain} arrive in M2.5 -- this is the raw bits and {@code
     * k} only).
     *
     * <p>⚠️ {@code k} is a SINGLE decimal digit (1-9): the wire grammar
     * {@code B<k><base64url bits>} has no delimiter between {@code k} and the
     * payload, and base64url's alphabet includes digits, so a multi-digit
     * {@code k} would be unparseable. Every {@code k} this project's own FPR
     * tables ever call for (research doc 02 §3: 2, 3, 4, 7) fits.
     *
     * <p>⚠️ {@code m} is NOT a separate wire field -- it is {@code
     * bytes.length * 8} of the encoded payload, always rounded up to a byte
     * boundary. {@code encode()} therefore pads to exactly {@code m} bits
     * (not {@link BitSet#toByteArray()}'s own trimmed length, which drops
     * trailing all-zero bytes) so a decoded filter's {@code m} matches what
     * it was constructed with, which M2.5's {@code h_i = h1 + i·h2 (mod m)}
     * depends on.
     */
    record Bloom(int k, BitSet bits, int m) implements MembershipFilter {

        public Bloom {
            Objects.requireNonNull(bits, "bits");
            if (k < 1 || k > 9) {
                throw new IllegalArgumentException("k is a single decimal digit, 1-9: " + k);
            }
            if (m <= 0 || m % 8 != 0) {
                throw new IllegalArgumentException("m is a positive multiple of 8: " + m);
            }
            if (bits.length() > m) {
                throw new IllegalArgumentException(
                        "a set bit at or past m=" + m + " does not fit this filter's own width");
            }
        }

        @Override public char tag() { return 'B'; }

        @Override
        public String encode() {
            byte[] padded = new byte[m / 8];
            byte[] raw = bits.toByteArray();
            System.arraycopy(raw, 0, padded, 0, raw.length);
            return "B" + k + urlEncode(padded);
        }

        static Bloom decode(String payload) throws IOException {
            if (payload.isEmpty()) {
                throw new IOException("tag B payload is missing its k digit: " + payload);
            }
            char kChar = payload.charAt(0);
            if (kChar < '1' || kChar > '9') {
                throw new IOException("tag B's k must be a single digit 1-9: " + kChar);
            }
            int k = kChar - '0';
            byte[] raw = urlDecode(payload.substring(1), 'B');
            if (raw.length == 0) {
                throw new IOException("tag B carries no bit payload: " + payload);
            }
            return new Bloom(k, BitSet.valueOf(raw), raw.length * 8);
        }
    }

    private static String urlEncode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static byte[] urlDecode(String payload, char tag) throws IOException {
        try {
            return Base64.getUrlDecoder().decode(payload);
        } catch (IllegalArgumentException e) {
            throw new IOException("malformed base64url payload for tag " + tag + ": " + payload, e);
        }
    }
}
