// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The content of {@code <prefix>/ctl/lease/<slot>.json}: who is sequencing, for
 * which term, and until when (ADR-0002, ADR-0007, ADR-0008). Pure encode/decode
 * only — the CAS'd acquire/renew/release orchestration lives in
 * {@code sequencer}'s {@code LeaseManager}, which is the thing that touches
 * {@link binjava.binstore.BinStore}.
 *
 * <p>⚠️ THE EPOCH IS A COUNTER, NOT A CLOCK. This is the load-bearing choice in
 * the whole fencing design (ADR-0002, taking SlateDB's pattern): the store
 * guarantees the counter's uniqueness through the conditional write, so a wrong
 * clock costs availability and never correctness. AutoMQ uses a wall-clock node
 * epoch and can, because KRaft arbitrates stale epochs for them; we have no
 * arbiter, so we cannot.
 *
 * <p>⚠️ {@code expiresAtMillis} is LIVENESS ONLY. A lease is not a lock: safety
 * comes from the epoch in the object path plus the write-once chain, so a clock
 * skew that expires a lease early costs a failover, not a lost write. That is
 * why the corpus says to challenge aggressively rather than conservatively.
 *
 * <p>⚠️ REAL JSON, hand-written, exactly as {@link IndexRegistry} does for the
 * other {@code putIfMatch} CAS'd control object, and for the same reason: when a
 * cluster stops committing, the first question anyone asks is "who holds the
 * lease and until when", and the answer must be legible with {@code cat}. The
 * shape is four flat fields with no nesting and no floats. ⚠️ There is no
 * ESCAPING either, so the constructor refuses every character the encoder
 * could not represent — see {@code rejectUnrepresentable}. An earlier draft of
 * this javadoc justified the absence by saying "a {@code podId} refuses
 * {@code -} and {@code /} upstream", which was wrong twice over: that guard is
 * on another module's record and never runs here, and those are not the
 * JSON-hostile characters. Narrowing the domain is the cheap moment now;
 * adding escaping once leases exist would be a format change
 * (non-negotiable 8).
 *
 * @param epoch the term of leadership, in the object path so a fenced writer's
 *     in-flight PUT lands where readers of the new epoch never look
 * @param holderPodId which node holds it
 * @param holderEndpoint where to reach that node. ⚠️ EMPTY in M4, which has no
 *     peer mesh to publish; the field is in the shape from the first lease so
 *     that M5's commit forwarding — which must REACH the holder — is not a
 *     format change
 * @param expiresAtMillis when it lapses, on the injected clock's timeline
 */
public record Lease(long epoch, String holderPodId, String holderEndpoint, long expiresAtMillis) {

    public Lease {
        Objects.requireNonNull(holderPodId, "holderPodId");
        Objects.requireNonNull(holderEndpoint, "holderEndpoint");
        if (epoch < 0) {
            // ⚠️ Epochs only ever increase. A negative one could only come
            // from corruption or a hand edit, and it would order BEFORE every
            // real epoch — the one thing fencing must never allow.
            // ⚠️ 0 is PERMITTED here but is never a LEASE's epoch: it names the
            // unleased chain `CommitLog`'s 2-arg constructor writes, and
            // `LeaseManager` starts its first term at 1 (M4.4b). This record
            // does not refuse 0, because `decode` must still parse a legacy or
            // hand-edited object rather than turn it into a liveness stall —
            // the reservation is enforced where leases are MINTED.
            throw new IllegalArgumentException("epoch is never negative: " + epoch);
        }
        if (holderPodId.isBlank()) {
            throw new IllegalArgumentException("holderPodId is never blank");
        }
        // ⚠️ THE ENCODER CANNOT ESCAPE, so refuse what it cannot represent.
        // Round-1 review measured the alternative: a podId of `pod"1` encodes
        // to invalid JSON and decodes back as `pod`, silently and with no
        // exception -- and an endpoint carrying `","expiresAtMillis":0,"x":"`
        // INJECTS A FIELD, so decode returns an already-expired lease. That is
        // a fabricated answer to "who may write the commit chain".
        // ⚠️ Refused now rather than escaped later: adding escaping once M4.3b
        // and M5 have written leases would be a format change (non-negotiable
        // 8). The characters are JSON's own, not the key grammar's -- the
        // `-`/`/` restriction `CommitRequest` carries lives in another module
        // and would not have refused a quote.
        rejectUnrepresentable("holderPodId", holderPodId);
        rejectUnrepresentable("holderEndpoint", holderEndpoint);
        if (expiresAtMillis < 0) {
            throw new IllegalArgumentException(
                    "expiresAtMillis is never negative: " + expiresAtMillis);
        }
    }

    private static void rejectUnrepresentable(String field, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\' || c < 0x20) {
                throw new IllegalArgumentException(
                        field + " may not contain a quote, a backslash or a control character: "
                                + field + "[" + i + "]=0x" + Integer.toHexString(c));
            }
        }
        // ⚠️ UNPAIRED SURROGATES too, and this one is subtle enough that both
        // round-2 reviewers found it independently: an unpaired surrogate is
        // none of the three characters above, but `getBytes(UTF_8)` silently
        // replaces it with '?', so the round trip is not identity and NOTHING
        // throws. The canonical check in `decode` cannot see it either --
        // both sides derive from the same lossy conversion. Same silent
        // corruption as the quote case, on the field that answers "who may
        // write the commit chain".
        // ⚠️ `codePoints()` pairs surrogates correctly, so a well-formed
        // astral character (an emoji in a pod name) is still legal; only an
        // UNPAIRED half reaches the surrogate range here.
        // ⚠️ The RANGE, not `Character::isSurrogate` -- that takes a `char`,
        // so a code point cast down to one is misclassified whenever its low
        // 16 bits happen to land in 0xD800-0xDFFF. Round-3 review measured
        // that: 32,768 of 1,048,576 astral code points, 3.13% -- U+1DC00 among
        // them, which the test uses. ⚠️ NOT "every astral character", which an
        // earlier draft of this comment claimed: U+1F600 casts to U+F600,
        // Private Use, so an emoji alone does not expose the difference.
        if (value.codePoints().anyMatch(cp -> cp >= 0xD800 && cp <= 0xDFFF)) {
            throw new IllegalArgumentException(
                    field + " may not contain an unpaired surrogate: " + field);
        }
    }

    /**
     * Whether this lease has lapsed at {@code nowMillis}.
     *
     * <p>⚠️ The instant is SUPPLIED, never read from the system clock — the
     * clock is injected all the way down (non-negotiable 7), and M4.12's
     * simulation drives expiry deterministically. ⚠️ Expiry is INCLUSIVE: at
     * exactly {@code expiresAtMillis} the lease is gone. A holder treating it as
     * exclusive keeps sequencing one tick past its own term.
     */
    public boolean isExpiredAt(long nowMillis) {
        return nowMillis >= expiresAtMillis;
    }

    /**
     * The same term, extended.
     *
     * <p>⚠️ RENEWAL MUST NOT BUMP THE EPOCH. The epoch identifies a term and is
     * in the object path; bumping it on every renew would start a new chain
     * every few seconds and orphan the one before it.
     */
    public Lease renewedUntil(long newExpiresAtMillis) {
        return new Lease(epoch, holderPodId, holderEndpoint, newExpiresAtMillis);
    }

    /**
     * The next term, held by someone else.
     *
     * <p>⚠️ The epoch advances by EXACTLY ONE. Skipping would leave gaps a
     * reader following the chain cannot tell from an epoch it failed to read;
     * reusing would let a fenced writer's objects pass for the new term's.
     */
    public Lease takenOverBy(String podId, String endpoint, long newExpiresAtMillis) {
        return new Lease(epoch + 1, podId, endpoint, newExpiresAtMillis);
    }

    /** ⚠️ Fixed field order, so equal leases are byte-identical — see the test. */
    public byte[] encode() {
        String json = "{\"epoch\":" + epoch
                + ",\"holderPodId\":\"" + holderPodId + '"'
                + ",\"holderEndpoint\":\"" + holderEndpoint + '"'
                + ",\"expiresAtMillis\":" + expiresAtMillis + '}';
        return json.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * ⚠️ REFUSES anything that is not exactly this shape, loudly and now. A
     * lease a future reader cannot parse makes leadership undecidable, and the
     * cluster would stop committing with no explanation in the object itself.
     */
    public static Lease decode(byte[] bytes) throws IOException {
        if (bytes == null) {
            // ⚠️ Inside the IOException contract, like every other rejection
            // here: a caller catching IOException on the recovery path must not
            // meet an unchecked exception from this method.
            throw new IOException("no lease bytes");
        }
        // ⚠️ A STRICT decoder, not `new String(bytes, UTF_8)`. That one
        // replaces malformed input with U+FFFD, and the canonical comparison
        // below would then re-encode the already-lossy string and compare it
        // against itself -- so a corrupt byte inside a podId was ACCEPTED,
        // yielding a holder nobody is called. Round-3 review measured it. Same
        // lossy-conversion blind spot as the unpaired-surrogate case, mirrored
        // onto the decode side. `CharacterCodingException` is an `IOException`,
        // so it needs no special handling here.
        // ⚠️ `strip()` is load-bearing for the `cat`/hand-edit story: a lease
        // written with `echo` carries a trailing newline and must still decode.
        // SURROUNDING whitespace is tolerated; INTERIOR whitespace is not,
        // because the canonical comparison below rejects it.
        String s = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                .decode(java.nio.ByteBuffer.wrap(bytes)).toString().strip();
        if (s.length() < 2 || s.charAt(0) != '{' || s.charAt(s.length() - 1) != '}') {
            throw new IOException("not a lease object: " + s);
        }
        Lease parsed;
        try {
            parsed = new Lease(
                    longField(s, "epoch"),
                    stringField(s, "holderPodId"),
                    stringField(s, "holderEndpoint"),
                    longField(s, "expiresAtMillis"));
        } catch (IllegalArgumentException | NullPointerException e) {
            // ⚠️ `decode` is declared `throws IOException` and every caller --
            // M4.3b's LeaseManager on the recovery path -- catches exactly
            // that. An unchecked exception escaping from the constructor
            // underneath would bypass all of them.
            throw new IOException("not a valid lease: " + s, e);
        }
        // ⚠️ CANONICAL COMPARISON, and it does the work of three separate
        // guards at once: it refuses DUPLICATE fields (`{"epoch":1,"epoch":99}`
        // decoded to 1 while jq says 99 -- so an operator verifying the fencing
        // counter with jq would get a different answer than the code uses),
        // UNKNOWN fields (the SPEC lists a lease-carried pointer as a candidate
        // M4.8 may pick, and with no version field an older reader must refuse
        // what it cannot account for rather than silently drop it), and any
        // REORDERING or stray whitespace. IndexRegistry refuses duplicates for
        // the same reason.
        if (!java.util.Arrays.equals(parsed.encode(), s.getBytes(StandardCharsets.UTF_8))) {
            throw new IOException("lease is not in canonical form: " + s);
        }
        return parsed;
    }

    private static String stringField(String s, String name) throws IOException {
        String marker = '"' + name + "\":\"";
        int at = s.indexOf(marker);
        if (at < 0) {
            throw new IOException("lease is missing " + name + ": " + s);
        }
        int from = at + marker.length();
        int end = s.indexOf('"', from);
        if (end < 0) {
            throw new IOException("lease has an unterminated " + name + ": " + s);
        }
        return s.substring(from, end);
    }

    private static long longField(String s, String name) throws IOException {
        String marker = '"' + name + "\":";
        int at = s.indexOf(marker);
        if (at < 0) {
            throw new IOException("lease is missing " + name + ": " + s);
        }
        int from = at + marker.length();
        int end = from;
        while (end < s.length() && (Character.isDigit(s.charAt(end)) || s.charAt(end) == '-')) {
            end++;
        }
        if (end == from) {
            // ⚠️ A quoted number, or nothing at all. Refused rather than
            // coerced: a lease whose epoch is a string is a lease somebody
            // hand-edited, and guessing what they meant is how a fenced writer
            // gets mistaken for the current one.
            throw new IOException("lease has a non-numeric " + name + ": " + s);
        }
        try {
            return Long.parseLong(s.substring(from, end));
        } catch (NumberFormatException e) {
            throw new IOException("lease has a malformed " + name + ": " + s, e);
        }
    }
}
