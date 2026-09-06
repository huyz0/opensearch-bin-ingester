// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import java.util.Locale;

/**
 * WHERE a chain's entries live, and which keys are its own.
 *
 * <p>⚠️ Split out of {@link CommitLog} when M4.8b1's key filter pushed that file
 * past the 500-line limit. code-structure.md rule 1: split it, do not raise the
 * limit. The seam is the one {@link ChainReplay} already asserts in its own
 * javadoc — <em>addressing comes from {@code CommitLog}, never re-derived</em> —
 * and honouring it had made both readers construct a whole {@code CommitLog}, a
 * WRITER, to ask a naming question. Naming is not writing.
 *
 * <p>⚠️ ZERO-PADDED TO 16 HEX DIGITS so lexicographic order IS numeric order.
 * ADR-0022 removed {@code lastModifiedMillis}, leaving key order as the only
 * ordering a reader has, so the padding is load-bearing rather than cosmetic:
 * unpadded, {@code 10.delta} sorts before {@code 9.delta}.
 *
 * <p>⚠️ {@code Locale.ROOT}: an object key is a wire value and must not depend
 * on the process's locale.
 */
record LogKeys(String prefix, long epoch) {

    private static final String SUFFIX = ".delta";

    /** Everything this chain writes sorts under here. */
    String logPrefix() {
        return String.format(Locale.ROOT, "%s/ctl/log/0/%016x/", prefix, epoch);
    }

    /** The one key slot {@code sequence} is written at. */
    String keyFor(long sequence) {
        return String.format(Locale.ROOT, "%s%016x%s", logPrefix(), sequence, SUFFIX);
    }

    /**
     * Where the checkpoint covering up to {@code sequence} is written.
     *
     * <p>⚠️ UNDER {@link #logPrefix}, which is why {@link #isEntryKey} exists:
     * {@code BinStore.list} is flat, so these come back from the chain's own
     * LIST and every reader must skip them by key. M4.8b1 landed that filter
     * before this grammar for exactly that reason.
     *
     * <p>⚠️ ZERO-PADDED, like {@link #keyFor} and for the same reason —
     * lexicographic order IS numeric order (ADR-0022 left key order as the only
     * ordering a reader has). Unpadded, {@code 10.ckpt} sorts before
     * {@code 9.ckpt}. ⚠️ M4.9 DOES NOT FIND THE NEWEST BY TAKING THE LAST OF
     * THESE, and an earlier version of this sentence said it did — that is the
     * {@code list}-and-take-last mechanism ADR-0034 rejects. It reads
     * {@link #latestCheckpointKey}. The padding still matters, because the
     * seq-keyed objects are the ordered history M7 prunes.
     */
    String checkpointKeyFor(long sequence) {
        return String.format(Locale.ROOT, "%sckpt/%016x.ckpt", logPrefix(), sequence);
    }

    /**
     * The pointer to the NEWEST checkpoint, at a key known without listing.
     *
     * <p>⚠️ NO SEQUENCE IN IT, which is the whole point: a reader that must
     * LIST or probe to learn the newest sequence cannot be constant in the
     * number of checkpoints. ADR-0034.
     *
     * <p>⚠️ IT DOES NOT END IN {@code .delta}, so {@link #isEntryKey} refuses it
     * and M4.8b1's filter skips it at both recovery sites — the same protection
     * that lets the {@code ckpt/} objects live under this prefix at all.
     */
    String latestCheckpointKey() {
        return logPrefix() + "ckpt/LATEST";
    }

    /**
     * Whether {@code key} is one this chain WROTE, rather than merely one that
     * sorts under its prefix.
     *
     * <p>⚠️ {@code BinStore.list} IS FLAT AND UNDELIMITED, so a LIST of
     * {@link #logPrefix} returns everything beneath it — including the
     * {@code ckpt/...} objects M4.8b2 writes there, which sort AFTER every entry
     * because every hex digit is below {@code 'k'}. Without this filter an
     * ordinary checkpoint is read as the chain end, fails to decode, and
     * {@code LocalSequencer.start} releases the lease on every takeover forever,
     * since nothing ever deletes it.
     *
     * <p>⚠️ AN ALLOW-LIST ON THE KEY, NEVER A CAUGHT DECODE FAILURE. Skipping a
     * key whose BYTES will not decode is I3 — it lets a reader step over a
     * {@code Seal} it does not understand and apply a discarded suffix, which
     * {@code CommitLogTest.recoveryStopsOnAnEntryItCannotUnderstandRatherThanSkippingIt}
     * exists to forbid. The question here is only whether the KEY is ours;
     * anything that passes is still decoded, and still throws if it is corrupt.
     *
     * <p>⚠️ THE GRAMMAR IS NOT RESTATED. This is {@link #keyFor} run backwards:
     * parse the candidate and require the canonical key to regenerate
     * byte-identically. A second copy — a regex, a bare {@code endsWith} — is
     * the wire-format duplication this class exists to prevent, and would
     * silently admit an unpadded, uppercase, wrong-length or one-segment-deeper
     * key that no writer can produce. Measured: with a bare
     * {@code endsWith(SUFFIX)}, a valid delta parked at
     * {@code <logPrefix>ckpt/<seq>.delta} decodes and drives {@code nextSequence}
     * past the chain's real end, with nothing thrown and nothing logged.
     */
    boolean isEntryKey(String key) {
        String logPrefix = logPrefix();
        if (!key.startsWith(logPrefix) || !key.endsWith(SUFFIX)) {
            return false;
        }
        String digits = key.substring(logPrefix.length(), key.length() - SUFFIX.length());
        long sequence;
        try {
            sequence = Long.parseUnsignedLong(digits, 16);
        } catch (NumberFormatException notOurs) {
            return false;
        }
        return key.equals(keyFor(sequence));
    }
}
