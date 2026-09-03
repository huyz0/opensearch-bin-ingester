// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * The first entry in a new epoch's chain, linking back across the boundary.
 *
 * <p>⚠️ ABSENCE IS NOT REPRESENTABLE, and that is the whole design rather than
 * an omission. M4.4b reserved epoch 0 for the UNLEASED chain, so 0 names a live
 * epoch and cannot double as "no previous chain" — a sentinel there would make
 * a first leader's link indistinguishable from a real link to the chain every
 * unleased caller writes. The resolution is not a different sentinel: every
 * chain above epoch 0 has a predecessor, and epoch 0 is the unleased chain,
 * which conceptually always exists. A first leader at epoch 1 writes
 * {@code {prevEpoch=0, prevSeq=0}}, which truthfully says the unleased chain
 * was empty — not that there was none.
 *
 * <p>⚠️ Commits no runs, for the same reason {@link Seal} does not.
 *
 * @param sequence where this entry sits in its OWN chain — normally 0, since
 *     it opens one
 * @param prevEpoch the epoch this chain continues from
 * @param prevSeq how far that chain got, so a reader knows where its sealed
 *     prefix ends without reading it
 */
public record Continue(long sequence, long prevEpoch, long prevSeq) implements ChainEntry {

    public Continue {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence is never negative: " + sequence);
        }
        if (prevEpoch < 0) {
            throw new IllegalArgumentException("prevEpoch is never negative: " + prevEpoch);
        }
        if (prevSeq < 0) {
            throw new IllegalArgumentException("prevSeq is never negative: " + prevSeq);
        }
    }

    @Override
    public byte[] encode() {
        var out = ChainEntry.kinded(ChainEntry.KIND_CONTINUE);
        SegmentWriter.putUvarint(out, sequence);
        SegmentWriter.putUvarint(out, prevEpoch);
        SegmentWriter.putUvarint(out, prevSeq);
        return out.toByteArray();
    }
}
