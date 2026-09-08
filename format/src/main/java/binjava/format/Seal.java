// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * The last entry in an epoch's chain: nothing after this belongs to it.
 *
 * <p>⚠️ COMMITS NO RUNS BY CONSTRUCTION, which is why it is a separate shape
 * rather than a {@link CommitDelta} with an empty run list. That record refuses
 * an empty list for a good reason — "an empty delta would consume a sequence
 * number and commit nothing, so a replay would see a gap it cannot explain" —
 * and relaxing the guard for the seal case would relax it for the accidental
 * case too. A seal consumes a sequence number ON PURPOSE, and the type says so.
 *
 * <p>⚠️ A SEAL IS CLAIMED, NOT OBSERVED. The protocol never asks whether
 * {@code seq N+1} is empty; it races for it with {@code putIfAbsent}. That is
 * what makes a fenced leader collide with the seal rather than slip past it,
 * and it is why the chain must be consecutive.
 *
 * <p>⚠️ {@code continuedAt} IS THE SEALER'S OWN EPOCH, not a guess about
 * someone else's. The seal is written by the NEW leader — it recovers the old
 * chain through {@code seq = N}, then races for {@code N+1} in that chain — so
 * it stamps the epoch it already holds, and it writes {@link Continue} into its
 * own chain moments later. Same term, same process, so the forward and backward
 * links cannot disagree. This is the corpus's design
 * ({@code 03-metadata-and-cas.md} §7), and the forward link is what lets a
 * reader cross the boundary without discovering the next epoch some other way.
 *
 * <p>⚠️ Never 0. Epoch 0 is the UNLEASED chain (M4.4b) and a seal is written by
 * a leader, whose first term is 1 — so a seal continuing at 0 would claim
 * failover TO the chain every leaderless caller writes.
 *
 * @param sequence the slot in the OLD chain this seal claims
 * @param continuedAt the epoch whose chain carries on from here
 */
public record Seal(long sequence, long continuedAt) implements ChainEntry {

    public Seal {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence is never negative: " + sequence);
        }
        if (continuedAt < 1) {
            throw new IllegalArgumentException(
                    "continuedAt names a leader's epoch, which is never below 1: " + continuedAt);
        }
    }

    @Override
    public byte[] encode() {
        var out = ChainEntry.kinded(ChainEntry.KIND_SEAL);
        SegmentWriter.putUvarint(out, sequence);
        SegmentWriter.putUvarint(out, continuedAt);
        return out.toByteArray();
    }
}
