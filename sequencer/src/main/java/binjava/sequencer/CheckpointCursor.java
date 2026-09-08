// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.Checkpoint;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * WHERE a bounded replay may start: the newest checkpoint of one chain.
 *
 * <p>⚠️ Split out of {@link ChainReplay} when M4.9's bounded crossing pushed
 * that file past the 500-line limit — code-structure.md rule 1, split rather
 * than raise. The seam is ADR-0034's own: DISCOVERY is a mechanism in its own
 * right, chosen and recorded separately from the replay that consumes it.
 */
final class CheckpointCursor {

    private CheckpointCursor() {
    }

    /**
     * The NEWEST checkpoint of {@code epoch}, or empty if it has none.
     *
     * <p>⚠️ A CONSTANT NUMBER OF REQUESTS, INDEPENDENT OF HOW MANY CHECKPOINTS
     * EXIST — one {@code stat} on a cold chain, one {@code stat} and one
     * {@code get} on a warm one. That is the property AC5 needs and it is
     * strictly stronger than the "zero {@code list}" it states: a backward walk
     * from the chain head, and probing the sequence space with {@code stat},
     * both issue zero LISTs while costing one request per checkpoint.
     * ADR-0034.
     *
     * <p>⚠️ IT ANSWERS FOR ONE EPOCH, the one passed. A leader acquires a FRESH
     * epoch — {@code start} takes {@code won.get().epoch()}, and M4.6e records
     * that every restart, rolling deploy, TTL expiry and failed {@code start}
     * burns one — so at a takeover its OWN chain has no checkpoint and this
     * returns empty for it. The checkpoints that bound the replay are the
     * PREDECESSOR's.
     *
     * <p>⚠️ AN EARLIER VERSION SAID M4.9 MUST NOT CALL THIS WITH ITS OWN EPOCH.
     * M4.9 does, deliberately: {@code ChainReplay}'s ancestry walk probes the epoch it is
     * given BEFORE walking, which is what bounds {@code inherited()} — the call
     * that actually crosses, and which starts AT the predecessor. Probing only
     * epochs reached by following a {@code CONTINUE} skipped exactly that one
     * and left the bound doing nothing. The own-epoch probe costs one
     * {@code stat} that usually misses (ADR-0035) and bounds
     * {@code CommitLog.recover()} on a chain that does have its own checkpoint.
     *
     * <p>⚠️ AN EARLIER VERSION OF THIS PARAGRAPH SAID THE OPPOSITE — that a
     * chain-wide answer costs one {@code stat} per ancestor and "nothing needs
     * it". The ARITHMETIC was right and the CONCLUSION was wrong, and a first
     * correction overstated that as "both halves were false". M4.9's row
     * requires the CROSSING to be bounded, not merely own-chain replay, and a
     * {@code Checkpoint} IS offsets plus a sequence, so a predecessor's is
     * exactly what bounds it.
     *
     * <p>⚠️ ONE {@code stat} PER ANCESTOR IS CHEAP BUT NOT BOUNDED, and M4.9
     * owns the difference. It replaces one GET per entry of every ancestor
     * term — which M4.6e measured growing with the cluster's whole history — so
     * it is the better side of that trade; but it is {@code O(epochs walked)},
     * and an outage that burns a run of epochs makes that a page-count
     * weakening in a new dimension. M4.9 bounds the WALK as well as the replay: the probe below is what
     * stops it, and `LocalSequencerBoundedRecoveryTest` measures the result.
     *
     * <p>⚠️ {@code stat} FIRST, NEVER A CAUGHT {@code get}. {@link BinStore}
     * defines {@code IOException} as "the store is unreachable" and has no
     * not-found type, so catching it and returning empty would report an outage
     * as a chain that has never checkpointed — and M4.9 would then replay the
     * whole term believing that was correct.
     */
    static Optional<Checkpoint> newest(BinStore store, String prefix, long epoch)
            throws IOException {
        String pointer = new LogKeys(prefix, epoch).latestCheckpointKey();
        if (store.stat(pointer).isEmpty()) {
            return Optional.empty();
        }
        try (InputStream in = store.get(pointer)) {
            return Optional.of(Checkpoint.decode(in.readAllBytes()));
        }
    }
}
