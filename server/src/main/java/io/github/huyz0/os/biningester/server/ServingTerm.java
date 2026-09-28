// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.http.CatchUpService;
import io.github.huyz0.os.biningester.ingest.DurableCatchUpResponder;
import io.github.huyz0.os.biningester.ingest.RetentionLoop;
import io.github.huyz0.os.biningester.ingest.SegmentGc;
import io.github.huyz0.os.biningester.ingest.SnapshotCommittedDeltaSource;
import io.github.huyz0.os.biningester.sequencer.ChainCollector;
import io.github.huyz0.os.biningester.sequencer.ChainMemory;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import java.io.IOException;
import java.util.Optional;

/**
 * What a node does with the committed term it currently SERVES: the term the
 * retention loop works on, and a catch-up response from its chain. Moved out
 * of {@link Assembly} unchanged by M11.24b.
 */
final class ServingTerm {

    private ServingTerm() {
    }

    /**
     * The term the retention loop works on, if this node serves one.
     *
     * <p>⚠️ {@code serving()}, NOT MERELY {@code instanceof}: a term this node
     * still HOLDS may already have been fenced by a takeover, and its frozen
     * chain as a keep list deletes the successor's committed segments.
     */
    static Optional<RetentionLoop.Term> retention(FleetSequencer sequencer, String prefix) {
        return LocalSequencer.underneath(sequencer.heldTerm()).filter(LocalSequencer::serving)
                .map(local -> new RetentionLoop.Term(local.chain(), local::observeRetained,
                        local::serving, fenced -> new ChainCollector(local, prefix)
                                .collect(fenced, SegmentGc.DEFAULT_DELETE_BATCH)));
    }

    /** Responds from this node's currently-served committed chain without electing a term. */
    static void respondCatchUp(FleetSequencer sequencer, BinStore store, IndexCostLedger costLedger,
            CatchUpRequestFrame request, CatchUpService.FrameSink sink) throws IOException {
        LocalSequencer local = LocalSequencer.underneath(sequencer.heldTerm())
                .filter(LocalSequencer::serving)
                .orElseThrow(() -> new IOException("this node has no serving committed chain"));
        ChainMemory.Snapshot snapshot = local.chain().snapshot();
        if (!snapshot.complete()) {
            throw new IOException("the committed chain is incomplete and cannot replay safely");
        }
        new DurableCatchUpResponder(store,
                new SnapshotCommittedDeltaSource(() -> snapshot), local::epoch,
                costLedger)
                .respond(request, sink::write);
    }
}
