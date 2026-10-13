// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.FastBatcher;
import io.github.huyz0.os.biningester.ingest.FastPath;
import io.github.huyz0.os.biningester.ingest.FastWriterPath;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.FastWriter;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Where a {@code wal=true} index's appends go (M13.29e; ADR-0081 §2): batched
 * into COMMITs to the term this pod leads, else to the newest term it joined
 * -- the leader is a writer of its own term too, and joins none of its own.
 */
final class FastRouting {

    /** COMMITs of one batch, its retries under one key included. */
    static final int ATTEMPTS = 3;

    private FastRouting() {
    }

    /** Whether the catalog says an index is fast: an unknown one is not. */
    static Predicate<String> fastIndices(IndexCatalog catalog) {
        return index -> catalog.resolve(index).map(IndexRegistration::wal).orElse(false);
    }

    /** With no fast index, the deadline ADR-0013 gives {@code flush_timer} by default. */
    static final java.time.Duration DEFAULT_FLUSH_TIMER = java.time.Duration.ofSeconds(5);

    /**
     * The tightest {@code flush_timer} among the catalog's fast indices: the
     * upload's deadline (ADR-0081 §7; M13.31c).
     */
    static java.time.Duration tightestFlushTimer(IndexCatalog catalog) {
        return catalog.fastRegistrations().stream()
                .map(r -> java.time.Duration.ofMillis(r.flushTimerMillis()))
                .min(java.time.Duration::compareTo).orElse(DEFAULT_FLUSH_TIMER);
    }

    /**
     * The term to write through: the one {@code held} serves, led by this
     * pod, else the newest {@code joined} names.
     *
     * <p>⚠️ THE TERM THIS POD LEADS FIRST: a pod that joined a term as a
     * follower and then won the next would otherwise send every batch to the
     * deposed one, refused as fenced on every retry.
     */
    static Supplier<Optional<JoinedTerms.Leader>> leaders(Supplier<Sequencer> held,
            JoinedTerms joined, Roster.Incarnation self) {
        return () -> {
            Optional<LocalSequencer> leading = LocalSequencer.underneath(held.get())
                    .filter(LocalSequencer::serving);
            if (leading.isPresent()) {
                return Optional.of(new JoinedTerms.Leader(leading.get().epoch(), self.podUid(),
                        self.endpoint()));
            }
            return joined.latest();
        };
    }

    /** The fast path over this pod's {@link FastWriter}, batched. */
    static FastPath path(ServerConfig config, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport, Supplier<Sequencer> held, UUID incarnationId,
            Duration exposedWait) {
        FastWriter writer = new FastWriter(self, transport, disk.fence(), disk.journal(),
                disk.exposed(), exposedWait);
        return new FastBatcher(new FastWriterPath(config.podId(), incarnationId,
                leaders(held, disk.joined(), self), writer::write, ATTEMPTS,
                FastLeaderTerm.STREAM_BOUND), FastLeaderTerm.STREAM_BOUND);
    }
}
