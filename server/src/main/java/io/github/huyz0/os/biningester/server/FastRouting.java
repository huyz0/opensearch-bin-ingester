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
