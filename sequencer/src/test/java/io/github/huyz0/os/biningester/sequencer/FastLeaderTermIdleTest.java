// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A leader whose term holds no batch asks the store nothing on its drain's
 * tick (M13.87; ADR-0081 §1, non-negotiable 6): the EXPOSEDs, REPLICAs and
 * upload it would send are none, so its roster is not read -- a journaled
 * pod in a fleet with no fast index otherwise read it once a second for ever.
 */
class FastLeaderTermIdleTest {

    /** A store counting its reads. */
    static final class Reads extends ForwardingBinStore {
        final AtomicInteger gets = new AtomicInteger();

        Reads(MemoryBinStore delegate) {
            super(delegate);
        }

        @Override
        public InputStream get(String key) throws IOException {
            gets.incrementAndGet();
            return super.get(key);
        }
    }

    private static FastLeaderTerm term3(MemoryBinStore store, Reads reads) throws Exception {
        put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        put(store, FastTermStartTest.roster(2, 1, "y", false, 0, 0, false));
        FastTermStartTest.latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(reads, "p"),
                new EmptyTermCloser(reads, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(reads, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, reads, "p", new Mono(), Duration.ofMillis(250));
    }

    @Test
    void anIDLELeadersTickReadsNoRoster() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Reads reads = new Reads(store);
        FastLeaderTerm term = term3(store, reads);
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        CommitDesk desk = term.commitDesk(journal, SELF);
        int before = reads.gets.get();

        term.sendReplicas(desk, (endpoint, frame) -> {
            throw new IOException("nothing is sent");
        });
        term.sendExposed(desk, (endpoint, frame) -> {
            throw new IOException("nothing is sent");
        });
        FastUploader.Chain chain = new FastUploader.Chain() {
            @Override
            public long next(io.github.huyz0.os.biningester.format.RunKey stream) {
                return 0;
            }

            @Override
            public Map<io.github.huyz0.os.biningester.format.RunKey, Long> commitAt(
                    String segmentKey, Map<io.github.huyz0.os.biningester.format.RunKey, Long> firsts,
                    Map<io.github.huyz0.os.biningester.format.RunKey, Integer> counts)
                    throws IOException {
                throw new IOException("nothing is committed");
            }
        };
        assertThat(term.upload(desk, journal, chain, runs -> {
            throw new IOException("nothing is uploaded");
        }, (endpoint, frame) -> {
            throw new IOException("nothing is sent");
        })).isEmpty();

        assertThat(reads.gets.get() - before).as("no roster read with no batch held")
                .isZero();
    }
}
