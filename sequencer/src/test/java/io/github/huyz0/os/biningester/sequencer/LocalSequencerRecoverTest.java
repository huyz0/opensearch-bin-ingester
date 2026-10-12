// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A takeover's recovery entry goes through the term's chain writer (M13.33o):
 * the chain is single-writer, so it is appended under the same lock as every
 * default-path commit, and refused by a term that released its lease.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerRecoverTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final String PREFIX = "bins/cluster-a";

    private static final class FixedClock extends Clock {
        @Override public long millis() {
            return 1_000_000L;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId z) {
            return this;
        }
    }

    private static LocalSequencer start(BinStore store) throws IOException {
        return LocalSequencer.start(store, PREFIX, new LeaseManager(store,
                new LeaseConfig(PREFIX, "pod-a", "", Duration.ofSeconds(10),
                        Duration.ofSeconds(3)), new FixedClock()), 8).orElseThrow();
    }

    /** S decided to commit two records at offset 0 and resume at 2. */
    private static StreamDecision.Decided decided() {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            records.add(new SegmentRecord("d" + i, OpType.INDEX, OptionalLong.empty(),
                    new byte[] {1}));
        }
        return new StreamDecision.Decided(List.of(new FastJournalRecord.Entry(1, S, 0, 1, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 0), records)),
                List.of(), 2);
    }

    @Test
    void aRECOVERYIsAppendedAtTheChainsNextSlotAndMovesTheStreamsNextOffset()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer term = start(store);
        AtomicInteger uploads = new AtomicInteger();

        Recovery written = term.recover(List.of(decided()), runs -> {
            uploads.incrementAndGet();
            return "seg-0";
        });

        assertThat(uploads).hasValue(1);
        assertThat(term.committedNext(S)).isEqualTo(2);
        try (InputStream in = store.get(new CommitLog(store, PREFIX, term.epoch())
                .keyFor(written.sequence()))) {
            assertThat(ChainEntry.decode(in.readAllBytes())).isEqualTo(written);
        }
    }

    @Test
    void aFASTUploadIsCommittedAtTheOffsetsItWasAssigned() throws Exception {
        // ⚠️ M13.31a: its offsets were the chain's next when assigned.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer term = start(store);

        CommitDelta delta = term.commitAt(new CommitRequest("poda", "fast1", 0, "seg-f",
                java.util.Map.of(S, 3)), java.util.Map.of(S, 0L));

        assertThat(delta.segments()).singleElement().satisfies(seg -> assertThat(seg.runs())
                .singleElement().satisfies(r -> assertThat(r.firstOffset()).isZero()));
        assertThat(term.committedNext(S)).isEqualTo(3);
    }

    @Test
    void aFASTUploadAtOffsetsTheChainDidNotReachIsRefused() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer term = start(store);

        assertThatThrownBy(() -> term.commitAt(new CommitRequest("poda", "fast1", 0, "seg-f",
                java.util.Map.of(S, 3)), java.util.Map.of(S, 5L)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(term.committedNext(S)).as("nothing committed").isZero();
    }

    @Test
    void aTERMThatReleasedItsLeaseRecoversNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer term = start(store);
        term.close();
        AtomicInteger uploads = new AtomicInteger();

        assertThatThrownBy(() -> term.recover(List.of(decided()), runs -> {
            uploads.incrementAndGet();
            return "seg-0";
        })).isInstanceOf(IOException.class).hasMessageContaining("released");
        assertThat(uploads).as("nothing uploaded either").hasValue(0);
    }
}
