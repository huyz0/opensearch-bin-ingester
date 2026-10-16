// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.INDEX;
import static io.github.huyz0.os.biningester.server.TakeoverRig.S;
import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The real takeover over seeds (SPEC "Suites extended"; M13.33u5): one to
 * three unclosed terms led by {@code x}, a chain at a random next offset,
 * fast entries on {@code x} and {@code m} with holes, either pod gone or
 * answering, {@code q} one or two -- each seed's recovery held to an oracle
 * that reads only the generated facts, never the code under test, and a
 * commit of the new term following it.
 *
 * <p>⚠️ SCOPE, said plainly: this drives leader death and quorum loss
 * through the real chain, store and takeover, and the new term's commits
 * after them. A deposed leader and a death mid-takeover are not driven here;
 * M13.33u5b owns them.
 */
@Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastTakeoverSeedsTest {

    private static final int SEEDS = 150;

    /** What one seed generated: the entries each pod holds, who is gone, and q. */
    private record Case(int unclosed, long c0, int q, boolean xGone, boolean mGone,
            Map<String, List<FastJournalRecord.Entry>> held) {
    }

    private static Case generate(long seed) {
        Random random = new Random(seed);
        int unclosed = 1 + random.nextInt(3);
        long c0 = random.nextInt(4) == 0 ? 0 : 3 + random.nextInt(20);
        int q = 1 + random.nextInt(2);
        boolean xGone = random.nextBoolean();
        boolean mGone = random.nextInt(3) == 0;
        List<FastJournalRecord.Entry> onX = new ArrayList<>();
        List<FastJournalRecord.Entry> onM = new ArrayList<>();
        // x's terms assign contiguously from c0 (some below it, committed)
        long offset = Math.max(0, c0 - random.nextInt(3));
        for (int epoch = 1; epoch <= unclosed; epoch++) {
            int assigned = random.nextInt(4);
            for (int i = 0; i < assigned; i++, offset++) {
                FastJournalRecord.Entry e = entry(S, q, epoch, offset);
                if (random.nextInt(4) != 0) {
                    onX.add(e);
                }
                if (random.nextInt(3) != 0) {
                    onM.add(e);
                }
            }
        }
        return new Case(unclosed, c0, q, xGone, mGone, Map.of("uid-x", onX, "uid-m", onM));
    }

    /** Whether the oracle says S lost quorum: x gone, and for q = 2 the az-m pod too. */
    private static boolean lost(Case c) {
        return c.xGone() && (c.q() == 1 || c.mGone());
    }

    private static Map<Long, FastJournalRecord.Entry> live(Case c) {
        Map<Long, FastJournalRecord.Entry> live = new TreeMap<>();
        for (Map.Entry<String, List<FastJournalRecord.Entry>> h : c.held().entrySet()) {
            boolean gone = h.getKey().equals("uid-x") ? c.xGone() : c.mGone();
            if (gone) {
                continue;
            }
            h.getValue().stream().filter(e -> e.firstOffset() >= c.c0())
                    .forEach(e -> live.putIfAbsent(e.firstOffset(), e));
        }
        return live;
    }

    @Test
    void overSEEDSTheTakeoversRecoveryMatchesTheOracleAndTheChainContinues() throws Exception {
        int lostSeeds = 0;
        int survivedSeeds = 0;
        int voidSeeds = 0;
        int committedSeeds = 0;
        int continued = 0;
        for (long seed = 0; seed < SEEDS; seed++) {
            Case c = generate(seed);
            Map<Long, FastJournalRecord.Entry> live = live(c);
            boolean lost = lost(c);
            List<Recovery.VoidRange> expectedVoids = new ArrayList<>();
            long next = c.c0();
            long expectedCommits = 0;
            for (long offset : live.keySet()) {
                if (offset != next && !lost) {
                    break;
                }
                if (offset > next) {
                    expectedVoids.add(new Recovery.VoidRange(S, next, offset));
                }
                next = offset + 1;
                expectedCommits++;
            }
            if (lost && next < c.c0() + FastLeaderTerm.STREAM_BOUND) {
                expectedVoids.add(new Recovery.VoidRange(S, next,
                        c.c0() + FastLeaderTerm.STREAM_BOUND));
                next = c.c0() + FastLeaderTerm.STREAM_BOUND;
            }

            try (TakeoverRig rig = new TakeoverRig(c.unclosed(), c.c0(), c.held(),
                    Map.of(INDEX, c.q()))) {
                assertThat(rig.takeOver(uid -> uid.equals("uid-x") && c.xGone()
                        || uid.equals("uid-m") && c.mGone())).as("seed " + seed).isTrue();

                assertThat(rig.voids).as("seed " + seed + " " + c + " voids")
                        .isEqualTo(expectedVoids);
                assertThat(rig.term.committedNext(S)).as("seed " + seed + " resumed at")
                        .isEqualTo(next);
                long recordsRecovered = rig.recovered.stream()
                        .mapToLong(r -> Long.parseLong(r.substring(r.indexOf('+') + 1,
                                r.indexOf('@'))))
                        .sum();
                assertThat(recordsRecovered).as("seed " + seed + " committed records")
                        .isEqualTo(expectedCommits);

                rig.term.commitAll(List.of(new CommitRequest("self", "i2", 0, "seg/next",
                        Map.of(S, 2))));
                assertThat(rig.term.committedNext(S)).as("seed " + seed + " the chain continues")
                        .isEqualTo(next + 2);
                continued++;
            }
            if (lost) {
                lostSeeds++;
            } else {
                survivedSeeds++;
            }
            if (!expectedVoids.isEmpty()) {
                voidSeeds++;
            }
            if (expectedCommits > 0) {
                committedSeeds++;
            }
        }
        assertThat(lostSeeds).as("quorum loss reached").isGreaterThan(SEEDS / 5);
        assertThat(survivedSeeds).as("a survived leader loss reached").isGreaterThan(SEEDS / 5);
        assertThat(voidSeeds).as("voids reached").isGreaterThan(SEEDS / 5);
        assertThat(committedSeeds).as("recovered commits reached").isGreaterThan(SEEDS / 5);
        assertThat(continued).isEqualTo(SEEDS);
    }
}
