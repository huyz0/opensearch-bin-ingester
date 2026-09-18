// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.StoreCounts;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.RunKey;
import binjava.format.SegmentKey;
import binjava.sequencer.CommitLog;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The retention loop: what a tick costs, what it may delete, and what it must
 * leave alone (M8.5, FR-9, NFR-2, NFR-3).
 *
 * <p>⚠️ **OVER A REAL {@code CommitLog} AND ITS REAL CHAIN**, never a list
 * handed in: the loop's inputs are the chain a term actually wrote, and the
 * defects this file exists for -- forgetting a kept delta, sweeping an hour
 * whose commits sit below a checkpoint -- are properties of that chain, not of
 * a fixture's.
 */
class RetentionLoopTest {

    private static final RunKey STREAM = new RunKey(new UUID(0x5151_5151L, 1), 0);
    private static final String PREFIX = "bucket";
    private static final Duration MIN = Duration.ofHours(6);
    private static final Duration MAX = Duration.ofHours(24);
    private static final Duration GRACE = OrphanSweep.DEFAULT_GRACE;

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void set(Instant at) {
            now = at;
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    /** A lease that is always free, or always gone the moment it is asked. */
    private static final class TestLease implements GcLease {
        final AtomicInteger acquires = new AtomicInteger();
        final boolean holdsThroughThePass;

        TestLease(boolean holdsThroughThePass) {
            this.holdsThroughThePass = holdsThroughThePass;
        }

        @Override
        public boolean acquire() {
            acquires.incrementAndGet();
            return true;
        }

        @Override
        public boolean stillHeld() {
            return holdsThroughThePass;
        }

        @Override
        public void release() {
        }
    }

    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-17T10:30:00Z"));
    private final List<Map<RunKey, Long>> reported = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        backing.close();
    }

    /** Every consumer has read everything: nothing a watermark protects. */
    private RetentionRule readToTheEnd() {
        return new RetentionRule(clock, MIN, MAX, 0, (key, age) -> { },
                stream -> new WatermarkTable.Watermark(true, true, Long.MAX_VALUE / 2));
    }

    /** No consumer has read anything: everything a watermark protects. */
    private RetentionRule readNothing() {
        return new RetentionRule(clock, MIN, MAX, 0, (key, age) -> { },
                stream -> new WatermarkTable.Watermark(true, true, 0));
    }

    private RetentionLoop loop(AtomicReference<CommitLog> term, RetentionRule rule,
            GcLease lease) {
        return loop(term, rule, lease, () -> true);
    }

    private RetentionLoop loop(AtomicReference<CommitLog> term, RetentionRule rule,
            GcLease lease, java.util.function.BooleanSupplier live) {
        RetentionObservable observable = new RetentionObservable(clock, MIN, MAX,
                Duration.ofMinutes(1), alarm -> { });
        return new RetentionLoop(
                () -> Optional.ofNullable(term.get())
                        .map(log -> new RetentionLoop.Term(log.chain(), reported::add, live)),
                new LeasedGc(lease, store), rule, observable, clock, PREFIX, MIN, GRACE,
                SegmentGc.DEFAULT_DELETE_BATCH);
    }

    private String segment(Instant writtenAt, long sequence) {
        return new SegmentKey(PREFIX, writtenAt.toEpochMilli(), "poda", sequence, 12).key();
    }

    private String committed(CommitLog log, Instant writtenAt, long sequence) throws Exception {
        String key = segment(writtenAt, sequence);
        backing.put(key, Body.ofBytes(new byte[] {1}));
        log.commit(key, Map.of(STREAM, 5));
        return key;
    }

    private String orphan(Instant writtenAt, long sequence) throws Exception {
        String key = segment(writtenAt, sequence);
        backing.put(key, Body.ofBytes(new byte[] {1}));
        return key;
    }

    @Test
    void anIDLETickCostsNOTHINGNotEvenTheLEASE() throws Exception {
        // ⚠️ M8's CRITERION 4 IN MINIATURE. Taking the GC lease is a
        // conditional write and a read; a loop that took it every tick would
        // cost a request per interval for ever on an idle cluster, and every
        // "zero LIST, zero GET" assertion about the pass itself would still be
        // green, because the lease is not the pass.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        committed(log, clock.instant().minus(Duration.ofMinutes(1)), 1);
        TestLease lease = new TestLease(true);
        RetentionLoop loop = loop(new AtomicReference<>(log), readToTheEnd(), lease);

        StoreCounts before = store.counts();
        for (int i = 0; i < 10; i++) {
            loop.tick();
        }
        StoreCounts after = store.counts();

        assertThat(after.total() - before.total())
                .as("⚠️ ZERO REQUESTS OVER TEN TICKS with a young segment in the chain")
                .isZero();
        assertThat(lease.acquires)
                .as("⚠️ AND THE LEASE WAS NEVER ASKED FOR. Nothing is past the floor, so "
                        + "nothing can be due, and that is decided in memory")
                .hasValue(0);
    }

    @Test
    void aDUEPassDELETESReportsTheBOUNDARYAndForgetsWhatItCOLLECTED() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        String first = committed(log, clock.instant().minus(Duration.ofHours(7)), 1);
        String second = committed(log, clock.instant().minus(Duration.ofHours(7)), 2);
        RetentionLoop loop = loop(new AtomicReference<>(log), readToTheEnd(),
                new TestLease(true));

        loop.tick();

        assertThat(backing.stat(first)).as("expired and read: collected").isEmpty();
        assertThat(backing.stat(second)).isEmpty();
        assertThat(reported)
                .as("⚠️ THE BOUNDARY REACHED THE TERM's SINK, which is what a consumer's "
                        + "refusal is read from (M7.10)")
                .isNotEmpty();
        assertThat(reported.get(reported.size() - 1)).containsEntry(STREAM, 10L);
        assertThat(log.chain().snapshot().deltas())
                .as("⚠️ AND WHAT WAS DELETED IS FORGOTTEN, which is what keeps a healthy "
                        + "chain small rather than the cap")
                .isEmpty();
    }

    @Test
    void aLEASELostBeforeTheBATCHSendsNODelete() throws Exception {
        // ⚠️ THE DATA-LOSS PATH M7.10 MEASURED. A pass that deletes through
        // any store but the FENCED one keeps deleting after the lease has
        // moved -- two pods deleting from two views of the watermark table.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        String doomed = committed(log, clock.instant().minus(Duration.ofHours(7)), 1);
        RetentionLoop loop = loop(new AtomicReference<>(log), readToTheEnd(),
                new TestLease(false));

        StoreCounts before = store.counts();
        loop.tick();

        assertThat(store.counts().deletes() - before.deletes())
                .as("⚠️ NOT ONE DELETE REACHED THE STORE after the role was lost")
                .isZero();
        assertThat(backing.stat(doomed)).as("and the segment is still there").isPresent();
        assertThat(log.chain().snapshot().deltas())
                .as("⚠️ AND NOTHING WAS FORGOTTEN: a delta whose segment survived must be "
                        + "judged again by whoever holds the role next")
                .hasSize(1);
    }

    @Test
    void aDELTAWhoseSegmentWasKEPTIsNeverForgottenEvenIfALaterOneWasCollected()
            throws Exception {
        // ⚠️ THE TEMPTING CALL IS "FORGET THROUGH THE SNAPSHOT's LAST POSITION".
        // It forgets the kept delta too, and then two things go wrong: the
        // kept segment is never judged again (a leak), and it drops out of the
        // orphan sweep's keep list -- which FAILS OPEN, so a committed segment
        // is listed, found in no delta, and deleted.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        String kept = committed(log, clock.instant().minus(Duration.ofHours(2)), 1);
        committed(log, clock.instant().minus(Duration.ofHours(7)), 2);
        RetentionLoop loop = loop(new AtomicReference<>(log), readToTheEnd(),
                new TestLease(true));

        loop.tick();

        assertThat(backing.stat(kept)).as("inside the floor, so kept").isPresent();
        assertThat(log.chain().snapshot().deltas())
                .as("⚠️ BOTH DELTAS ARE STILL HELD: the kept one blocks the prefix, and the "
                        + "chain cannot hold a hole")
                .hasSize(2);
    }

    @Test
    void anUNREADSegmentIsKEPTAndItsDeltaIsNotForgotten() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        String unread = committed(log, clock.instant().minus(Duration.ofHours(7)), 1);
        RetentionLoop loop = loop(new AtomicReference<>(log), readNothing(),
                new TestLease(true));

        loop.tick();

        assertThat(backing.stat(unread))
                .as("past the floor but not read: the watermark protects it").isPresent();
        assertThat(log.chain().snapshot().deltas()).hasSize(1);
    }

    @Test
    void theORPHANSweepNEVEREntersAnHourThatStartsBeforeTheTERMPlusTheMARGIN()
            throws Exception {
        // ⚠️ A REPLAY STOPS AT A CHECKPOINT (M8.38), so after a takeover the
        // chain in memory lacks everything below the newest checkpoint. An
        // hour that holds segments committed BEFORE this term may hold ones no
        // delta here names -- and the sweep deletes those as orphans.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        RetentionLoop loop = loop(new AtomicReference<>(log), readNothing(),
                new TestLease(true));
        loop.tick(); // the term is first seen at 10:30, so the sweep may start at 12:00

        String tooEarly = orphan(Instant.parse("2026-09-17T11:10:00Z"), 1);
        String sweepable = orphan(Instant.parse("2026-09-17T12:10:00Z"), 2);
        String committedLater = committed(log, Instant.parse("2026-09-17T12:20:00Z"), 3);

        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        loop.tick();

        assertThat(backing.stat(sweepable))
                .as("an uncommitted segment in an hour this term owns, past grace: an orphan")
                .isEmpty();
        assertThat(backing.stat(committedLater))
                .as("⚠️ A COMMITTED SEGMENT IN THE SAME HOUR IS KEPT: the chain names it")
                .isPresent();
        assertThat(backing.stat(tooEarly))
                .as("⚠️ THE HOUR BEFORE THE MARGIN IS NEVER ENTERED. Its segments could be "
                        + "named by a delta below a checkpoint, which this chain does not hold")
                .isPresent();
    }

    @Test
    void anHOURIsSweptONCEAndNeverListedAgain() throws Exception {
        // ⚠️ R15: EVERY SEGMENT IN AN HOUR A GRACE PAST ITS END IS PAST GRACE,
        // so one sweep finds every orphan that hour will ever hold. A second
        // sweep finds nothing and costs a LIST per 1,000 keys each time.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        RetentionLoop loop = loop(new AtomicReference<>(log), readNothing(),
                new TestLease(true));
        loop.tick();
        orphan(Instant.parse("2026-09-17T12:10:00Z"), 1);

        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        loop.tick();
        StoreCounts afterFirst = store.counts();
        clock.set(Instant.parse("2026-09-17T14:30:00Z"));
        loop.tick();

        assertThat(store.counts().lists() - afterFirst.lists())
                .as("⚠️ THE SAME HOUR IS NOT LISTED TWICE")
                .isZero();
    }

    @Test
    void anINCOMPLETEChainIsNeverUsedAsAKeepList() throws Exception {
        // ⚠️ A CHAIN AT ITS CAP HAS DROPPED ITS OLDEST DELTAS, and the segments
        // they named are exactly the ones a sweep would then find in no delta.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1, 1);
        RetentionLoop loop = loop(new AtomicReference<>(log), readNothing(),
                new TestLease(true));
        loop.tick();
        String dropped = committed(log, Instant.parse("2026-09-17T12:10:00Z"), 1);
        committed(log, Instant.parse("2026-09-17T12:20:00Z"), 2);
        assertThat(log.chain().snapshot().complete())
                .as("the premise: the cap of 1 dropped the first delta").isFalse();

        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        StoreCounts before = store.counts();
        loop.tick();

        assertThat(backing.stat(dropped))
                .as("⚠️ COMMITTED, AND KEPT: the chain that forgot it is not asked")
                .isPresent();
        assertThat(store.counts().lists() - before.lists())
                .as("and no hour was even listed").isZero();
    }

    @Test
    void aNEWTermRESTARTSTheMarginFromWhenItWasFirstSeen() throws Exception {
        // ⚠️ THE MARGIN IS PER TERM. A chain this node held earlier says
        // nothing about hours committed while another node held the lease, and
        // the new term's chain is a new object for exactly that reason.
        AtomicReference<CommitLog> term = new AtomicReference<>(
                new CommitLog(store, PREFIX + "/ctl/log", 1));
        RetentionLoop loop = loop(term, readNothing(), new TestLease(true));
        loop.tick(); // first term seen at 10:30

        clock.set(Instant.parse("2026-09-17T14:00:00Z"));
        term.set(new CommitLog(store, PREFIX + "/ctl/log", 2));
        loop.tick(); // second term seen at 14:00, so its sweep starts at 15:00
        String fromTheOldTermsHours = orphan(Instant.parse("2026-09-17T13:10:00Z"), 1);

        clock.set(Instant.parse("2026-09-17T15:00:01Z"));
        loop.tick();

        assertThat(backing.stat(fromTheOldTermsHours))
                .as("⚠️ HOUR 13 BELONGED TO THE OLD TERM's WINDOW, and the new term has "
                        + "not seen its commits")
                .isPresent();
    }

    @Test
    void aNODEThatHoldsNOTermDoesNothingAtAll() throws Exception {
        TestLease lease = new TestLease(true);
        RetentionLoop loop = loop(new AtomicReference<>(null), readToTheEnd(), lease);

        StoreCounts before = store.counts();
        loop.tick();

        assertThat(store.counts().total() - before.total()).isZero();
        assertThat(lease.acquires)
                .as("⚠️ A FOLLOWER HAS NO CHAIN AND RUNS NO PASS: five pods in six hold "
                        + "nothing, every interval, for ever")
                .hasValue(0);
    }

    @Test
    void aDEPOSEDTermIsNeverUsedEvenThoughThisNodeStillHOLDSIt() throws Exception {
        // ⚠️ FOUND BY M8.5's REVIEW, AND IT DELETES COMMITTED DATA. A term
        // fenced by a takeover is still returned as held until a commit
        // through it throws, so a node that receives no writes keeps it. Its
        // chain is frozen at the takeover; the successor keeps committing; and
        // an hour later this node's sweep lists the successor's hours, finds
        // its segments in no delta here, and deletes them -- with the GC
        // lease genuinely held, so the fence lets every batch through.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        java.util.concurrent.atomic.AtomicBoolean live =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        TestLease lease = new TestLease(true);
        RetentionLoop loop = loop(new AtomicReference<>(log), readToTheEnd(), lease, live::get);
        loop.tick(); // the term is first seen, live, at 10:30

        live.set(false); // deposed; this node still holds the object
        String successorsCommit = orphan(Instant.parse("2026-09-17T12:10:00Z"), 1);
        String expired = committed(log, clock.instant().minus(Duration.ofHours(7)), 2);
        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        StoreCounts before = store.counts();
        loop.tick();

        assertThat(backing.stat(successorsCommit))
                .as("⚠️ A SEGMENT THE SUCCESSOR COMMITTED -- in no delta of this dead chain -- "
                        + "IS NOT TAKEN FOR AN ORPHAN")
                .isPresent();
        assertThat(backing.stat(expired))
                .as("and a dead term runs no pass at all").isPresent();
        assertThat(store.counts().total() - before.total())
                .as("⚠️ NOT ONE REQUEST: no lease, no LIST, no DELETE").isZero();
        assertThat(loop.termTicks())
                .as("the dead term's tick is not counted as one that read a chain")
                .isEqualTo(1);
    }

    @Test
    void aTermDeposedMIDTickSweepsNoFurtherHOUR() throws Exception {
        // ⚠️ ASKED AGAIN BEFORE EVERY HOUR, not once per tick: two hours can
        // come due in one tick, and a takeover between them leaves the second
        // swept against a keep list that is already a dead term's.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        AtomicInteger asked = new AtomicInteger();
        // live for the source's own check and the first hour, dead after
        RetentionLoop loop = loop(new AtomicReference<>(log), readNothing(),
                new TestLease(true), () -> asked.incrementAndGet() <= 3);
        loop.tick(); // ask 1: first seen at 10:30, nothing due
        String inTwelve = orphan(Instant.parse("2026-09-17T12:10:00Z"), 1);
        String inThirteen = orphan(Instant.parse("2026-09-17T13:10:00Z"), 2);

        clock.set(Instant.parse("2026-09-17T15:00:01Z"));
        loop.tick(); // ask 2: the source; ask 3: hour 12; ask 4: hour 13 -- dead

        assertThat(backing.stat(inTwelve)).as("hour 12 was swept while the term lived")
                .isEmpty();
        assertThat(backing.stat(inThirteen))
                .as("⚠️ HOUR 13 WAS NOT: the term died between the two")
                .isPresent();
    }

    @Test
    void anHOURIsNotSweptUntilAGRACEAfterItENDS() throws Exception {
        // ⚠️ EACH HOUR IS SWEPT ONCE, so sweeping it early is sweeping it
        // never. An orphan written at 12:50 is inside its grace at 13:30; a
        // sweep then keeps it, and a loop that had already moved past hour 12
        // would leave it in the bucket for ever.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        RetentionLoop loop = loop(new AtomicReference<>(log), readNothing(),
                new TestLease(true));
        loop.tick();
        String late = orphan(Instant.parse("2026-09-17T12:50:00Z"), 1);

        clock.set(Instant.parse("2026-09-17T13:30:00Z"));
        loop.tick();
        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        loop.tick();

        assertThat(backing.stat(late))
                .as("⚠️ SWEPT AFTER HOUR 12 WAS A WHOLE GRACE PAST ITS END, not at its end")
                .isEmpty();
    }

    @Test
    void aTermDeposedBeforeThePASSDeletesNothing() throws Exception {
        // ⚠️ THE LIVE CHECK BEFORE THE PASS, which review measured unpinned:
        // the mid-tick case above uses a rule that keeps everything, so no
        // pass is ever due there and the check before it could be deleted with
        // the suite green. Here the pass IS due, and the term dies between
        // being obtained and the pass.
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 1);
        String expired = committed(log, clock.instant().minus(Duration.ofHours(7)), 1);
        AtomicInteger asked = new AtomicInteger();
        RetentionLoop loop = loop(new AtomicReference<>(log), readToTheEnd(),
                new TestLease(true), () -> asked.incrementAndGet() <= 1);

        StoreCounts before = store.counts();
        loop.tick(); // ask 1: the source, live; ask 2: before the pass, dead

        assertThat(backing.stat(expired))
                .as("⚠️ EXPIRED AND READ, AND STILL KEPT: the term that would delete it is "
                        + "no longer the chain's writer")
                .isPresent();
        assertThat(store.counts().deletes() - before.deletes()).isZero();
    }
}
