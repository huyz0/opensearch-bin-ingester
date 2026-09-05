// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * One delta per window, however many pods flushed into it (M4.7).
 *
 * <p>⚠️ ACCEPTANCE CRITERION 9 IS AN EQUALITY, not an approximation, which is
 * why the window closes on a SEAM rather than on a sleep: a test that slept
 * would assert "roughly one PUT" and would be the flaky kind testing.md forbids.
 * Here the window closes exactly when the requests under test have queued.
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class BatchingSequencerTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts(RunKey key, int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(key, n);
        return m;
    }

    private static CommitRequest from(String pod, long flushSeq) {
        return new CommitRequest(pod, flushSeq, "bins/" + pod + "/" + flushSeq + ".bseg",
                counts(new RunKey(A, 0), 3));
    }

    /**
     * A window that closes once {@code expected} requests are really in it.
     *
     * <p>⚠️ WAITS ON THE QUEUE, not on a latch the callers trip. {@code commit}
     * blocks, so a caller can only count down BEFORE it enqueues — and a window
     * closing on that countdown splits the batch across two windows. Measured
     * with the latch version: 2 PUTs where the criterion demands 1, and a
     * duplicate-key window that passed because the clash landed in the second.
     * The committer has already taken one request, so the window is full when
     * {@code expected - 1} remain queued behind it.
     *
     * <p>⚠️ THE {@code Thread.sleep(1)} HERE IS A BOUNDED POLL, not the fixed
     * sleep testing.md forbids, and the difference is what the test would do if
     * the system were slower: this waits for a CONDITION and fails loudly at 20
     * seconds, where a fixed sleep would proceed regardless and assert against
     * whatever had happened by then. It is inside the window timer, whose whole
     * contract is to block. No test in this class sleeps to synchronise.
     */
    private static final class QueuedTimer implements BatchingSequencer.WindowTimer {
        private final int expected;
        private volatile BatchingSequencer watching;

        QueuedTimer(int expected) {
            this.expected = expected;
        }

        void watch(BatchingSequencer sequencer) {
            this.watching = sequencer;
        }

        @Override
        public void awaitWindowClose() throws InterruptedException {
            long giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (watching == null || watching.queuedCount() < expected - 1) {
                if (System.nanoTime() > giveUpAt) {
                    throw new InterruptedException("the window never filled");
                }
                Thread.sleep(1);
            }
        }
    }

    private static CommitDelta commitConcurrently(Sequencer sequencer,
            int pods, List<Throwable> failures) throws Exception {
        var deltas = new CopyOnWriteArrayList<CommitDelta>();
        var threads = new java.util.ArrayList<Thread>();
        for (int pod = 0; pod < pods; pod++) {
            CommitRequest request = from("pod" + pod, 0);
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    deltas.add(sequencer.commit(request));
                } catch (Throwable t) {
                    failures.add(t);
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        return deltas.isEmpty() ? null : deltas.get(0);
    }

    @Test
    void sixPodsCommittingInOneWindowCostONEPutNotSix() throws Exception {
        // ⚠️ THE POD DIMENSION of acceptance criterion 9, through the SEAM this
        // time rather than at the log: without batching each of the six callers
        // is its own conditional PUT, so the commit rate — and the $52/month —
        // scales with the fleet.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "p", 1);
        QueuedTimer timer = new QueuedTimer(6);
        var failures = new CopyOnWriteArrayList<Throwable>();

        try (var batching = new BatchingSequencer(new DirectSequencer(log), timer)) {
            timer.watch(batching);
            commitConcurrently(batching, 6, failures);
        }

        assertThat(failures).as("every caller must succeed").isEmpty();
        assertThat(store.counts().puts())
                .as("six pods, one window, one conditional PUT")
                .isEqualTo(1L);
    }

    @Test
    void everyCallerInAWindowGetsTheDurableDeltaCarryingItsOwnSegment() throws Exception {
        // ⚠️ ATTRIBUTION, end to end: a caller must be able to find ITS offsets
        // in a delta that carries five other pods' flushes. The rule the
        // Sequencer contract settles is "match on the segment key you
        // submitted", and this is that rule exercised through the batcher.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        QueuedTimer timer = new QueuedTimer(3);
        var failures = new CopyOnWriteArrayList<Throwable>();
        var mine = new CopyOnWriteArrayList<long[]>();
        var threads = new java.util.ArrayList<Thread>();

        try (var batching = new BatchingSequencer(new DirectSequencer(log), timer)) {
            timer.watch(batching);
            for (int pod = 0; pod < 3; pod++) {
                // ⚠️ DISTINCT COUNTS PER POD (1, 2, 4). With all three
                // committing the same count, the expected bases were a multiset
                // invariant under EVERY permutation of segment-key to runs: a
                // mutation pairing each key with the NEXT segment's runs handed
                // every caller another pod's range and the suite stayed green.
                // Distinct counts make each caller's own pair checkable.
                int count = 1 << pod;
                CommitRequest request = new CommitRequest("pod" + pod, 0,
                        "bins/pod" + pod + "/0.bseg", counts(new RunKey(A, 0), count));
                threads.add(Thread.ofVirtual().start(() -> {
                    try {
                        CommitDelta delta = batching.commit(request);
                        var own = delta.segments().stream()
                                .filter(s -> s.segmentKey().equals(request.segmentKey()))
                                .findFirst().orElseThrow();
                        assertThat(own.runs().get(0).recordCount())
                                .as("each caller's segment carries ITS OWN run, not a neighbour's")
                                .isEqualTo(count);
                        mine.add(new long[] {own.runs().get(0).firstOffset(), count});
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                }));
            }
            for (Thread t : threads) {
                t.join();
            }
        }

        assertThat(failures).isEmpty();
        // ⚠️ THE ORDER OF ARRIVAL IS NOT DETERMINISTIC, so the exact bases are
        // not either — asserting a fixed triple made this fail on the interleave
        // rather than on the behaviour, twice. What IS invariant is the TILING:
        // three callers, three disjoint ranges, together covering 0..6 with no
        // gap and no overlap, each range as long as the count ITS caller
        // submitted. A gap or an overlap is I2; the permutation is not.
        assertThat(mine).hasSize(3);
        var ranges = mine.stream().sorted(java.util.Comparator.comparingLong(r -> r[0])).toList();
        long expectedStart = 0;
        for (long[] range : ranges) {
            assertThat(range[0])
                    .as("each range starts where the previous ended -- no gap, no overlap")
                    .isEqualTo(expectedStart);
            expectedStart += range[1];
        }
        assertThat(expectedStart).as("and together they cover every record committed")
                .isEqualTo(7);
    }

    @Test
    void anIdleSequencerOpensNOWindowAndIssuesNOStoreRequest() throws Exception {
        // ⚠️ NFR-2, and the reason the window opens with its FIRST REQUEST
        // rather than on a fixed tick: a timer firing regardless would write an
        // empty delta every interval forever, turning an idle cluster into a
        // billed one.
        // ⚠️ NO SLEEP, and no "wait and see nothing happened" — testing.md
        // forbids the first and the second proves nothing a dead batcher would
        // not also prove. Instead the timer COUNTS the windows it is asked to
        // close, and the test ends with a real commit: zero windows while idle,
        // exactly one once a request arrives. The second assertion is the
        // liveness half, without which "nothing happened" is also what a
        // committer that never started looks like.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "p", 1);
        var windows = new java.util.concurrent.atomic.AtomicInteger();

        try (var batching = new BatchingSequencer(new DirectSequencer(log),
                windows::incrementAndGet)) {
            assertThat(windows.get()).as("idle: no window has opened").isZero();
            assertThat(store.counts().total()).as("idle: no request of any kind").isZero();

            batching.commit(from("pod0", 0));

            assertThat(windows.get())
                    .as("one request, one window -- and the batcher was alive to open it")
                    .isEqualTo(1);
            assertThat(store.counts().puts()).isEqualTo(1L);
        }
    }

    @Test
    void theCommitterServesWindowAFTERWindowNotJustTheFirst() throws Exception {
        // ⚠️ MEASURED SURVIVING MUTATION: returning from the drain loop after
        // one batch left the whole suite green, and in production every caller
        // after the first window hangs rather than fails. Nothing drove a
        // second window.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "p", 1);
        var windows = new java.util.concurrent.atomic.AtomicInteger();

        try (var batching = new BatchingSequencer(new DirectSequencer(log),
                windows::incrementAndGet)) {
            batching.commit(from("pod0", 0));
            batching.commit(from("pod0", 1));

            assertThat(windows.get()).as("two commits, two windows").isEqualTo(2);
            assertThat(store.counts().puts()).isEqualTo(2L);
        }
    }

    @Test
    void anExplicitBatchGoesThroughTheWindowRatherThanPastIt() throws Exception {
        // ⚠️ `commitAll` IS THE INTERFACE PRIMITIVE, and a pass-through made it
        // the one method that does not batch — one PUT per submission, a rate
        // scaling with pods. Worse, it ran the delegate on the CALLER's thread
        // concurrently with the committer's, and `CommitLog` is single-writer by
        // construction: two deltas built from one offset base, which is I2 and
        // which no conditional write catches.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "p", 1);
        var windows = new java.util.concurrent.atomic.AtomicInteger();

        try (var batching = new BatchingSequencer(new DirectSequencer(log),
                windows::incrementAndGet)) {
            CommitDelta delta = batching.commitAll(List.of(from("pod0", 0), from("pod1", 0)));

            assertThat(delta.segments()).as("both submissions are in the entry").hasSize(2);
            assertThat(store.counts().puts()).as("and they cost ONE PUT").isEqualTo(1L);
            assertThat(windows.get()).as("it went through a window, not past it").isEqualTo(1);
        }
    }

    @Test
    void theRealClockTimerActuallyWaitsItsInterval() throws Exception {
        // ⚠️ THE SHIPPING TIMER RAN IN NO TEST: the only test touching the
        // Duration constructor asserted it throws, so `() -> Thread.sleep(millis)`
        // could have been a no-op and criterion 9 would still read green — a
        // window that closes instantly batches nothing.
        // ⚠️ A LOWER BOUND, which is not flaky: a sleep may overrun, never
        // undershoot.
        // ⚠️ BOUNDED ON BOTH SIDES, and the upper bound is what makes this test
        // about the CONSTRUCTOR rather than about `sleepFor`. `sleepFor` is
        // package-private and pinned directly by the sub-millisecond test, so
        // both of that test's bounds BYPASS the public constructor -- and the
        // constructor is what production uses. Measured surviving mutations
        // with a lower bound alone: `this(delegate, sleepFor(interval))` ->
        // `sleepFor(interval.multipliedBy(4))`, and installing a constant
        // `() -> Thread.sleep(200)` while still calling `sleepFor(interval)` for
        // its guard. The second turns a configured 5 s interval into a 200 ms
        // window -- 25x the PUT rate, which is precisely the NFR-1 dimension
        // this class exists to protect.
        // ⚠️ 300 ms RATHER THAN 120, to buy separation: a 4x mutation lands at
        // 1200 ms and a 200 ms constant lands below the floor, so both bounds
        // discriminate with ~400 ms of slack rather than tens of milliseconds.
        // A sleep may overrun, never undershoot, so the floor cannot flake; the
        // ceiling would need the close and join to take 400 ms.
        CommitLog log = new CommitLog(new MemoryBinStore(), "p", 1);
        long before = System.nanoTime();

        try (var batching = new BatchingSequencer(
                new DirectSequencer(log), java.time.Duration.ofMillis(300))) {
            batching.commit(from("pod0", 0));
        }
        long elapsed = System.nanoTime() - before;

        assertThat(elapsed)
                .as("the window really stayed open for its interval")
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(300));
        assertThat(elapsed)
                .as("and the CONSTRUCTOR installed THAT interval, not a multiple or a constant")
                .isLessThan(TimeUnit.MILLISECONDS.toNanos(700));
    }

    @Test
    void aZeroIntervalIsRefusedBecauseItSilentlyRestoresThePerPodRate() {
        assertThatThrownBy(() -> new BatchingSequencer(
                new DirectSequencer(new CommitLog(new MemoryBinStore(), "p", 1)), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void aSubMillisecondIntervalIsStillAWINDOWRatherThanNoneAtAll() throws Exception {
        // ⚠️ THE GUARD AND THE TIMER MUST ASK THE SAME QUESTION. The guard asks
        // the `Duration`; the timer used to sleep `toMillis()`, which TRUNCATES
        // — so every interval in (0 ms, 1 ms) passed a check whose message reads
        // "must be positive" and then installed `Thread.sleep(0)`: the exact
        // no-window state `Duration.ZERO` is refused for, reached by a value the
        // guard accepts. Measured on the previous draft, six pods committing
        // 40/s against a 20 ms PUT: 250 ms cost 7 PUTs, 500 us cost 98.
        // ⚠️ THE TIMER IS TIMED, not a whole `commit`: a PUT and a thread join
        // around it would swamp a sub-millisecond window and the assertion would
        // hold either way.
        // ⚠️ THE MINIMUM OF MANY, NOT ONE MEASUREMENT, and the first draft of
        // this test was wrong for exactly that reason: a single timing PASSED
        // against the truncating code, because on a cold JVM the first
        // `Thread.sleep(0)` on a virtual thread costs more than 900 us all by
        // itself. Cold-start noise inflates individual runs, so it can only hide
        // the defect — it cannot manufacture a fast run that never happened.
        // Correct code sleeps at least the interval EVERY time, so the minimum
        // is bounded below; truncating code returns promptly at least once in a
        // hundred, so the minimum collapses. A sleep may overrun, never
        // undershoot, which is what makes the bound safe rather than flaky.
        long fastest = Long.MAX_VALUE;
        var timer = BatchingSequencer.sleepFor(Duration.ofNanos(900_000));
        for (int i = 0; i < 100; i++) {
            long before = System.nanoTime();
            timer.awaitWindowClose();
            fastest = Math.min(fastest, System.nanoTime() - before);
        }

        assertThat(fastest)
                .as("every sub-millisecond window is honoured, not truncated to zero")
                .isGreaterThanOrEqualTo(900_000L);
        // ⚠️ AN UPPER BOUND TOO, because a lower bound alone is met by a timer
        // that IGNORES its argument: `() -> Thread.sleep(200)` survived every
        // window test for four rounds. The margin is ~50x the expected value, so
        // it is scheduler noise that would have to be pathological, not close.
        // ⚠️ THIS PINS `sleepFor`, NOT THE CONSTRUCTOR, and an earlier draft of
        // this comment claimed otherwise -- that together with the real-clock
        // test above it showed "the configured interval is actually consulted".
        // It does not: `sleepFor` is called here DIRECTLY, so both bounds bypass
        // the public constructor, and a constructor that passed a multiple of
        // its argument would satisfy every assertion in this method. That gap is
        // closed by the upper bound on `theRealClockTimerActuallyWaitsItsInterval`,
        // which is the only test here that goes through the constructor.
        assertThat(fastest)
                .as("the interval is CONSULTED, not a constant that ignores it")
                .isLessThan(TimeUnit.MILLISECONDS.toNanos(50));
    }

    @Test
    void aNEGATIVEIntervalIsRefusedForTheSameReasonAsZero() {
        // ⚠️ CARRIED AS A SURVIVING MUTATION FOR FOUR ROUNDS: narrowing the
        // guard to `isZero()` alone left the suite green. A negative duration
        // reaches `Thread.sleep`, which throws IllegalArgumentException from
        // the COMMITTER thread on its first window -- so the batcher would be
        // constructed successfully and then die on the first commit, with the
        // caller told the batcher terminated rather than that its configuration
        // was wrong. Refusing it at construction is the fail-fast the zero case
        // already gets.
        assertThatThrownBy(() -> new BatchingSequencer(
                new DirectSequencer(new CommitLog(new MemoryBinStore(), "p", 1)),
                Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void aCommitWithNoSegmentsIsRefusedRatherThanWritingAnEmptyDelta() throws Exception {
        // ⚠️ THE ASSERTION IS THAT THE DELEGATE IS NEVER REACHED, not merely
        // that something throws. Deleting this guard still throws -- `CommitLog`
        // carries an identical one, with the identical message -- so a test
        // asserting only the exception passes against its own removal, which is
        // measured, not assumed. What the local guard buys is WHERE the refusal
        // happens: on the caller's thread, immediately, instead of occupying a
        // window and a committer round-trip to reach a verdict already known.
        var reached = new java.util.concurrent.atomic.AtomicBoolean();
        Sequencer recording = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                reached.set(true);
                throw new IllegalArgumentException("a commit with no segments commits nothing");
            }

            @Override
            public void close() {
            }
        };
        try (var batching = new BatchingSequencer(recording, () -> { })) {
            assertThatThrownBy(() -> batching.commitAll(List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("commits nothing");
        }

        assertThat(reached.get())
                .as("an empty commit is refused here, without occupying a window")
                .isFalse();
    }

    /** ⚠️ A `Sequencer` over a `CommitLog` with no lease, so the batcher is the subject. */
    private record DirectSequencer(CommitLog log) implements Sequencer {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            return log.commitAll(requests);
        }

        @Override
        public void close() {
        }
    }
}
