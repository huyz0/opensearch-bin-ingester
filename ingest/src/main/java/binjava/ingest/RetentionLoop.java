// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import binjava.format.SegmentKey;
import binjava.sequencer.ChainMemory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One tick of retention on a running node: the segment pass, the orphan sweep,
 * and the alarms (M8.5, FR-9, NFR-2, NFR-3).
 *
 * <p>⚠️ **EVERY PIECE THIS DRIVES EXISTED AND NOTHING SHIPPED CALLED IT.**
 * {@link RetentionPass}, {@link LeasedGc}, {@link OrphanSweep} and
 * {@link RetentionObservable} were each built and tested in M7, and each was
 * constructed only by its own test (M7.21n). This class is the caller, and the
 * reason it is one class rather than four call sites is that the dangerous
 * part is the ORDER and the ARGUMENTS, not any one piece.
 *
 * <p>⚠️ **THE KNOWN DATA-LOSS PATH IS THE FIRST THING THIS CLASS AVOIDS.** M7.10
 * measured that composing {@link LeasedGc} with a {@link RetentionPass} the
 * obvious way -- build the pass, then call it inside {@code runIfLeader} --
 * compiles, passes every test, and DELETES UNFENCED, because the pass holds
 * the raw store it was built with. Here the {@link SegmentGc} and the
 * {@link OrphanSweep} are constructed INSIDE the lambda, from the fenced store
 * it is handed, and from nothing else: this class holds no store at all, so
 * there is no unfenced one to reach for.
 *
 * <p>⚠️ **A TICK WITH NOTHING PAST THE FLOOR COSTS NOTHING** -- not a lease
 * request, not a LIST, not a GET (NFR-2, M8's criterion 4). Whether anything
 * could be due is decided in memory, from the chain and the clock, BEFORE the
 * lease is asked for; taking the GC lease is a conditional write and a read,
 * and taking it every tick on an idle pod is a request per interval for ever.
 *
 * <p>⚠️ **THE GATE IS COARSE, AND WHAT THAT COSTS IS STATED RATHER THAN
 * HIDDEN** (found by M8.5's review, which found an earlier version of this
 * paragraph claiming more than it does). It asks "is any segment old enough
 * that the floor no longer protects it", which is a SUPERSET of what the rule
 * deletes: a segment past the floor that the rule KEEPS -- unread, or with no
 * fresh watermark -- makes every tick take and release the lease and delete
 * nothing. That is about three requests per pass interval on the GC leader,
 * for as long as such a segment exists, which can be up to the retention
 * ceiling. It scales with nodes and with nothing else, so it is inside
 * cost.md; it is not zero, and a slow consumer is the ordinary way to reach
 * it. ⚠️ The exact gate -- running the rule itself before asking -- is not
 * available as written: {@code RetentionRule.verdictFor} raises the ceiling
 * ALARM as a side effect, so pre-judging would page for deletions a pass that
 * then failed to get the lease never made.
 *
 * <p>⚠️ **THE ORPHAN SWEEP'S KEEP LIST IS THIS TERM'S CHAIN, AND THAT IS ONLY
 * TRUE FOR SOME HOURS.** The sweep deletes whatever it lists that no delta
 * names, and it fails OPEN. A chain in memory holds this term's commits and
 * whatever the takeover replay inherited -- and a replay STOPS AT A CHECKPOINT
 * (M8.38), so after any takeover the deltas below the newest checkpoint are
 * not here at all. Swept naively, every segment they name would be listed,
 * found in no delta, and deleted as an orphan: committed, acknowledged data.
 * So the sweep is confined to hours that START at least {@link #SKEW_MARGIN}
 * after this loop first saw the current term. Every segment in such an hour
 * was written after the term began, so every commit naming it was made by THIS
 * term and recorded here; a segment written before the term began cannot be
 * dated into it unless a writer's clock ran more than the margin fast.
 * ⚠️ WHAT THIS COSTS: orphans in the hours before a term's start plus the
 * margin are never swept by that term. That is storage, not data, and it is
 * recorded as such rather than solved.
 *
 * <p>⚠️ **AND ONLY OVER A COMPLETE CHAIN.** A chain that hit its cap has
 * dropped its oldest deltas, and the segments they named are in exactly the
 * hours the sweep would otherwise be allowed into.
 *
 * <p>⚠️ **EACH HOUR IS SWEPT ONCE**, when its end is a grace behind the clock.
 * Every segment in it is then past grace, so one LIST pass finds every orphan
 * it will ever hold; sweeping it again finds nothing and costs a LIST per
 * 1,000 keys each time -- R15's ceiling spent on nothing.
 *
 * <p>⚠️ **NOT THREAD-SAFE, AND IT NEED NOT BE**: one scheduler thread calls
 * {@link #tick()}. The chain it reads is shared with the commit thread, and
 * {@link ChainMemory} is where that is handled.
 */
public final class RetentionLoop {

    /** How often a node looks for something to collect. */
    public static final Duration DEFAULT_PASS_INTERVAL = Duration.ofMinutes(1);

    /**
     * ⚠️ **HOW FAST A WRITER'S CLOCK MAY RUN AND THE ORPHAN SWEEP STAY
     * CORRECT.** A segment's age is read from its key, which the WRITER dates;
     * a writer an hour fast can date a segment written before this term into
     * an hour the sweep is allowed into, and if the delta naming it sits below
     * a checkpoint that segment is deleted as an orphan. One hour is far past
     * any skew a running cluster tolerates for its own leases, and it costs
     * only a later first sweep.
     */
    public static final Duration SKEW_MARGIN = Duration.ofHours(1);

    /** The held term, as retention needs it. */
    @FunctionalInterface
    public interface Source {
        /** The term this node holds now, or empty where it holds none. */
        Optional<Term> current();
    }

    /**
     * What a term offers retention.
     *
     * @param chain the commit chain this term wrote and inherited
     * @param sink where the retained boundary is recorded -- the term's own
     *     checkpoint writer, so a consumer's refusal is read from what GC
     *     actually deleted (M7.10)
     * @param live whether the term is STILL the chain's writer. ⚠️ **ASKED
     *     AGAIN BEFORE THE PASS AND BEFORE EVERY HOUR'S SWEEP**, not only when
     *     the term is obtained: a term deposed mid-tick has a chain frozen at
     *     the takeover, and the sweep would take the successor's committed
     *     segments for orphans. See {@code LocalSequencer.serving()}
     */
    public record Term(ChainMemory chain, RetentionPass.RetainedSink sink,
            java.util.function.BooleanSupplier live) {

        public Term {
            Objects.requireNonNull(chain, "chain");
            Objects.requireNonNull(sink, "sink");
            Objects.requireNonNull(live, "live");
        }
    }

    private final Source source;
    private final LeasedGc leased;
    private final RetentionRule rule;
    private final RetentionObservable observable;
    private final Clock clock;
    private final String prefix;
    private final Duration minRetention;
    private final Duration orphanGrace;
    private final int deleteBatch;

    /** The term this loop last saw, by identity: a new term is a new chain. */
    private ChainMemory seen;

    /** The first hour the sweep may enter for {@link #seen}, as epoch millis. */
    private long nextSweepHourMillis;

    /**
     * @param prefix the key prefix segments live under -- the same one the
     *     writer uses, or the sweep lists a directory nothing writes to
     * @param deleteBatch keys per DELETE; ⚠️ production passes
     *     {@link SegmentGc#DEFAULT_DELETE_BATCH}, and a case asserts it
     */
    public RetentionLoop(Source source, LeasedGc leased, RetentionRule rule,
            RetentionObservable observable, Clock clock, String prefix,
            Duration minRetention, Duration orphanGrace, int deleteBatch) {
        this.source = Objects.requireNonNull(source, "source");
        this.leased = Objects.requireNonNull(leased, "leased");
        this.rule = Objects.requireNonNull(rule, "rule");
        this.observable = Objects.requireNonNull(observable, "observable");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.minRetention = Objects.requireNonNull(minRetention, "minRetention");
        this.orphanGrace = Objects.requireNonNull(orphanGrace, "orphanGrace");
        if (deleteBatch <= 0) {
            throw new IllegalArgumentException("deleteBatch is never " + deleteBatch);
        }
        this.deleteBatch = deleteBatch;
    }

    /**
     * The batch size this loop deletes in.
     *
     * <p>⚠️ **PUBLIC FOR THE M7.26 ASSERTION**, which has to be made against
     * the loop the composition root actually built: a default constant that no
     * production call site is checked against is the state M7.26 was opened
     * for.
     */
    public int deleteBatch() {
        return deleteBatch;
    }

    private final java.util.concurrent.atomic.AtomicLong ticks =
            new java.util.concurrent.atomic.AtomicLong();

    private final java.util.concurrent.atomic.AtomicLong termTicks =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * How many ticks found a LIVE term and read its chain.
     *
     * <p>⚠️ **SEPARATE FROM {@link #ticks()} BECAUSE A ZERO-REQUEST CLAIM
     * NEEDS BOTH.** Review MEASURED that `AssembledGcCostIT` stayed green with
     * the composition root's term source replaced by {@code Optional.empty()}:
     * a loop that never reads a chain costs nothing, and "ten ticks, zero
     * requests" is then about a GC loop that never ran.
     */
    public long termTicks() {
        return termTicks.get();
    }

    /**
     * How many ticks have run.
     *
     * <p>⚠️ **SO A ZERO-REQUEST CLAIM CAN SAY WHAT IT WAS ZERO OVER.** "No
     * request in five seconds" is equally true of a loop whose timer never
     * fired, and a cost assertion over a loop that did not run is the vacuous
     * green this project keeps paying for.
     */
    public long ticks() {
        return ticks.get();
    }

    /**
     * Collects what is due, if anything is, under the GC lease.
     *
     * <p>⚠️ **NOTHING ESCAPES**, because this runs on a schedule and a throw
     * would end GC on this node for good. {@link LeasedGc} already contains the
     * pass; the alarms are swept outside it and cost no request.
     */
    public void tick() {
        ticks.incrementAndGet();
        // ⚠️ THE ALARMS FIRST AND UNCONDITIONALLY. They read only memory, and
        // a node that is not the GC leader is still the one whose operator
        // needs telling that a copy has gone quiet.
        try {
            observable.sweep();
        } catch (RuntimeException failed) {
            // Reported by the observable's own sink when it can be; a throw
            // here must not stop the pass below.
        }

        Optional<Term> held = source.current().filter(t -> t.live().getAsBoolean());
        if (held.isEmpty()) {
            // ⚠️ FORGOTTEN, so a term regained later starts its sweep clock
            // again: a chain this node held last week says nothing about hours
            // committed while someone else held the lease.
            seen = null;
            return;
        }
        Term term = held.get();
        termTicks.incrementAndGet();
        if (term.chain() != seen) {
            seen = term.chain();
            nextSweepHourMillis = hourCeiling(clock.millis() + SKEW_MARGIN.toMillis());
        }

        ChainMemory.Snapshot snapshot = term.chain().snapshot();
        boolean retentionDue = anyPastTheFloor(snapshot.deltas());
        List<Long> sweepHours = snapshot.complete() ? dueHours() : List.of();
        if (!retentionDue && sweepHours.isEmpty()) {
            // ⚠️ THE IDLE PATH: no lease request, no LIST, no GET -- when
            // nothing is past the floor. See the class javadoc for what the
            // coarse gate costs when something is past it and kept.
            return;
        }

        leased.runIfLeader(fenced -> {
            if (retentionDue && term.live().getAsBoolean()) {
                // ⚠️ BUILT HERE, FROM `fenced`, AND NOWHERE ELSE. See the class
                // javadoc: a pass built outside this lambda deletes unfenced.
                SegmentGc gc = new SegmentGc(fenced, rule, deleteBatch);
                SegmentGc.Result result = new RetentionPass(gc, term.sink())
                        .run(snapshot.deltas(), nextOffsetsOf(snapshot.deltas()));
                term.chain().forgetCollected(Set.copyOf(result.deletedKeys()));
            }
            if (!sweepHours.isEmpty()) {
                // ⚠️ THE KEEP LIST IS TAKEN FROM THE SAME SNAPSHOT THE HOURS WERE
                // CHOSEN AGAINST, not re-read: a delta the pass above just
                // forgot named segments it deleted, which a LIST no longer
                // returns, so a snapshot taken now would be no safer and would
                // cost a second copy of the chain.
                Set<String> committed = committedKeys(snapshot.deltas());
                OrphanSweep sweep = new OrphanSweep(fenced, clock, orphanGrace,
                        OrphanSweep.MAX_PAGE_SIZE, deleteBatch);
                for (long hour : sweepHours) {
                    if (!term.live().getAsBoolean()) {
                        // ⚠️ DEPOSED MID-TICK: the keep list is now a dead
                        // term's chain, and the next hour could be the
                        // successor's. Nothing more is swept by this term.
                        return;
                    }
                    sweep.sweep(SegmentKey.hourPrefix(prefix, hour), committed);
                    // ⚠️ ADVANCED PER HOUR, AFTER ITS SWEEP, so a lease lost
                    // half-way leaves the rest for the next tick. A LIST that
                    // failed inside the sweep is NOT retried: the sweep reports
                    // it only in a log, and the cost is orphans left in that
                    // hour -- storage, the safe direction.
                    nextSweepHourMillis = hour + HOUR_MILLIS;
                }
            }
        });
    }

    private static final long HOUR_MILLIS = Duration.ofHours(1).toMillis();

    private static long hourCeiling(long millis) {
        return Math.ceilDiv(millis, HOUR_MILLIS) * HOUR_MILLIS;
    }

    /** Hours this term may sweep that are now wholly a grace in the past. */
    private List<Long> dueHours() {
        long now = clock.millis();
        List<Long> hours = new ArrayList<>();
        for (long hour = nextSweepHourMillis;
                hour + HOUR_MILLIS + orphanGrace.toMillis() <= now; hour += HOUR_MILLIS) {
            hours.add(hour);
        }
        return hours;
    }

    /**
     * Whether any segment is old enough that the retention floor no longer
     * protects it.
     *
     * <p>⚠️ **A SUPERSET OF WHAT THE RULE DELETES, NEVER A SUBSET**, so an
     * undatable key counts as due: the pass then runs, and {@link SegmentGc}
     * keeps it and counts it unreadable, which is where that is reported.
     */
    private boolean anyPastTheFloor(List<CommitDelta> deltas) {
        Instant floor = clock.instant().minus(minRetention);
        for (CommitDelta delta : deltas) {
            for (SegmentCommit segment : delta.segments()) {
                try {
                    if (!Instant.ofEpochMilli(SegmentKey.timestampOf(segment.segmentKey()))
                            .isAfter(floor)) {
                        return true;
                    }
                } catch (IllegalArgumentException undatable) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Each stream's next offset, as far as this chain knows.
     *
     * <p>⚠️ **FROM THE CHAIN, NOT FROM THE COMMIT LOG.** {@code CommitLog}'s
     * offsets are a plain map written on the commit thread, and its own javadoc
     * records that a cross-thread read of it is not safe. The chain's last run
     * per stream gives the same number or a LOWER one -- a commit after the
     * snapshot is missing -- and lower is the safe direction: the retained
     * boundary is reported conservatively and a consumer is refused LESS,
     * never for records that are still in the bucket.
     */
    static Map<RunKey, Long> nextOffsetsOf(List<CommitDelta> deltas) {
        Map<RunKey, Long> next = new HashMap<>();
        for (CommitDelta delta : deltas) {
            for (SegmentCommit segment : delta.segments()) {
                for (RunCommit run : segment.runs()) {
                    next.merge(run.key(), run.firstOffset() + run.recordCount(), Math::max);
                }
            }
        }
        return next;
    }

    private static Set<String> committedKeys(List<CommitDelta> deltas) {
        Set<String> keys = new HashSet<>();
        for (CommitDelta delta : deltas) {
            for (SegmentCommit segment : delta.segments()) {
                keys.add(segment.segmentKey());
            }
        }
        return keys;
    }
}
