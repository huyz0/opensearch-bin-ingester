// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.EmptyTermCloser;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFence;
import io.github.huyz0.os.biningester.sequencer.FastTermOpening;
import io.github.huyz0.os.biningester.sequencer.FastTermStart;
import io.github.huyz0.os.biningester.sequencer.JoinDesk;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.RosterJoins;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * A takeover over a real chain (M13.33s): the leader of a new term over
 * earlier UNCLOSED terms, each led by {@code x} with members, whose
 * members answer FENCE with the entries a test gives them -- the shape
 * {@code FastTakeoverStartTest} began, made reusable for the death and
 * multi-term cases (SPEC test-plan row 12).
 */
final class TakeoverRig implements AutoCloseable {

    static final UUID INDEX = new UUID(1, 1);
    static final RunKey S = new RunKey(INDEX, 0);
    /** A second index, recorded only by {@link #TakeoverRig(int, long, Map, Map)}. */
    static final UUID INDEX2 = new UUID(2, 2);
    static final RunKey S2 = new RunKey(INDEX2, 0);
    static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L), ZoneOffset.UTC);
    static final MonotonicClock MONO = () -> 0L;
    static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");
    static final Roster.Incarnation X = new Roster.Incarnation("x", "uid-x", "az-x", "x:1");
    static final Roster.Incarnation M = new Roster.Incarnation("m", "uid-m", "az-m", "m:1");

    final MemoryBinStore store = new MemoryBinStore();
    final AtomicInteger uploads = new AtomicInteger();
    /** Each RELEASE's target, with S's committed next offset when it arrived. */
    final List<String> released = new CopyOnWriteArrayList<>();
    /** Each recovery handed on to be pushed live, as "segment:first+count@epoch" (M13.33w). */
    final List<String> recovered = new CopyOnWriteArrayList<>();
    /** Each void of a recovery committed, in order (M13.33u3). */
    final List<Recovery.VoidRange> voids = new CopyOnWriteArrayList<>();
    /** Each member a FENCE was sent to, silent or not, in order. */
    final List<String> fenced = new CopyOnWriteArrayList<>();
    final LocalSequencer term;
    final FastDisk disk;
    private final Map<String, List<FastJournalRecord.Entry>> held;

    /** One record of S at {@code offset}, assigned in term {@code epoch}. */
    static FastJournalRecord.Entry entry(long epoch, long offset) {
        return entry(S, 1, epoch, offset);
    }

    /** One record of {@code key} at {@code offset}, assigned at {@code q} in term {@code epoch}. */
    static FastJournalRecord.Entry entry(RunKey key, int q, long epoch, long offset) {
        return new FastJournalRecord.Entry(epoch, key, offset, q, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, epoch), offset),
                List.of(new SegmentRecord("d" + epoch + "-" + offset, OpType.INDEX,
                        OptionalLong.empty(), new byte[] {1})));
    }

    /**
     * @param unclosed the earlier terms, 1 to {@code unclosed}, each recording
     *     INDEX at {@code q = 1} and left open
     * @param chainNext S's committed next offset before the takeover
     * @param held what each member's FENCE answer holds
     */
    TakeoverRig(int unclosed, long chainNext, Map<String, List<FastJournalRecord.Entry>> held)
            throws Exception {
        this(unclosed, chainNext, held, Map.of(INDEX, 1));
    }

    /** The same, each earlier term recording {@code quorums} instead (M13.33u3). */
    TakeoverRig(int unclosed, long chainNext, Map<String, List<FastJournalRecord.Entry>> held,
            Map<UUID, Integer> quorums) throws Exception {
        this.held = held;
        LeaseConfig leases = new LeaseConfig("p", "self", "", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
        for (int i = 0; i < unclosed; i++) {
            LocalSequencer.start(store, "p", new LeaseManager(store, leases, WALL), 8)
                    .orElseThrow().close();
        }
        term = LocalSequencer.start(store, "p", new LeaseManager(store, leases, WALL), 8)
                .orElseThrow();
        long epoch = term.epoch();
        assertThat(epoch).as("the premise").isEqualTo(unclosed + 1L);
        if (chainNext > 0) {
            term.commitAll(List.of(new CommitRequest("self", "i1", 0, "seg/0",
                    Map.of(S, (int) chainNext))));
        }
        for (long e = 1; e <= unclosed; e++) {
            Roster r = new Roster(e, e == 1 ? -1 : e - 1, X,
                    List.of(new Roster.Member(X, Roster.State.ROSTERED),
                            new Roster.Member(M, Roster.State.ROSTERED)),
                    List.of(new Roster.TermRecord(0, new TreeMap<>(quorums))),
                    List.of(), 0, 0, false);
            store.put(Roster.key("p", e), Body.ofBytes(r.encode()));
        }
        store.put(Roster.latestKey("p"), Body.ofBytes(Roster.encodeLatest(unclosed)));
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), WALL, MONO), SELF, Map::of)
                .open(epoch, term::close).orElseThrow();
        assertThat(opened.stillOpen()).as("the premise").hasSize(unclosed);
        term.attach(new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", epoch, MONO, Duration.ofMillis(250)),
                        nanos -> { }),
                term::committedNext, store, "p", MONO, Duration.ofMillis(250)));
        disk = FastDisk.open(store, leases.leaseKey(),
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files());
    }

    /**
     * Each member answers FENCE with what it holds, but a {@code gone} one,
     * which answers nothing; a RELEASE is recorded and left unanswered.
     */
    private TermJoiner.Transport members(Predicate<String> gone) {
        return (endpoint, frame) -> {
            FastFrame.Frame asked = FastFrame.decode(frame);
            String member = asked.header().targetUid();
            if (asked.body() instanceof FastTakeoverFrame.Fence) {
                fenced.add(member);
            }
            if (gone.test(member)) {
                throw new IOException(member + " is gone");
            }
            if (asked.body() instanceof FastTakeoverFrame.Release) {
                released.add(member + "@" + term.committedNext(S));
                throw new IOException("RELEASE left unanswered");
            }
            if (!(asked.body() instanceof FastTakeoverFrame.Fence fence)) {
                throw new IOException("only a FENCE is answered here");
            }
            List<FastJournalRecord.Entry> entries = new ArrayList<>();
            for (FastJournalRecord.Entry e : held.getOrDefault(member, List.of())) {
                if (fence.unclosedEpochs().contains(e.epoch())) {
                    entries.add(e);
                }
            }
            return FastFrame.encode(term.epoch(), member, "uid-self",
                    new FastTakeoverFrame.Collected(fence.page(), true, entries));
        };
    }

    /** Runs the takeover on the caller's thread, {@code gone} the pods deleted and silent. */
    boolean takeOver(Predicate<String> gone) {
        return takeOver(gone, gone);
    }

    /**
     * The same, {@code silent} the pods that answer nothing and {@code deleted}
     * those whose UID is gone: silent and not deleted is out of the
     * EndpointSlice, partitioned, or not yet ready (M13.33u3).
     */
    boolean takeOver(Predicate<String> silent, Predicate<String> deleted) {
        return FastTakeover.start(term, disk, SELF, members(silent),
                incarnation -> deleted.test(incarnation.podUid()),
                runs -> {
                    uploads.incrementAndGet();
                    return "seg-r" + uploads.get();
                }, (recovery, epoch) -> {
                    voids.addAll(recovery.voids());
                    recovery.segments().forEach(s -> s.runs().forEach(
                            run -> recovered.add(s.segmentKey() + ":" + run.firstOffset() + "+"
                                    + run.recordCount() + "@" + epoch)));
                },
                index -> index.equals(INDEX) ? java.util.Set.of(S)
                        : index.equals(INDEX2) ? java.util.Set.of(S2) : java.util.Set.of(),
                store, "p", WALL, Runnable::run);
    }

    boolean takeOver() {
        return takeOver(uid -> false);
    }

    /** Runs the takeover with a real {@code upload}, its recoveries handed to {@code recovered}. */
    boolean takeOver(io.github.huyz0.os.biningester.sequencer.RecoveryWriter.SegmentUpload upload,
            FastTakeover.Recovered recovered) {
        return FastTakeover.start(term, disk, SELF, members(uid -> false), incarnation -> false,
                upload, recovered, store, "p", WALL, Runnable::run);
    }

    Roster roster(long epoch) throws Exception {
        try (InputStream in = store.get(Roster.key("p", epoch))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    @Override
    public void close() throws Exception {
        disk.close();
        term.close();
    }
}
