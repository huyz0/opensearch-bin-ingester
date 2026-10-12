// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.OpType;
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
import io.github.huyz0.os.biningester.sequencer.PodGone;
import io.github.huyz0.os.biningester.sequencer.RosterJoins;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The server's start of a term's takeover, over a real term (M13.33r review
 * rounds 1-2, T1, T4): term 2 over an unclosed term 1 whose member {@code m}
 * holds one entry of S at the chain's next offset -- decided, written as a
 * recovery entry, recorded on roster 2, resumed, RELEASE sent to every
 * member, and term 1 closed; once per term, and not before {@code notBefore}.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastTakeoverStartTest {

    private static final UUID INDEX = new UUID(1, 1);
    private static final RunKey S = new RunKey(INDEX, 0);
    /** S's next offset before the takeover: default-path commits took 0-4. */
    private static final long C0 = 5;
    private static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);
    private static final MonotonicClock MONO = () -> 0L;
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");
    private static final Roster.Incarnation X =
            new Roster.Incarnation("x", "uid-x", "az-x", "x:1");
    private static final Roster.Incarnation M =
            new Roster.Incarnation("m", "uid-m", "az-m", "m:1");

    /** Term 2, led by this pod, over term 1, which recorded INDEX and so stays open. */
    private record Rig(MemoryBinStore store, LocalSequencer term, FastDisk disk,
            AtomicInteger uploads, List<String> released) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            disk.close();
            term.close();
        }
    }

    /** @param notBefore term 1's {@code notBefore}, which term 2 inherits */
    private static Rig rig(long notBefore) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseConfig leases = new LeaseConfig("p", "self", "", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
        LocalSequencer.start(store, "p", new LeaseManager(store, leases, WALL), 8)
                .orElseThrow().close();
        LocalSequencer term = LocalSequencer.start(store, "p",
                new LeaseManager(store, leases, WALL), 8).orElseThrow();
        assertThat(term.epoch()).isEqualTo(2);
        term.commitAll(List.of(new CommitRequest("self", "i1", 0, "seg/0",
                Map.of(S, (int) C0))));
        Roster one = new Roster(1, -1, X, List.of(new Roster.Member(X, Roster.State.ROSTERED),
                new Roster.Member(M, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(INDEX, 1)))), List.of(),
                notBefore, 0, false);
        store.put(Roster.key("p", 1), Body.ofBytes(one.encode()));
        store.put(Roster.latestKey("p"), Body.ofBytes(Roster.encodeLatest(1)));
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), WALL, MONO), SELF, Map::of)
                .open(2, term::close).orElseThrow();
        assertThat(opened.stillOpen()).hasSize(1);
        term.attach(new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 2, MONO, Duration.ofMillis(250)),
                        nanos -> { }),
                term::committedNext, store, "p", MONO, Duration.ofMillis(250)));
        FastDisk disk = FastDisk.open(store, leases.leaseKey(),
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files());
        return new Rig(store, term, disk, new AtomicInteger(), new CopyOnWriteArrayList<>());
    }

    /**
     * Every member answers FENCE, {@code m} with one entry of S at C0; a
     * RELEASE is recorded by its target and left unanswered.
     */
    private static TermJoiner.Transport members(Rig rig) {
        return (endpoint, frame) -> {
            FastFrame.Frame asked = FastFrame.decode(frame);
            String member = asked.header().targetUid();
            if (asked.body() instanceof FastTakeoverFrame.Release) {
                rig.released().add(member);
                throw new IOException("RELEASE left unanswered");
            }
            if (!(asked.body() instanceof FastTakeoverFrame.Fence fence)) {
                throw new IOException("only a FENCE is answered here");
            }
            List<FastJournalRecord.Entry> held = member.equals("uid-m")
                    ? List.of(new FastJournalRecord.Entry(1, S, C0, 1, 0,
                            new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 0),
                            List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                    new byte[] {1}))))
                    : List.of();
            return FastFrame.encode(2, member, "uid-self",
                    new FastTakeoverFrame.Collected(fence.page(), true, held));
        };
    }

    private static boolean start(Rig rig, Executor launch) {
        return FastTakeover.start(rig.term(), rig.disk(), SELF, members(rig), PodGone.NEVER,
                runs -> {
                    rig.uploads().incrementAndGet();
                    return "seg-r";
                }, rig.store(), "p", WALL, launch);
    }

    private static Roster roster(MemoryBinStore store, long epoch) throws Exception {
        try (InputStream in = store.get(Roster.key("p", epoch))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    @Test
    void aTAKEOVERRunsToItsEndOverTheTermItLeadsAndOnlyOnce() throws Exception {
        try (Rig rig = rig(0)) {
            assertThat(start(rig, Runnable::run)).isTrue();

            assertThat(start(rig, Runnable::run)).as("one takeover per term").isFalse();
            assertThat(rig.uploads()).as("m's entry, uploaded").hasValue(1);
            assertThat(rig.term().committedNext(S)).as("committed at the chain's next offset")
                    .isEqualTo(C0 + 1);
            assertThat(roster(rig.store(), 2).decisions())
                    .anySatisfy(d -> {
                        assertThat(d.stream()).isEqualTo(S);
                        assertThat(d.resumeAt()).isEqualTo(C0 + 1);
                    });
            assertThat(rig.released()).as("one RELEASE to each member")
                    .containsExactlyInAnyOrder("uid-x", "uid-m");
            assertThat(roster(rig.store(), 1).closed()).as("term 1 decided, then closed").isTrue();
            assertThat(rig.term().serving()).as("the term kept").isTrue();
        }
    }

    @Test
    void aTAKEOVERDecidesNothingBeforeItsNotBefore() throws Exception {
        try (Rig rig = rig(WALL.millis() + Duration.ofHours(1).toMillis())) {
            AtomicReference<Runnable> captured = new AtomicReference<>();
            assertThat(start(rig, captured::set)).isTrue();
            Thread running = Thread.ofVirtual().start(captured.get());

            running.join(Duration.ofSeconds(1));

            assertThat(running.isAlive()).as("still waiting for notBefore").isTrue();
            assertThat(rig.uploads()).as("nothing decided, nothing uploaded").hasValue(0);
            running.interrupt();
            running.join();
            assertThat(rig.term().serving()).as("interrupted: given back").isFalse();
        }
    }

    @Test
    void aLAUNCHThatFailsGivesTheTermBack() throws Exception {
        try (Rig rig = rig(0)) {
            assertThatThrownBy(() -> start(rig, run -> {
                throw new RejectedExecutionException("no thread to run it on");
            })).isInstanceOf(RejectedExecutionException.class);

            assertThat(rig.term().serving()).as("given back, not held taking over").isFalse();
        }
    }

    @Test
    void aLAUNCHThatFailsWithAnErrorGivesTheTermBack() throws Exception {
        try (Rig rig = rig(0)) {
            assertThatThrownBy(() -> start(rig, run -> {
                throw new StackOverflowError("no stack to run it on");
            })).isInstanceOf(StackOverflowError.class);

            assertThat(rig.term().serving()).isFalse();
        }
    }
}
