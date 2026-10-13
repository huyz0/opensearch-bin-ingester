// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastKey;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.CommitDesk;
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
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * No deposed leader exposes (M13.32e; SPEC criterion 10, ADR-0081 §3): a
 * leader holding its lease assigns entries -- one its own writer's (W = L),
 * one another pod's -- and then PAUSES, its monotonic clock passing the
 * lease's validity less the margin, as a stalled process's would while a
 * successor takes the lease. The pause falls after assignment and before each
 * exposing send's check: the EXPOSED that acks either writer, a PUBLISH to an
 * interested pod and the push to its own subscribers, a catch-up's journal
 * events, and the {@code /ctl/fast-entry} answer a proxied {@code /seg} read
 * forwards to. Each exposes nothing; and once the lease is renewed the same
 * calls expose, so the refusals are the lease's, not the rig's.
 *
 * <p>⚠️ Not here: the pause between a check and its send toward a fenced
 * member -- the writer's, the interested pod's and the forwarding pod's own
 * epoch guards, each pinned at its site (M13.27, M13.30, M13.32i).
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastDeposedLeaderTest {

    private static final UUID INDEX = new UUID(1, 1);
    private static final RunKey S = new RunKey(INDEX, 0);
    private static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");
    private static final Roster.Incarnation B =
            new Roster.Incarnation("b", "uid-b", "az-b", "b:1");

    private final long[] nanos = {0};
    private final MonotonicClock mono = () -> nanos[0];
    private LocalSequencer term;
    private FastLeaderTerm fast;
    private FastDisk disk;
    private CommitDesk desk;
    private FastLeaseFence fence;

    @AfterEach
    void close() throws Exception {
        if (disk != null) {
            disk.close();
        }
        if (term != null) {
            term.close();
        }
    }

    /** A serving fast term led by SELF, holding its lease, with B joined to it. */
    private void lead() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseConfig leases = new LeaseConfig("p", "self", "", TTL, Duration.ofSeconds(3));
        term = LocalSequencer.start(store, "p", new LeaseManager(store, leases, WALL), 8)
                .orElseThrow();
        disk = FastDisk.open(store, leases.leaseKey(),
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files());
        fence = new FastLeaseFence(TTL, WALL, mono);
        renew();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close, fence, SELF, Map::of)
                .open(term.epoch(), term::close).orElseThrow();
        fast = new FastLeaderTerm(opened, new JoinDesk(new RosterJoins(store, "p",
                term.epoch(), mono, Duration.ofMillis(250)), n -> { }),
                term::committedNext, store, "p", mono, Duration.ofMillis(250));
        term.attach(fast);
        fast.catalogSays(Map.of(INDEX, 1));
        fast.flushTermRecord();
        assertThat(fast.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, term.epoch(),
                B.podUid(), SELF.podUid()), new FastFrame.Join(B, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);
        desk = fast.commitDesk(disk.journal().orElseThrow(), SELF);
    }

    /** The lease won, or renewed, at the monotonic clock's reading now. */
    private void renew() {
        long sent = fence.acquiring(null);
        fence.won(new Lease(1, "self", "uid-self", "self:1", WALL.millis() + TTL.toMillis()),
                sent);
    }

    /** The pause: past the lease's validity less its margin, on the monotonic clock. */
    private void pause() {
        nanos[0] += Duration.ofSeconds(9).toNanos();
    }

    /** One record of S assigned at the writer {@code writer}'s commit, at q = 1. */
    private void assign(Roster.Incarnation writer, long seq) throws IOException {
        fast.answerCommit(new FastFrame.Header(FastWriteFrame.KIND_COMMIT, term.epoch(),
                writer.podUid(), SELF.podUid()),
                new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey(writer.podId(),
                        new UUID(9, 9), seq), List.of(new FastWriteFrame.CommitRun(S,
                        List.of(new SegmentRecord(writer.podId() + seq, OpType.INDEX,
                                OptionalLong.empty(), new byte[] {1}))))), desk);
    }

    /** Every exposing send, each counted by what it exposed. */
    private List<String> exposeEverything() throws IOException {
        List<String> exposed = new ArrayList<>();
        fast.sendExposed(desk, (endpoint, frame) -> {
            exposed.add("EXPOSED to " + endpoint);
            return FastFrame.encode(term.epoch(), B.podUid(), SELF.podUid(),
                    new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        });
        FastLeaderFrames.publish(term, disk, SELF, (endpoint, frame) -> {
            exposed.add("PUBLISH to " + endpoint);
            return new byte[0];
        }, entry -> exposed.add("own push @" + entry.firstOffset()));
        FastCatchUp.respond(new CatchUpRequestFrame(new UUID(4, 4),
                List.of(new CatchUpRequestFrame.Stream(S, 0))), frame -> {
                    try {
                        exposed.add("catch-up @" + CatchUpEventFrame.decode(frame).event()
                                .firstOffset());
                    } catch (IOException theEndFrame) {
                        // not an event
                    }
                }, (request, sink) -> sink.write(new io.github.huyz0.os.biningester.format
                        .CatchUpEndFrame(request.requestId()).encode()),
                (session, key, from) -> FastLeaderFrames.exposedFrom(term, disk, SELF, "p",
                        session, key, from, WALL.millis()),
                () -> FastLeaderFrames.mayExpose(term));
        FastJournalRecord.Entry first = disk.journal().orElseThrow().held().get(0);
        FastLeaderFrames.exposedEntry(term, disk, SELF, "p", FastKey.of("p", first.epoch(),
                first.assignedAfter(), S, first.firstOffset()), WALL.millis())
                .ifPresent(served -> exposed.add("fast-entry @" + first.firstOffset()));
        return exposed;
    }

    @Test
    void aLEADERPausedAfterAssignmentExposesNothingAtAnySendUntilRenewed() throws Exception {
        lead();
        desk.publications().interest(B, List.of(S), List.of());
        FastLeaderFrames.interestOwn(term, disk, SELF, java.util.Set.of(S));
        // ⚠️ EXPOSED WHILE THE LEASE HELD: owed a PUBLISH, an own push,
        // a catch-up and a fast-entry read, none sent yet -- a pause before
        // sendExposed alone would leave them nothing to send whatever their checks.
        assign(SELF, 1);
        assign(B, 1);
        fast.sendExposed(desk, (endpoint, frame) -> FastFrame.encode(term.epoch(),
                B.podUid(), SELF.podUid(),
                new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE)));
        // and assigned after it, each writer's ack owed.
        assign(SELF, 2);
        assign(B, 2);

        pause();

        assertThat(fast.mayExpose()).as("the lease forbids").isFalse();
        assertThat(exposeEverything()).as("at no send").isEmpty();

        renew();

        assertThat(exposeEverything()).as("renewed: owed, not lost").contains(
                "EXPOSED to self:1", "EXPOSED to b:1", "PUBLISH to b:1", "own push @0",
                "catch-up @0", "catch-up @1", "fast-entry @0");
    }
}
