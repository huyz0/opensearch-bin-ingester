// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastPublishFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToLongFunction;

/**
 * A node's handler of the frames a leader answers -- JOIN (M13.27o), DEPART
 * and HELD (M13.27k), COMMIT (M13.27s), CONFIRM (M13.27v): the term this pod leads answers them, and a pod leading
 * no term refuses them {@code NOT_ROSTERED} -- the sender learned an old
 * leader, and asks the new one.
 */
final class FastLeaderFrames {

    private FastLeaderFrames() {
    }

    /**
     * A COMMIT (M13.27s), answered by the term this pod leads over its fast
     * journal: ⚠️ A POD WITHOUT ONE HOLDS NO FAST WRITE, so refuses
     * {@code NOT_FAST} -- the writer sends the batch the default way.
     */
    static FastFrame.Body answerCommit(Sequencer held, FastDisk disk, Roster.Incarnation self,
            FastFrame.Header header, FastWriteFrame.Commit commit) throws IOException {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no term");
        }
        if (disk.journal().isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                    "this pod holds no fast journal");
        }
        return term.get().answerCommit(header, commit,
                term.get().commitDesk(disk.journal().get(), self));
    }

    /**
     * The term this pod leads, if it holds a journal, tells each batch's
     * quorum wait to {@link FastMetrics} (M13.36).
     */
    static void observe(Sequencer held, FastDisk disk, Roster.Incarnation self,
            io.github.huyz0.os.biningester.sequencer.MonotonicClock mono) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isPresent() && disk.journal().isPresent()) {
            term.get().commitDesk(disk.journal().get(), self)
                    .observeQuorumWait(mono, FastMetrics::quorumWaited);
        }
    }

    /** Journaled with a leader's leave at a graceful stop (M13.35a). */
    static final String LEFT = "fast leave: ";

    /**
     * This pod's leave of the term it leads, if any, at a graceful stop
     * (M13.35a; ADR-0081 §9): before its lease is released, so its successor
     * finds it departed. A failed write leaves it undeparted, as a crash.
     *
     * @return how it ended; empty when this pod leads no term
     */
    static Optional<FastLeaderTerm.Left> leave(Sequencer held,
            io.github.huyz0.os.biningester.sequencer.FastUploader.Last last, FastDisk disk) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return Optional.empty();
        }
        try {
            // ⚠️ M13.35c: what it exposed, uploaded and committed before it leaves.
            // ⚠️ M13.35d: its journal says which earlier terms it holds entries of.
            return Optional.of(term.get().leave(last,
                    disk == null ? Optional.empty() : disk.journal()));
        } catch (IOException failed) {
            return Optional.of(FastLeaderTerm.Left.STAYED);
        }
    }

    /**
     * The catalog's fast indices recorded in the term this pod leads, if any
     * (M13.27l; ADR-0081 §4): wanted, then flushed at most once per
     * {@code min_upload_interval}; a failed write is retried next call.
     */
    static void recordCatalog(Sequencer held, java.util.Map<java.util.UUID, Integer> fast) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return;
        }
        term.get().catalogSays(fast);
        try {
            term.get().flushTermRecord();
        } catch (IOException failed) {
            // ⚠️ THE CHANGES STAY PENDING, their batches waiting; the next
            // tick writes them.
            System.getLogger(FastLeaderFrames.class.getName()).log(
                    System.Logger.Level.WARNING, "the term record was not written", failed);
        }
    }

    /** A CONFIRM (M13.27v), counted by the same desk its COMMIT was assigned by. */
    static FastFrame.Body answerConfirm(Sequencer held, FastDisk disk, Roster.Incarnation self,
            FastFrame.Header header, FastWriteFrame.Confirm confirm) throws IOException {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no term");
        }
        if (disk.journal().isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                    "this pod holds no fast journal");
        }
        return term.get().answerConfirm(header, confirm,
                term.get().commitDesk(disk.journal().get(), self));
    }

    /**
     * Registers the write's leader frames -- COMMIT (M13.27s) and CONFIRM
     * (M13.27v) -- each answer waking {@code exposed} (M13.27w): either can
     * complete a batch's quorum. ⚠️ EVEN A REFUSAL WAKES IT: a wake that
     * finds nothing owed costs one in-memory drain.
     */
    static void registerWrite(java.util.function.BiConsumer<Integer, FastFrameRouter.Handler> handle,
            java.util.function.Supplier<Sequencer> held, FastDisk disk, Roster.Incarnation self,
            ExposedDrain exposed) {
        handle.accept(FastWriteFrame.KIND_COMMIT, (header, body) -> {
            FastFrame.Body answer = answerCommit(held.get(), disk, self, header,
                    (FastWriteFrame.Commit) body);
            exposed.wake();
            return answer;
        });
        // ⚠️ M13.30d: a rostered pod's interest, kept for publication.
        handle.accept(FastPublishFrame.KIND_INTEREST,
                (header, body) -> answerInterest(held.get(), disk, self, header,
                        (FastPublishFrame.Interest) body));
        handle.accept(FastWriteFrame.KIND_CONFIRM, (header, body) -> {
            FastFrame.Body answer = answerConfirm(held.get(), disk, self, header,
                    (FastWriteFrame.Confirm) body);
            exposed.wake();
            return answer;
        });
    }

    /**
     * Sends the EXPOSEDs the leading term's desk owes (M13.27w); nothing on
     * a pod leading no term or holding no fast journal. ⚠️ A ROSTER READ
     * THAT FAILS SENDS NOTHING, and the exposures stay owed for the next
     * answer's drain.
     */
    static void sendExposed(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        try {
            term.get().sendExposed(term.get().commitDesk(disk.journal().get(), self), transport);
        } catch (IOException rosterUnread) {
            // owed still: the next COMMIT or CONFIRM answer drains again
        }
    }

    /**
     * A writer's forwarded REGISTER (M13.83a; ADR-0082 §2): taken into the
     * catalog this pod's term record is fed from.
     *
     * <p>⚠️ ONLY FROM A POD ROSTERED IN THE FAST TERM THIS POD LEADS, AND
     * ONLY A {@code wal=true} INDEX (its review round 1, P1): the leader's own
     * writes are placed by this catalog, so a REGISTER from anyone else -- or
     * for a default index -- would move them. A rostered pod is a member of
     * this cluster, its sender bound by ADR-0084's peer certificate, and its
     * registration is the plugin's word as a direct one is: last wins, as
     * ADR-0015 has it -- a deleted and recreated index re-registers under its
     * old name (round 2, P5). Refused {@code not rostered} or {@code not fast}
     * otherwise.
     */
    static FastFrame.Body answerRegister(Sequencer held,
            io.github.huyz0.os.biningester.ingest.IndexCatalog catalog,
            FastFrame.Header header, FastFrame.Register register) throws IOException {
        Optional<FastLeaderTerm> term = LocalSequencer.underneath(held)
                .filter(LocalSequencer::serving).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no fast term");
        }
        if (term.get().rostered(header.senderUid()).isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    header.senderUid() + " is not rostered in term " + term.get().epoch());
        }
        io.github.huyz0.os.biningester.format.IndexRegistration registration =
                register.registration();
        if (!registration.wal()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                    "only a wal=true index is forwarded");
        }
        catalog.register(registration);
        return new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
    }

    /**
     * An INTEREST (ADR-0081 §6; M13.30d), kept by the term this pod leads --
     * only from a pod ROSTERED in it (the header's sender, which ADR-0084's
     * peer certificate binds), so a successor's FENCE reaches every pod a
     * PUBLISH could reach. Refused {@code not rostered} otherwise, and
     * {@code not fast} by a leader holding no fast journal, which publishes
     * nothing.
     */
    static FastFrame.Body answerInterest(Sequencer held, FastDisk disk,
            Roster.Incarnation self, FastFrame.Header header,
            FastPublishFrame.Interest interest)
            throws IOException {
        Optional<FastLeaderTerm> term = LocalSequencer.underneath(held)
                .filter(LocalSequencer::serving).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || header.epoch() != term.get().epoch()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no fast term " + header.epoch());
        }
        if (disk.journal().isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(),
                    "this pod holds no fast journal");
        }
        Optional<Roster.Incarnation> pod = term.get().rostered(header.senderUid());
        if (pod.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    header.senderUid() + " is not rostered in term " + term.get().epoch());
        }
        term.get().commitDesk(disk.journal().get(), self).publications()
                .interest(pod.get(), interest.added(), interest.removed());
        return new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
    }

    /** The most entry bytes one PUBLISH carries, under a peer frame's bound. */
    static final long PUBLISH_BYTES =
            io.github.huyz0.os.biningester.http.FastFrameService.MAX_FRAME_BYTES - 4_096;

    /**
     * Sends what the leading term's exposures owe each interested pod, in
     * offset order per stream, chunked under a peer frame's bytes (ADR-0081
     * §6; M13.30d): how many PUBLISH frames were sent.
     *
     * <p>⚠️ ONLY TO THE INCARNATION STILL ROSTERED (review round 1, P1): a
     * pod that departed or restarted since its INTEREST is owed nothing and its
     * interest is forgotten, so a dead endpoint is not sent to on every tick.
     * The roster is the term's cached one, read at most once per
     * {@code min_upload_interval}. ⚠️ BEST EFFORT: one that fails is
     * dropped and counted, and its pod's subscribers read the entries from the
     * chain once uploaded.
     */
    static int publish(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport) {
        return publish(held, disk, self, transport, entry -> { });
    }

    /**
     * The same, what this pod is owed itself handed to {@code own} -- its own
     * hub -- rather than sent as a frame to itself (M13.30k).
     */
    static int publish(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport, FastPublishHandler.Pushed own) {
        Optional<FastLeaderTerm> term = LocalSequencer.underneath(held)
                .filter(LocalSequencer::serving).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return 0;
        }
        var desk = term.get().commitDesk(disk.journal().get(), self).publications();
        Map<FastJournalRecord.Entry, Long> sizes = new IdentityHashMap<>();
        int sent = 0;
        for (var owed : desk.take().entrySet()) {
            Roster.Incarnation pod = owed.getKey();
            if (pod.equals(self)) {
                for (FastJournalRecord.Entry e : owed.getValue()) {
                    try {
                        own.deliver(e);
                    } catch (IOException | RuntimeException undelivered) {
                        // read from the chain once uploaded, as a follower's
                    }
                }
                continue;
            }
            try {
                if (!term.get().rostered(pod.podUid()).equals(Optional.of(pod))) {
                    desk.forget(pod);
                    continue;
                }
            } catch (IOException unread) {
                FastMetrics.published(false);
                continue;
            }
            for (List<FastJournalRecord.Entry> entries : chunks(owed.getValue(),
                    PUBLISH_BYTES, e -> sizes.computeIfAbsent(e, x -> x.encode().length + 4L))) {
                try {
                    transport.exchange(pod.endpoint(), FastFrame.encode(term.get().epoch(),
                            self.podUid(), pod.podUid(), new FastPublishFrame.Publish(entries)));
                    FastMetrics.published(true);
                    sent++;
                } catch (IOException | RuntimeException unsent) {
                    FastMetrics.published(false); // the chain carries them once uploaded
                }
            }
        }
        return sent;
    }

    /**
     * The leader pod's own interest (ADR-0081 §6; M13.30k): {@code wanted},
     * its fast streams with a subscriber on its own hub, set on the term it
     * leads on each drain -- the leader joins no term of its own, so no
     * INTEREST of its reaches a leader. Nothing when it leads no fast term.
     */
    static void interestOwn(Sequencer held, FastDisk disk, Roster.Incarnation self,
            java.util.Set<io.github.huyz0.os.biningester.format.RunKey> wanted) {
        Optional<FastLeaderTerm> term = LocalSequencer.underneath(held)
                .filter(LocalSequencer::serving).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        term.get().commitDesk(disk.journal().get(), self).publications()
                .interestExactly(self, wanted);
    }

    /**
     * {@code entries}, in order, cut into runs of at most {@code bound} bytes
     * by {@code size} (M13.30c review, P2): one exposure step can hold many
     * batches, each at most the _bulk chunk. An entry over the bound alone is
     * a run of its own.
     */
    static List<List<FastJournalRecord.Entry>> chunks(List<FastJournalRecord.Entry> entries,
            long bound, ToLongFunction<FastJournalRecord.Entry> size) {
        List<List<FastJournalRecord.Entry>> runs = new ArrayList<>();
        List<FastJournalRecord.Entry> run = new ArrayList<>();
        long bytes = 0;
        for (FastJournalRecord.Entry e : entries) {
            long s = size.applyAsLong(e);
            if (!run.isEmpty() && bytes + s > bound) {
                runs.add(run);
                run = new ArrayList<>();
                bytes = 0;
            }
            run.add(e);
            bytes += s;
        }
        if (!run.isEmpty()) {
            runs.add(run);
        }
        return runs;
    }

    /** A committed fast segment, delivered to subscribers: {@code DefaultIngest.delivered}. */
    @FunctionalInterface
    interface Delivered {
        void delivered(String segmentKey, io.github.huyz0.os.biningester.format.CommitDelta delta,
                long epoch);
    }

    /**
     * Uploads what the leading term has exposed and the chain has not
     * committed (M13.31a; ADR-0081 §7), at the offsets assigned; nothing on a
     * pod leading no serving term or holding no fast journal. ⚠️ A FAILED
     * UPLOAD LEAVES THE ENTRIES HELD AND EXPOSED: the next one retries them.
     * Once committed, the segment is delivered as a flush's is (M13.30a).
     */
    static void upload(Sequencer held, FastDisk disk, Roster.Incarnation self,
            io.github.huyz0.os.biningester.sequencer.RecoveryWriter.SegmentUpload upload,
            TermJoiner.Transport transport, Delivered delivered) {
        upload(held, disk, self, upload, transport, delivered, true);
    }

    /** The same, unpaced when not {@code paced}: a leader's last as it leaves (M13.35c). */
    static void upload(Sequencer held, FastDisk disk, Roster.Incarnation self,
            io.github.huyz0.os.biningester.sequencer.RecoveryWriter.SegmentUpload upload,
            TermJoiner.Transport transport, Delivered delivered, boolean paced) {
        upload(held, disk, self, upload, transport, delivered, paced, () -> 0L,
                java.time.Duration.ZERO);
    }

    /**
     * The same, a paced upload made only once its trigger fires (ADR-0081 §7;
     * M13.31c): the tightest {@code flushTimer} since the term's last, a
     * stream at {@code B / 2}, the journal past half its cap, or a departing
     * pod's ask.
     */
    static void upload(Sequencer held, FastDisk disk, Roster.Incarnation self,
            io.github.huyz0.os.biningester.sequencer.RecoveryWriter.SegmentUpload upload,
            TermJoiner.Transport transport, Delivered delivered, boolean paced,
            io.github.huyz0.os.biningester.sequencer.MonotonicClock mono,
            java.time.Duration flushTimer) {
        Optional<LocalSequencer> local = LocalSequencer.underneath(held)
                .filter(LocalSequencer::serving);
        Optional<FastLeaderTerm> term = local.flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        FastLeaderTerm fast = term.get();
        io.github.huyz0.os.biningester.sequencer.FastUploader.Chain chain =
                new io.github.huyz0.os.biningester.sequencer.FastUploader.Chain() {
                    @Override
                    public long next(io.github.huyz0.os.biningester.format.RunKey stream) {
                        return local.get().committedNext(stream);
                    }

                    @Override
                    public java.util.Map<io.github.huyz0.os.biningester.format.RunKey, Long>
                            commitAt(String segmentKey,
                                    java.util.Map<io.github.huyz0.os.biningester.format.RunKey,
                                            Long> firsts,
                                    java.util.Map<io.github.huyz0.os.biningester.format.RunKey,
                                            Integer> counts) throws IOException {
                        io.github.huyz0.os.biningester.format.CommitDelta delta = local.get()
                                .commitAt(new io.github.huyz0.os.biningester.sequencer
                                        .CommitRequest(self.podId(), "fast" + fast.epoch(),
                                                fast.uploadSeq(firsts), segmentKey, counts),
                                        firsts);
                        fast.uploadCommitted(firsts);
                        // ⚠️ M13.30a: pushed and signalled as a flush's is -- and
                        // a delivery that throws never undoes the commit's
                        // bookkeeping (its review round 1, P3).
                        try {
                            delivered.delivered(segmentKey, delta, local.get().epoch());
                        } catch (RuntimeException undelivered) {
                            System.getLogger(FastLeaderFrames.class.getName()).log(
                                    System.Logger.Level.WARNING,
                                    "a fast segment was committed but not delivered",
                                    undelivered);
                        }
                        return committedNext(delta);
                    }
                };
        try {
            io.github.huyz0.os.biningester.sequencer.CommitDesk desk =
                    fast.commitDesk(disk.journal().get(), self);
            if (paced && !desk.uploadDue(mono.nanos(), flushTimer)) {
                return;
            }
            if (fast.upload(desk, disk.journal().get(), chain, upload, transport, paced)
                    .isPresent()) {
                desk.uploaded(mono.nanos());
                FastMetrics.uploaded(paced); // ⚠️ M13.36: by cause, never by index
            }
        } catch (IOException | RuntimeException failed) {
            System.getLogger(FastLeaderFrames.class.getName()).log(
                    System.Logger.Level.WARNING, "the fast upload failed; the next retries",
                    failed);
        }
    }

    /** Each stream's committed next offset after {@code delta}: what it actually holds. */
    static java.util.Map<io.github.huyz0.os.biningester.format.RunKey, Long> committedNext(
            io.github.huyz0.os.biningester.format.CommitDelta delta) {
        java.util.Map<io.github.huyz0.os.biningester.format.RunKey, Long> next =
                new java.util.HashMap<>();
        for (io.github.huyz0.os.biningester.format.SegmentCommit segment : delta.segments()) {
            for (io.github.huyz0.os.biningester.format.RunCommit run : segment.runs()) {
                next.merge(run.key(), run.firstOffset() + run.recordCount(), Math::max);
            }
        }
        return next;
    }

    /**
     * Sends the REPLICAs the leading term's desk owes and counts their acks
     * (M13.27x); nothing on a pod leading no term or holding no fast journal.
     * ⚠️ A ROSTER READ THAT FAILS SENDS NOTHING: the copies stay owed for the
     * next drain.
     */
    static void sendReplicas(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport) {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty() || disk.journal().isEmpty()) {
            return;
        }
        try {
            term.get().sendReplicas(term.get().commitDesk(disk.journal().get(), self), transport);
        } catch (IOException rosterUnread) {
            // owed still: the next COMMIT or CONFIRM answer drains again
        }
    }

    static FastFrame.Body answer(Sequencer held, FastFrame.Header header, FastFrame.Body body)
            throws IOException {
        Optional<FastLeaderTerm> term =
                LocalSequencer.underneath(held).flatMap(LocalSequencer::fastTerm);
        if (term.isEmpty()) {
            return new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                    "this pod leads no term");
        }
        return switch (body) {
            case FastFrame.Join join -> term.get().answerJoin(header, join);
            case FastFrame.Depart depart -> term.get().answerDepart(header, depart);
            case FastFrame.HeldReport report -> term.get().answerHeld(header, report);
            default -> throw new IllegalArgumentException("a leader answers no kind "
                    + body.kind() + " here");
        };
    }
}
