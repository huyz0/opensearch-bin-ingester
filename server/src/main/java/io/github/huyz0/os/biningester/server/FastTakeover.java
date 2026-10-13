// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitDesk;
import io.github.huyz0.os.biningester.sequencer.EmptyTermCloser;
import io.github.huyz0.os.biningester.sequencer.FastJournal;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.FastTermOpening;
import io.github.huyz0.os.biningester.sequencer.JoinDesk;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.PodGone;
import io.github.huyz0.os.biningester.sequencer.RecoveryWriter;
import io.github.huyz0.os.biningester.sequencer.ReleaseSender;
import io.github.huyz0.os.biningester.sequencer.RosterDecisions;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.StreamDecision;
import io.github.huyz0.os.biningester.sequencer.TakeoverCollection;
import io.github.huyz0.os.biningester.sequencer.TakeoverRun;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Starts the takeover of the term this pod leads, once (M13.33r; ADR-0081 §5
 * steps 4-7): {@link TakeoverRun} over the term's chain, its roster, the
 * members' frames and this pod's journal, on its own thread.
 *
 * <p>⚠️ A TAKEOVER THAT CANNOT FINISH GIVES THE TERM BACK: its leader holds
 * every stream it has not decided, so a term kept would serve nothing fast
 * for its whole lease. Given back, a newer term -- this pod's or another's --
 * walks the same unclosed terms and decides them afresh.
 */
final class FastTakeover {

    /** Collection rounds per pass; a member still waited for is asked again next pass. */
    static final int ROUNDS = 3;
    /**
     * The most record bytes one recovery entry's uploads carry (M13.33q): a
     * peer frame's worth, the most a member's COLLECTED pages hand over at once.
     */
    static final long RECOVERY_BATCH_BYTES = 64L << 20;
    /** Between collection rounds, and between asking whether the wait has passed. */
    static final Duration RETRY = Duration.ofMillis(250);

    /**
     * A recovery entry committed, under its term's epoch, handed on to be
     * pushed live (M13.33w): {@code DefaultIngest.recovered}.
     */
    @FunctionalInterface
    interface Recovered {
        void recovered(Recovery recovery, long epoch);
    }

    /** One takeover, run to its end. */
    @FunctionalInterface
    interface Run {
        TakeoverRun.Outcome run() throws IOException, InterruptedException;
    }

    private FastTakeover() {
    }

    /**
     * Starts {@code held}'s takeover when it has one not yet claimed.
     *
     * @param launch runs the takeover: a virtual thread, or the caller's own in a test
     * @return whether a takeover started
     */
    static boolean start(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport, PodGone podGone, RecoveryWriter.SegmentUpload upload,
            BinStore store, String prefix, Clock wall, java.util.concurrent.Executor launch) {
        return start(held, disk, self, transport, podGone, upload, (recovery, epoch) -> { },
                store, prefix, wall, launch);
    }

    /** The same, each recovery entry committed handed to {@code recovered} (M13.33w). */
    static boolean start(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport, PodGone podGone, RecoveryWriter.SegmentUpload upload,
            Recovered recovered, BinStore store, String prefix, Clock wall,
            java.util.concurrent.Executor launch) {
        return start(held, disk, self, transport, podGone, upload, recovered,
                index -> java.util.Set.of(), store, prefix, wall, launch);
    }

    /**
     * The same, {@code streamsOf} naming every stream of an index (M13.33aa):
     * a lost index's streams are each decided, entries or none.
     */
    static boolean start(Sequencer held, FastDisk disk, Roster.Incarnation self,
            TermJoiner.Transport transport, PodGone podGone, RecoveryWriter.SegmentUpload upload,
            Recovered recovered, java.util.function.Function<java.util.UUID,
                    java.util.Set<RunKey>> streamsOf, BinStore store, String prefix, Clock wall,
            java.util.concurrent.Executor launch) {
        Optional<LocalSequencer> term = LocalSequencer.underneath(held);
        Optional<FastLeaderTerm> fast = term.flatMap(LocalSequencer::fastTerm);
        if (fast.isEmpty()) {
            return false;
        }
        if (disk.journal().isEmpty()) {
            // ⚠️ NO TAKEOVER, SO NO HOLD (M13.33n review round 1, P2): a
            // diskless leader recovers nothing, and holding a walked index's
            // default path for its whole term would stop it for nothing.
            fast.get().noTakeover();
            return false;
        }
        Optional<FastTermOpening.Opened> claimed = fast.get().claimTakeover();
        if (claimed.isEmpty()) {
            return false;
        }
        LocalSequencer chain = term.get();
        // ⚠️ THE CLAIM IS SPENT (its review round 1, P1): whatever fails
        // before the takeover runs gives the term back, or its leader would
        // hold every stream for the rest of its lease.
        try {
            launch(claimed.get(), fast.get(), chain, disk.journal().get(), self, transport,
                    podGone, upload, recovered, streamsOf, store, prefix, wall, launch);
        } catch (RuntimeException | Error failed) {
            giveBack(chain::close);
            throw failed;
        }
        return true;
    }

    private static void launch(FastTermOpening.Opened opened, FastLeaderTerm fastTerm,
            LocalSequencer chain, FastJournal journal, Roster.Incarnation self,
            TermJoiner.Transport transport, PodGone podGone, RecoveryWriter.SegmentUpload upload,
            Recovered recovered, java.util.function.Function<java.util.UUID,
                    java.util.Set<RunKey>> streamsOf, BinStore store, String prefix, Clock wall,
            java.util.concurrent.Executor launch) {
        Optional<FastLeaderTerm> fast = Optional.of(fastTerm);
        CommitDesk desk = fastTerm.commitDesk(journal, self);
        long epoch = fastTerm.epoch();
        JoinDesk.Sleeper sleeper = nanos -> TimeUnit.NANOSECONDS.sleep(nanos);
        TakeoverCollection collection =
                new TakeoverCollection(self.podUid(), epoch, transport, podGone, sleeper, RETRY);
        RosterDecisions decisions = new RosterDecisions(store, prefix, epoch);
        ReleaseSender release = new ReleaseSender(self.podUid(), epoch, transport);
        EmptyTermCloser closer = new EmptyTermCloser(store, prefix);
        TakeoverRun.Steps steps = new TakeoverRun.Steps() {
            @Override
            public TakeoverCollection.Collected collect(List<Roster> stillOpen,
                    long closedThrough) throws InterruptedException {
                return collection.collect(stillOpen, closedThrough, ROUNDS);
            }

            @Override
            public long chainNext(RunKey stream) {
                return chain.committedNext(stream);
            }

            @Override
            public java.util.Set<RunKey> streamsOf(java.util.UUID index) {
                return streamsOf.apply(index);
            }

            @Override
            public Recovery recover(List<StreamDecision.Decided> batch) throws IOException {
                Recovery recovery = chain.recover(batch, upload);
                FastMetrics.voided(recovery); // ⚠️ M13.36
                // ⚠️ M13.33w: pushed live, as a fast upload's commit is.
                recovered.recovered(recovery, chain.epoch());
                return recovery;
            }

            @Override
            public RosterDecisions.Outcome recordDecisions(Map<RunKey, Long> resumeAt)
                    throws IOException {
                return decisions.record(resumeAt);
            }

            @Override
            public void rosterWritten() throws IOException {
                fastTerm.rosterWritten();
            }

            @Override
            public int release(List<Roster> unclosed, Map<RunKey, Long> below) {
                return release.send(unclosed, below);
            }

            @Override
            public int close(List<Roster> unclosed, Predicate<Roster> decided)
                    throws IOException {
                return closer.close(epoch, unclosed, decided);
            }
        };
        TakeoverRun run = new TakeoverRun(steps, desk.hold(),
                () -> opened.mayDecide(wall.millis()), sleeper, RETRY,
                FastLeaderTerm.STREAM_BOUND, RECOVERY_BATCH_BYTES);
        launch.execute(() -> runOrGiveBack(
                () -> run.run(opened.stillOpen(), opened.closedThrough(), journal.held()),
                chain::close));
    }

    /** Runs {@code run}, giving {@code term} back unless it finished. */
    static void runOrGiveBack(Run run, Closeable term) {
        System.Logger log = System.getLogger(FastTakeover.class.getName());
        try {
            TakeoverRun.Outcome outcome = run.run();
            if (outcome instanceof TakeoverRun.Done) {
                return;
            }
            log.log(System.Logger.Level.WARNING, "a newer term fenced this takeover: " + outcome);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.log(System.Logger.Level.WARNING, "the takeover was interrupted");
        } catch (IOException | RuntimeException failed) {
            log.log(System.Logger.Level.WARNING, "the takeover failed", failed);
        } catch (Error fatal) {
            giveBack(term);
            throw fatal;
        }
        giveBack(term);
    }

    private static void giveBack(Closeable term) {
        try {
            term.close();
        } catch (IOException failed) {
            System.getLogger(FastTakeover.class.getName()).log(System.Logger.Level.WARNING,
                    "giving the term back failed", failed);
        }
    }

    /**
     * {@code upload}, at most one per {@code interval} (ADR-0081 §5 step 6:
     * {@code min_upload_interval}): a takeover's segments wait their turn.
     */
    static RecoveryWriter.SegmentUpload paced(RecoveryWriter.SegmentUpload upload,
            MonotonicClock mono, Duration interval, JoinDesk.Sleeper sleeper) {
        long gap = interval.toNanos();
        long[] last = {0};
        boolean[] any = {false};
        return runs -> {
            synchronized (last) {
                if (any[0]) {
                    long wait = last[0] + gap - mono.nanos();
                    if (wait > 0) {
                        try {
                            sleeper.sleepNanos(wait);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException("interrupted between uploads", interrupted);
                        }
                    }
                }
                any[0] = true;
                last[0] = mono.nanos();
                return upload.upload(runs);
            }
        };
    }
}
