// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.Checkpoint.StreamOffsets;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WRITES a checkpoint every K deltas or T seconds, so M4.9 can bound recovery.
 *
 * <p>⚠️ IT WRITES ONLY WHEN THERE IS SOMETHING NEW, guarded by a dirty flag SET
 * by a commit and CLEARED by a successful write. ⚠️ AN EARLIER DRAFT JUSTIFIED
 * THAT WITH NFR-2 AND AN OBJECT COUNT GROWING WITH UPTIME, and both halves were
 * wrong: NFR-2 is zero requests from CONSUMERS and says nothing about the
 * sequencer, and an unconditional tick rewrites the SAME key while {@code seq}
 * is unchanged, so what grows is the PUT count at a fixed rate per chain, not
 * the object count. The honest reason is smaller: a request that buys nothing
 * is waste, and the M4 SPEC prices checkpoints at one PUT per K deltas or T
 * seconds — a tick with no deltas is neither.
 *
 * <p>⚠️ THE ROW SPECIFYING THIS ASKED FOR TWO THINGS THAT CANNOT BOTH HOLD —
 * "the SAME key for a T tick with no deltas between" and "a cluster which
 * commits and then stops eventually stops writing". The row settles it itself:
 * it names "a dirty flag that is never cleared" as a mutant the quiet-cluster
 * test must kill, so it already assumes the flag. The same-key clause describes
 * a writer without one and is void rather than untested.
 *
 * <p>⚠️ {@code seq} IS THE CHAIN'S {@code nextSequence}, the next slot NOT
 * included, so M4.9 replays deltas from {@code seq} forward. A per-checkpoint
 * counter would leave M4.9 unable to bound replay from the key at all.
 *
 * <p>⚠️ A FAILED CHECKPOINT NEVER FAILS A COMMIT. The records are already
 * durable and acknowledged when this runs; a checkpoint is a recovery
 * optimisation, so an {@code IOException} here is logged, the dirty flag is
 * LEFT SET, and the next trigger retries. Propagating it would fail writes that
 * already succeeded.
 * <p>⚠️ EVERY {@code SegmentCommit} CARRIES AN OPTIONAL
 * {@code Attribution(podId, incarnationId, flushSeq)} (ADR-0036), and this
 * writer records a POINTER to the delta that last applied for each pod.
 * ⚠️ M5.1 IS WHERE A SUCCESSOR READS THEM BACK, folding this checkpoint and the
 * uncheckpointed tail after it into {@code IdempotencyWindow}.
 *
 * <p>⚠️ WHAT REMAINS PROCESS-LOCAL is the in-memory map below: it learns only
 * from {@link #observe}, which {@code LocalSequencer} calls AFTER a successful
 * {@code commitAll}. An AMBIGUOUS commit — the PUT landed, the response was
 * lost — applies without ever reaching it. M5.23 reconciles that case from the
 * chain into the WINDOW, so the running process answers the retry, and M5.25
 * hands it to this map as well through {@link #observeReconciled} — so a
 * SUCCESSOR inherits it too, rather than only while that delta is still in the
 * uncheckpointed tail.
 */
final class CheckpointWriter implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(CheckpointWriter.class.getName());

    /** The T trigger's seam, mirroring {@link LocalSequencer.RenewTicker}. */
    @FunctionalInterface
    interface Ticker {
        void awaitNextTick() throws InterruptedException;
    }

    static Ticker sleepFor(Duration interval) {
        // ⚠️ Duration, never toMillis(): M4.7 measured an interval under 1ms
        // truncating to Thread.sleep(0) and writing 98 objects where 7 were due.
        return () -> Thread.sleep(interval);
    }

    private final BinStore store;
    private final CommitLog log;
    private final LogKeys keys;
    private final long everyDeltas;
    private final Map<String, Checkpoint.PodState> pods = new HashMap<>();
    private final AtomicLong ticksProcessed = new AtomicLong();
    private final Thread tickerThread;
    private long pendingSequence;
    private Map<RunKey, StreamOffsets> pendingStreams = Map.of();
    /**
     * The oldest offset GC says is still readable, per stream.
     *
     * <p>⚠️ IT ONLY EVER RISES. A stream absent from it reads as 0, which is
     * also what "everything is retained" means — the two are the same answer
     * here on purpose, because a stream nobody has reported on and one nothing
     * has been collected from are indistinguishable to a consumer, and both
     * must be readable from the beginning.
     */
    private final Map<RunKey, Long> retainedFloor = new LinkedHashMap<>();

    private long deltasSinceCheckpoint;
    private boolean dirty;

    /**
     * What this writer has written and not yet seen collected, oldest first,
     * and the newest of them (M8.39).
     *
     * <p>⚠️ **HELD SO CHAIN GC NEEDS NO READ.** Every other source of "the
     * newest checkpoint and every checkpoint of the chain" is a GET and a LIST,
     * which wired into the retention loop is a request per pass on an idle
     * leader. What a predecessor wrote is not here: it stays in the bucket.
     */
    private final List<ChainGc.CheckpointAt> written = new ArrayList<>();

    /** A week of checkpoints at the shipped one-minute interval. */
    static final int MAX_WRITTEN = 10_080;
    private Checkpoint newestWritten;
    private volatile boolean closed;

    CheckpointWriter(BinStore store, CommitLog log, String prefix, long everyDeltas,
            Duration everyInterval, Ticker ticker) {
        this.store = Objects.requireNonNull(store, "store");
        this.log = Objects.requireNonNull(log, "log");
        this.keys = new LogKeys(Objects.requireNonNull(prefix, "prefix"), log.epoch());
        Objects.requireNonNull(everyInterval, "everyInterval");
        Objects.requireNonNull(ticker, "ticker");
        // ⚠️ REFUSED AT CONSTRUCTION, not at the first trigger. K = 0 is "every
        // zero deltas" and T = 0 is a tick loop that never sleeps; both are
        // configuration errors, and a deployment that fails to start is cheaper
        // than one that bills until somebody notices.
        checkPolicy(everyDeltas, everyInterval);
        this.everyDeltas = everyDeltas;
        this.tickerThread = Thread.ofVirtual().name("ckpt-tick").unstarted(() -> tickForever(ticker));
        this.tickerThread.start();
    }

    /**
     * What a GC pass reports it has actually deleted, per stream (M7.10).
     *
     * <p>⚠️ UNTIL THIS EXISTED THE WRITER WROTE A LITERAL 0, which was truthful
     * only while nothing deleted anything. A checkpoint that keeps saying 0
     * after GC has run tells every consumer that the whole stream is still
     * readable, so one seeking into collected records meets a 404 instead of
     * the refusal it is owed.
     *
     * <p>⚠️ WHAT IS REPORTED IS WHAT WAS DELETED, NOT WHAT WAS CONDEMNED. A
     * failed batch leaves its objects readable, and moving the boundary over
     * them refuses a consumer for records that are still there.
     *
     * <p>⚠️ A STREAM NOT NAMED HERE KEEPS 0. A boundary invented for a stream
     * no pass has reported on is the same defect one step earlier.
     *
     * <p>⚠️ AND NOTHING IN HERE MAY THROW ON THE COMMIT THREAD. A value past
     * {@code nextOffset} would make {@code StreamOffsets} refuse — taking the
     * sequencer down over a GC report that arrived late — so it is CLAMPED to
     * {@code nextOffset}, which says the stream is EMPTY — the strongest
     * boundary there is, refusing every consumer of that stream until it
     * commits again. That is deliberate rather than conservative: a report that
     * arrived after the stream was truncated is still a report that those
     * records are gone. A negative one is ignored and logged.
     */
    public synchronized void observeRetained(Map<RunKey, Long> oldestRetained) {
        Objects.requireNonNull(oldestRetained, "oldestRetained");
        for (Map.Entry<RunKey, Long> reported : oldestRetained.entrySet()) {
            if (reported.getKey() == null || reported.getValue() == null) {
                continue;
            }
            if (reported.getValue() < 0) {
                // ⚠️ LOGGED, NOT SWALLOWED. A negative boundary is a defect
                // upstream, and the commit thread is not where it should be
                // discovered -- but a silent `continue` makes it undiscoverable
                // anywhere.
                LOG.log(System.Logger.Level.WARNING, () -> "a GC pass reported a negative "
                        + "oldest-retained offset for " + reported.getKey() + ": "
                        + reported.getValue() + " -- ignored, and the boundary for that "
                        + "stream stays where it was");
                continue;
            }
            // ⚠️ MERGED WITH max, SO RETENTION ONLY MOVES FORWARD. Two passes
            // can report out of order -- the second is not necessarily the
            // later one once a retry is involved -- and a boundary that walked
            // BACKWARDS would say deleted records are readable again.
            retainedFloor.merge(reported.getKey(), reported.getValue(), Math::max);
        }
    }

    /**
     * Every commit passes through here: it is the only place the writer can see
     * {@code podId} and {@code flushSeq}, which the chain carries only since M4.10c, and only per SegmentCommit.
     *
     * <p>⚠️ Does NOT throw. See the class note on failed checkpoints.
     */
    synchronized void observe(List<CommitRequest> requests, long appliedSequence) {
        try {
            capture(requests, appliedSequence);
        } catch (Throwable neverFailAnAcknowledgedCommit) {
            // ⚠️ Throwable, not IOException, and for the reason `LeaseRenewer.run`
            // and `commitBatch` both catch it: these records are ALREADY durable
            // and acknowledged. An unchecked throw escaping here fails a whole
            // BatchingSequencer window for writes that succeeded.
            LOG.log(System.Logger.Level.WARNING,
                    "checkpoint bookkeeping failed; the commit itself stands",
                    neverFailAnAcknowledgedCommit);
        }
    }

    private void capture(List<CommitRequest> requests, long appliedSequence) {
        // ⚠️ THE SEQUENCE OF THE DELTA THAT APPLIED, passed in by the caller --
        // NOT `log.nextSequence()`. `observe` runs AFTER `commitAll`, by which
        // point `apply` has advanced `nextSequence` past the delta, so reading
        // it here recorded a pointer ONE PAST the object it names: measured, a
        // delta at sequence 0 stored 1/1. Every checkpoint then pointed at an
        // empty slot or an unrelated later delta, and a detected replay could
        // not be answered at all.
        long pointerEpoch = log.epoch();
        long pointerSequence = appliedSequence;
        for (CommitRequest request : requests) {
            rememberPod(request.podId(), request.incarnationId(), request.flushSeq(),
                    pointerEpoch, pointerSequence);
        }
        // ⚠️ ONE `commitAll` IS ONE DELTA, however many requests it batches --
        // M4.7's whole point -- so this counts deltas, which is what K means.
        // ⚠️ THE CHAIN STATE IS CAPTURED HERE, ON THE COMMIT THREAD, and that
        // is a correctness requirement rather than tidiness. `CommitLog` holds
        // `nextOffsets` in a plain HashMap and `nextSequence` in a plain long
        // with no synchronization anywhere, so reading them from the ticker
        // thread has no happens-before with the writer. Two unsynchronised reads
        // can also SKEW: a sequence newer than the offsets makes M4.9 resume at
        // N with delta N-1 missing, and every stream in it rewinds -- I2, and
        // silent. (The other skew is harmless: `fold` uses Math::max.)
        // ⚠️ OFFSETS FIRST, SEQUENCE LAST, and the order is load-bearing now that
        // `observe` swallows Throwable: a throw BETWEEN the two assignments
        // leaves the process alive, and with the sequence written first it
        // leaves `pendingSequence` newer than `pendingStreams` -- the exact
        // skew this capture exists to prevent, waiting for the next tick.
        Map<RunKey, StreamOffsets> streams = new LinkedHashMap<>();
        for (Map.Entry<RunKey, Long> stream : log.offsets().entrySet()) {
            // ⚠️ 0 MEANS "NOTHING HAS BEEN COLLECTED FROM THIS STREAM", and
            // stays the answer until a GC pass says otherwise (M7.10) -- a
            // deployment with no GC role must not have its consumers refused.
            // ⚠️ CLAMPED, NEVER REFUSED: `StreamOffsets` throws when the
            // boundary is past `nextOffset`, and this runs on the commit
            // thread.
            long next = stream.getValue();
            long retained = Math.min(retainedFloor.getOrDefault(stream.getKey(), 0L), next);
            streams.put(stream.getKey(), new StreamOffsets(next, retained));
        }
        pendingStreams = streams;
        pendingSequence = log.nextSequence();
        deltasSinceCheckpoint++;
        dirty = true;
        if (deltasSinceCheckpoint >= everyDeltas) {
            writeIfDirty();
        }
    }

    /**
     * Seeds the pod states this leader INHERITED, so its checkpoint carries
     * pods it never saw commit (M5.55).
     *
     * <p>⚠️ WITHOUT THIS A MIDDLE LEADER SILENTLY TRUNCATES THE MAP, and the
     * records duplicate. {@code LocalSequencer.start} seeded the in-memory
     * window from {@code log.recoveredPods()} and built this writer with
     * nothing, so a leader that took over, committed, and checkpointed wrote
     * cumulative OFFSETS beside a pods map holding only the pods IT saw.
     * {@code ChainReplay} stops the next successor's walk AT that checkpoint,
     * so a pod that committed in the epoch before it is unprotected and its
     * retry is applied a second time -- I2, reachable with NO restart and no
     * legacy object, on any rolling deploy.
     *
     * <p>⚠️ THE KEYS ARRIVE SLOTTED AND ARE UN-SLOTTED HERE. {@code
     * recoveredPods()} is keyed {@code podId\0incarnationId}, because the
     * WINDOW must tell two incarnations apart; {@code Checkpoint.pods} is keyed
     * by {@code podId} alone, with the incarnation carried as a VALUE beside
     * the watermark. That is ADR-0036's explicit choice -- "{@code
     * incarnationId} is a VALUE beside {@code flushSeq}, never a qualifier on
     * the pod id" -- and it is what keeps this map ONE ENTRY PER POD rather
     * than one per restart, so seeding it does not trade the RECOVERY bound
     * away: the walk is bounded by the checkpoint sequence, which this does not
     * move. ⚠️ BUT THE MAP ITSELF NOW GROWS AND NOTHING EVICTS, which an
     * earlier draft of this paragraph denied by calling the size "bounded by
     * the fleet". The truncation M5.55 removes WAS the de-facto eviction. The
     * honest bound is DISTINCT podIds THAT HAVE EVER COMMITTED TO THIS CHAIN:
     * scale to 200 pods and back to 10 and all 200 slots ride in every
     * checkpoint thereafter. That is fleet-shaped only where pod names are
     * stable AND reused; ADR-0031 assumes uniqueness among LIVE pods, not
     * stability. {@code Checkpoint.PodState} names the invariant at stake --
     * "the checkpoint would grow with history against ADR-0033" -- and M5.72
     * owns the eviction rule.
     *
     * <p>⚠️ AND NO {@code cut < 0} ARM: every key is built by {@code
     * ChainReplay.slot}, so the separator is always present -- the same
     * construction proof as the paragraph below, and an earlier draft carried
     * an unreachable branch for it. ⚠️ WHAT BREAKING THAT PROOF WOULD COST,
     * stated because the price is out of proportion to the likelihood: a
     * separator-less key makes {@code substring} throw {@code
     * StringIndexOutOfBoundsException}, and this runs inside {@code
     * LocalSequencer.start}'s try, whose only catch is {@code IOException},
     * AFTER the renewer thread is up. An unchecked throw there escapes past
     * {@code leases.release()}, and the severity is PERMANENT rather than one
     * TTL: nothing then sets {@code closed} and nothing can set {@code fenced},
     * so the renewer renews forever and no node can ever take that term again.
     * ⚠️ AN EARLIER DRAFT OF THIS SENTENCE SAID "unusable for a full TTL",
     * which is the LESSER failure and the wrong way round -- {@code
     * LeaseRenewer#run} records that exact inversion being made and fixed once
     * already. {@link #checkPolicy} exists to prevent the same outage. Anyone
     * adding a second producer of {@code recoveredPods} owes this method either
     * the separator or a guard.
     *
     * <p>⚠️ NO BARE-SLOT GUARD, DELIBERATELY. A v0 slot names no incarnation and
     * could key nothing, but none can arrive: both producers of {@code
     * recoveredPods} are {@code ChainReplay} outputs, and it skips a slot with
     * no pointer or no incarnation before seeding. A guard here would be an
     * unreachable branch -- measured, by deleting it and finding nothing red.
     *
     * <p>⚠️ COLLISIONS ARE RESOLVED BY THE POINTER, AND AN EARLIER DRAFT GOT
     * THIS WRONG IN A WAY THAT DUPLICATED RECORDS HALF THE TIME. Two slots for
     * one pod differ by incarnation, and un-slotting collapses them onto one
     * key. That draft routed both through {@link #rememberPod} and argued the
     * existing merge "decides it exactly as a live commit would" -- false twice
     * over. A live commit resolves by ARRIVAL ORDER, which tracks recency;
     * here the order is {@code Map.copyOf}'s, which the JDK randomises per JVM.
     * And the merge keeps {@code existing} only when the incarnations MATCH, so
     * across incarnations the last visited simply wins. A pod that restarted
     * mid-term would have had its DEAD incarnation recorded and its live one
     * dropped on roughly half of starts, and the live one's retry would
     * duplicate at the next takeover.
     *
     * <p>⚠️ THE TIE IS REACHABLE AND IS BROKEN EXPLICITLY -- an earlier draft
     * called it an EQUIVALENT MUTANT and both reviewers measured that false.
     * The path is {@code commitAll} itself: {@code podb/i1} submits a flush,
     * the pod dies and restarts as {@code i2}, and {@code i2} submits before
     * the batcher drains. {@code BatchingSequencer.drainLoop} does not
     * partition by pod, {@code CommitLog} requires only distinct SEGMENT keys,
     * and the window keys on {@code (podId, incarnationId)} so neither is the
     * other's replay -- ONE delta, TWO attributions for one podId, and {@link
     * #capture} stamps both with the same pointer. That is the crash-during-
     * flush case the incarnation id exists for.
     *
     * <p>⚠️ ON A TIE THE OUTCOME IS STABLE RATHER THAN RIGHT. The pointer is
     * equal by construction and ADR-0036 makes the incarnation id unordered on
     * purpose, so what the tie-break buys is DETERMINISM, not correctness: the
     * outcome is arbitrary but reproducible, instead of {@code Map.copyOf}'s
     * salted order picking differently on half of JVM starts. A checkpoint that
     * consistently names one of the two can be reasoned about and reproduced;
     * one that flips per start cannot. ⚠️ THE WATERMARK IS NOT A RULE HERE, and
     * two drafts of this paragraph were wrong about it in opposite directions
     * -- first that nothing discriminates at all, then that the restarted slot
     * always holds the lower watermark. It holds over a window only: the
     * replacement begins at 0, so {@code <}-on-watermark picks it while the
     * dead slot's flush is not also that incarnation's first, and it inverts
     * once the replacement climbs past the dead watermark. M5.72 records it as
     * an option NOT TAKEN, with that window, rather than closing the case as
     * undecidable; what does remain undecidable is recorded there too, with the
     * rest of this map's open questions.
     *
     * <p>⚠️ THE ORDERING THAT DRAFT SAID DID NOT EXIST IS IN THE VALUE BEING
     * MERGED. {@code PodState} carries {@code (epoch, sequence)} -- the pointer
     * to the delta that last applied -- and that IS orderable even though the
     * incarnation id is not. Resolving here by the higher pointer leaves the
     * live commit path's semantics untouched.
     */
    synchronized void inherit(Map<String, Checkpoint.PodState> fromChain) {
        Map<String, Checkpoint.PodState> newest = new HashMap<>();
        fromChain.forEach((slot, state) -> newest.merge(
                slot.substring(0, slot.indexOf('\0')), state, CheckpointWriter::newer));
        newest.forEach((podId, state) -> rememberPod(podId, state.incarnationId(),
                state.lastAppliedFlushSeq(), state.epoch(), state.sequence()));
    }

    /**
     * Records one pod's flush as applied at {@code (epoch, sequence)}.
     *
     * <p>⚠️ Math::max WITHIN AN INCARNATION, never across one. A retried or late
     * flush arrives with a LOWER flushSeq than one already applied, and taking
     * the last seen would walk the watermark backwards. But a RESTARTED pod
     * reissues from 0 under a NEW incarnation, and maxing across that refuses
     * its second commit for ever — which is the suppression ADR-0036 exists to
     * prevent, and a depth-one fixture passes it.
     */
    private void rememberPod(String podId, String incarnationId, long flushSeq,
            long pointerEpoch, long pointerSequence) {
        pods.merge(podId,
                new Checkpoint.PodState(incarnationId, flushSeq, pointerEpoch, pointerSequence),
                (existing, incoming) -> existing.incarnationId() != null
                        && existing.incarnationId().equals(incoming.incarnationId())
                        && existing.lastAppliedFlushSeq() > incoming.lastAppliedFlushSeq()
                        ? existing : incoming);
    }

    /**
     * Of two slots for one pod, the one to carry forward: higher pointer first,
     * then a STABLE tie-break (M5.55).
     *
     * <p>⚠️ A TOTAL ORDER, DELIBERATELY. Returning either on a tie would leave
     * the answer to {@code Map.copyOf}'s per-JVM salt, which is the defect this
     * whole method exists to remove; the tie-break makes it arbitrary but
     * REPRODUCIBLE. It is not a claim that the winner is the live incarnation
     * -- see {@link #inherit} for what the fields do and do not settle.
     */
    private static Checkpoint.PodState newer(Checkpoint.PodState prior,
            Checkpoint.PodState now) {
        int byPointer = Long.compare(now.epoch(), prior.epoch());
        if (byPointer == 0) {
            byPointer = Long.compare(now.sequence(), prior.sequence());
        }
        if (byPointer != 0) {
            return byPointer > 0 ? now : prior;
        }
        return now.incarnationId().compareTo(prior.incarnationId()) > 0 ? now : prior;
    }

    /**
     * Records flushes learned from the CHAIN rather than from a commit that
     * returned (M5.25).
     *
     * <p>⚠️ THIS IS THE HALF M5.23 LEFT OPEN. An append whose response was lost
     * has still landed, and {@link #observe} only ever runs after a commit
     * RETURNS — so the flush was durable, answered by the running sequencer's
     * in-memory window, and invisible here. A SUCCESSOR seeds from
     * {@code Checkpoint.pods} plus the uncheckpointed tail, so it inherited the
     * fact only while that delta was still in the tail; once a later checkpoint
     * bounded past it, the retry was applied a second time (I2).
     *
     * <p>⚠️ IT RECORDS THE POD STATES AND NOTHING ELSE. {@link #capture} also
     * snapshots the chain — offsets and next sequence — from the commit thread
     * that just advanced them. Nothing advanced them here: the append that
     * landed was never applied to this log, so `nextSequence` still points AT
     * the delta rather than past it, and writing that pair would checkpoint a
     * chain state one behind the pointer it carries. The next real commit
     * captures both, and carries these pod states with it.
     *
     * <p>⚠️ Does NOT throw. See the class note on failed checkpoints.
     */
    synchronized void observeReconciled(List<SegmentCommit> landed, long epoch, long sequence) {
        try {
            for (SegmentCommit segment : landed) {
                SegmentCommit.Attribution attribution = segment.attribution();
                if (attribution != null) {
                    rememberPod(attribution.podId(), attribution.incarnationId(),
                            attribution.flushSeq(), epoch, sequence);
                }
            }
            // ⚠️ NOT MARKED DIRTY, and an earlier draft was. The reason given
            // -- so a tick writes these out if no commit follows -- does not
            // hold: until a real commit runs, `pendingSequence` is at or before
            // the ambiguous delta, so a successor finds that delta in the tail
            // anyway and needs no checkpoint to inherit it. What the flag DID
            // buy was a wasted write per reconcile and, more often, a
            // checkpoint whose `PodState` points at an entry at or beyond its
            // own `sequence`. ⚠️ NOT REMOVING THAT SHAPE, only its commonest
            // cause: `writeIfDirty` leaves the flag SET when its PUT throws, so
            // a tick between a reconcile and the next `capture` can still write
            // one. The next real commit carries these states, and that is the
            // checkpoint in which they mean anything.
        } catch (Throwable neverFailAnAcknowledgedCommit) {
            LOG.log(System.Logger.Level.WARNING,
                    "checkpoint bookkeeping failed for a reconciled append; the records "
                            + "themselves are durable either way",
                    neverFailAnAcknowledgedCommit);
        }
    }

    private void tickForever(Ticker ticker) {
        while (!closed) {
            try {
                ticker.awaitNextTick();
                if (!closed) {
                    writeOnTick();
                }
            } catch (InterruptedException maybeShutdown) {
                Thread.currentThread().interrupt();
                // ⚠️ AN INTERRUPT IS ONLY A SHUTDOWN IF `closed` SAYS SO, the
                // question `BatchingSequencer.drainLoop` already answers with
                // `return running ? stopping : null`. A store using the
                // restore-and-throw idiom -- `BoundedLock.takeOrFail` in this
                // very package uses it -- returns through the IOException catch
                // with the flag still set, and the next `Thread.sleep` throws at
                // once. Returning unconditionally there ends checkpointing for
                // the life of the process, with nothing logged.
                if (!closed) {
                    LOG.log(System.Logger.Level.ERROR,
                            "the checkpoint ticker was interrupted without being closed;"
                                    + " no further checkpoints will be written by this node",
                            maybeShutdown);
                }
                return;
            } catch (Throwable keepTicking) {
                // ⚠️ WITHOUT THIS THE T TRIGGER DIES SILENTLY AND PERMANENTLY,
                // and below K deltas per period the T trigger is ALL there is.
                LOG.log(System.Logger.Level.WARNING,
                        "a checkpoint tick failed; the next one still runs", keepTicking);
            }
            // ⚠️ COUNTED AFTER THE WORK, so a test that rendezvouses on this
            // cannot observe the tick before its effect.
            ticksProcessed.incrementAndGet();
        }
    }

    private synchronized void writeOnTick() {
        writeIfDirty();
    }

    private synchronized void writeIfDirty() {
        // ⚠️ `closed` IS CHECKED HERE, not only at the tick: a node that lost or
        // released its lease must not PUT into the chain's prefix at all.
        if (!dirty || closed) {
            return;
        }
        long sequence = pendingSequence;
        boolean written = false;
        try {
            // ⚠️ THE ENCODE IS INSIDE THE TRY, and an earlier version left it
            // out while the comment below claimed "every exit": an unchecked
            // throw from `encode` -- an OOME on a large offsets map is the
            // plausible one -- skipped the reset and put the counter back above
            // K, which is the rate defect that comment exists to prevent.
            Checkpoint checkpoint = new Checkpoint(sequence, pendingStreams, Map.copyOf(pods));
            byte[] body = checkpoint.encode();
            // ⚠️ `putIfAbsent`, which the M4 SPEC names for "the delta chain, the
            // seal and checkpoints".
            // ⚠️ AN EARLIER COMMENT JUSTIFIED IT AS STOPPING A FENCED LEADER
            // OVERWRITING ITS SUCCESSOR, WHICH IS IMPOSSIBLE: the epoch is
            // inside the key via `logPrefix()`, so leaders at E and E+1 cannot
            // collide at one key at all. What it actually buys is that the only
            // way to find the key occupied is THIS writer retrying a PUT that
            // reported IOException and had landed -- where the body is
            // USUALLY byte-identical. ⚠️ NOT ALWAYS, and an earlier comment
            // claimed otherwise: `pods.merge` runs in its own loop BEFORE the
            // capture, and `observe` swallows Throwable, so a throw between them
            // leaves `pods` advanced while `pendingSequence` has not moved. The
            // retried body then differs from the one that landed. Transient and
            // in the safe direction -- the landed body is older, and the next
            // successful checkpoint supersedes it -- but the empty Optional is
            // discarded, so this writer cannot tell "I wrote it" from "already
            // there".
            store.putIfAbsent(keys.checkpointKeyFor(sequence),
                    new Body(body.length, () -> new ByteArrayInputStream(body)));
            // ⚠️ THE POINTER IS WHAT MAKES DISCOVERY CONSTANT (ADR-0034), and it
            // carries the checkpoint's own BYTES rather than its sequence: a
            // pointer holding a number would be a second wire format to version
            // and golden-file, and this way discovery is one GET rather than two.
            // ⚠️ `put`, not `putIfAbsent`: this key is MEANT to be overwritten,
            // and it is the one object here that moves.
            store.put(keys.latestCheckpointKey(),
                    new Body(body.length, () -> new ByteArrayInputStream(body)));
            written = true;
            this.written.add(new ChainGc.CheckpointAt(log.epoch(), sequence));
            if (this.written.size() > MAX_WRITTEN) {
                // ⚠️ A NODE THAT NEVER HOLDS THE GC LEASE never collects, and
                // this would grow a checkpoint per interval for the term's
                // life. Past the cap the oldest stays in the bucket unjudged.
                this.written.remove(0);
            }
            newestWritten = checkpoint;
        } catch (IOException retryAtTheNextTrigger) {
            // ⚠️ THE DIRTY FLAG IS LEFT SET so a later trigger retries; clearing
            // it would wedge the writer until the next commit and, on a quiet
            // cluster, forever. ⚠️ THE DELTA COUNTER IS RESET ANYWAY, which is
            // the opposite call and deliberate: leaving it at or above K makes
            // EVERY subsequent commit issue another PUT inline, turning the rate
            // into one per delta -- a thousandfold at the shipped default --
            // exactly while the store is unhealthy.
            LOG.log(System.Logger.Level.WARNING,
                    "checkpoint at sequence " + sequence + " was not written; retrying at the"
                            + " next trigger", retryAtTheNextTrigger);
        } finally {
            // ⚠️ RESET ON EVERY EXIT, INCLUDING AN UNCHECKED THROW, and the
            // `finally` is why. An earlier version reset only in the IOException
            // branch, so a RuntimeException from the store escaped to `observe`'s
            // `catch (Throwable)` with the counter still at or above K -- and
            // then EVERY later commit issued another PUT inline. Measured by
            // review at 7 attempted PUTs for 9 deltas at K = 3, where 3 are
            // owed: the same rate defect the IOException branch was added to
            // remove, on its sibling path.
            deltasSinceCheckpoint = 0;
        }
        // ⚠️ ONLY A WRITE THAT LANDED CLEARS THE DIRTY FLAG, so a later trigger
        // retries; clearing it on failure wedges the writer until the next
        // commit and, on a quiet cluster, forever.
        if (written) {
            dirty = false;
        }
    }

    /**
     * The policy check, callable BEFORE a lease is acquired.
     *
     * <p>⚠️ SEPARATE FROM THE CONSTRUCTOR ON PURPOSE. {@code LocalSequencer}
     * starts its renewer thread before it builds this writer, so an
     * {@code IllegalArgumentException} thrown from the constructor escapes
     * {@code start}'s {@code catch (IOException)} with the lease HELD and being
     * renewed — and no node can ever take that term again.
     */
    static void checkPolicy(long everyDeltas, Duration everyInterval) {
        if (everyDeltas < 1) {
            throw new IllegalArgumentException("everyDeltas is at least 1: " + everyDeltas);
        }
        if (everyInterval == null || everyInterval.isZero() || everyInterval.isNegative()) {
            throw new IllegalArgumentException("everyInterval is positive: " + everyInterval);
        }
    }

    /** The newest checkpoint this writer wrote, or empty if it has written none. */
    synchronized java.util.Optional<Checkpoint> newestWritten() {
        return java.util.Optional.ofNullable(newestWritten);
    }

    /** Every checkpoint this writer wrote and chain GC has not collected, oldest first. */
    synchronized List<ChainGc.CheckpointAt> written() {
        return List.copyOf(written);
    }

    /** Forgets what chain GC deleted. */
    synchronized void collected(List<ChainGc.CheckpointAt> gone) {
        written.removeAll(gone);
    }

    /** How many T ticks the writer has finished PROCESSING, for a test to rendezvous on. */
    long ticksProcessed() {
        return ticksProcessed.get();
    }

    /**
     * Writes the uncheckpointed tail, then closes (M8.16).
     *
     * <p>⚠️ FOR A GRACEFUL RELEASE ONLY: the successor's recovery replays every
     * delta since the newest checkpoint before it commits anything, so a leader
     * that knows it is leaving writes that tail down while it still holds the
     * lease. A failed write, checked or not, does not stop the close: the
     * successor then replays, as after a crash.
     */
    synchronized void checkpointAndClose() {
        try {
            writeIfDirty();
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        closed = true;
        tickerThread.interrupt();
    }
}
