// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.format.Checkpoint;
import binjava.format.Checkpoint.StreamOffsets;
import binjava.format.RunKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
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
 *
 * <p>⚠️ PER-POD {@code lastAppliedFlushSeq} IS PROCESS-LOCAL, and that is a
 * known hole rather than a design: {@code CommitDelta} carries
 * {@code (sequence, segments)} and nothing else, so a new leader cannot recover
 * it and its first checkpoint carries no pods at all — emptying the idempotency
 * window M4.10 will depend on, exactly when duplicates are likeliest.
 * {@code CheckpointWriterContentTest} asserts that, to make M4.10 confront it
 * rather than inherit it.
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
    private final Map<String, Long> pods = new HashMap<>();
    private final AtomicLong ticksProcessed = new AtomicLong();
    private final Thread tickerThread;
    private long pendingSequence;
    private Map<RunKey, StreamOffsets> pendingStreams = Map.of();
    private long deltasSinceCheckpoint;
    private boolean dirty;
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
     * Every commit passes through here: it is the only place the writer can see
     * {@code podId} and {@code flushSeq}, which the chain does not carry.
     *
     * <p>⚠️ Does NOT throw. See the class note on failed checkpoints.
     */
    synchronized void observe(List<CommitRequest> requests) {
        try {
            capture(requests);
        } catch (Throwable neverFailAnAcknowledgedCommit) {
            // ⚠️ Throwable, not IOException, and for the reason `renewForever`
            // and `commitBatch` both catch it: these records are ALREADY durable
            // and acknowledged. An unchecked throw escaping here fails a whole
            // BatchingSequencer window for writes that succeeded.
            LOG.log(System.Logger.Level.WARNING,
                    "checkpoint bookkeeping failed; the commit itself stands",
                    neverFailAnAcknowledgedCommit);
        }
    }

    private void capture(List<CommitRequest> requests) {
        for (CommitRequest request : requests) {
            // ⚠️ Math::max, never put: a retried or late flush arrives with a
            // LOWER flushSeq than one already applied, and taking the last one
            // seen would walk the idempotency watermark backwards.
            pods.merge(request.podId(), request.flushSeq(), Math::max);
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
            // ⚠️ 0 is the TRUTHFUL oldestRetainedOffset until M7 gives retention
            // something to move it to; carried now because adding a field later
            // is a format change (ADR-0033).
            streams.put(stream.getKey(), new StreamOffsets(stream.getValue(), 0));
        }
        pendingStreams = streams;
        pendingSequence = log.nextSequence();
        deltasSinceCheckpoint++;
        dirty = true;
        if (deltasSinceCheckpoint >= everyDeltas) {
            writeIfDirty();
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

    /** How many T ticks the writer has finished PROCESSING, for a test to rendezvous on. */
    long ticksProcessed() {
        return ticksProcessed.get();
    }

    @Override
    public void close() {
        closed = true;
        tickerThread.interrupt();
    }
}
