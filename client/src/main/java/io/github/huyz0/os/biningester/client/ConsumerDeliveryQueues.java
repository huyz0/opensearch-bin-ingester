// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Bounded live/replay queues with a finite live-service quantum. */
final class ConsumerDeliveryQueues {

    @FunctionalInterface
    interface Decoder {
        boolean decode(Delivery delivery, Deque<ConsumerRecord> out, boolean replay);
    }

    private final BlockingQueue<Delivery> live;
    private final Semaphore liveAvailable = new Semaphore(0);
    private final Semaphore deliveryAvailable = new Semaphore(0);
    private final Semaphore gapReplayAvailable = new Semaphore(0);
    private final Object deliveryLock = new Object();
    private final Object liveDecodeLock = new Object();
    private final Object catchUpDecodeLock = new Object();
    private final CatchUpDeliveryLane catchUp;
    private final Deque<ConsumerRecord> readyLive = new ArrayDeque<>();
    private final Deque<ConsumerRecord> readyCatchUp = new ArrayDeque<>();
    private final Decoder decoder;
    /** Whether the catch-up lane is backing off and not yet due (M12.26). */
    private final java.util.function.BooleanSupplier catchUpBackingOff;
    private int liveRecordsSinceCatchUp;
    private volatile boolean livePausedForGap;
    private volatile java.util.UUID gapReplayRequestId;

    /**
     * ⚠️ THE ONLY CONSTRUCTOR (M13.6a, M12 harvest R5): a two-argument one
     * defaulted the catch-up lane to never backing off, silently dropping
     * M12.26's quantum hand-over for any queue built through it.
     */
    ConsumerDeliveryQueues(int capacity, Decoder decoder,
            java.util.function.BooleanSupplier catchUpBackingOff) {
        live = new ArrayBlockingQueue<>(capacity);
        catchUp = new CatchUpDeliveryLane(capacity, deliveryLock, deliveryAvailable);
        this.decoder = decoder;
        this.catchUpBackingOff = catchUpBackingOff;
    }

    /**
     * Whether the catch-up lane's quantum turn has come: the live quantum is
     * served, and the catch-up is not backing off.
     *
     * <p>⚠️ A BACKING-OFF CATCH-UP GIVES ITS TURN BACK TO LIVE (M12.26): its
     * backoff otherwise escaped {@code readNext} before live was tried, and
     * live waited out every catch-up backoff until the catch-up gave up. It
     * keeps its turn: the next read once it is due loads it first.
     */
    private boolean catchUpTurn() {
        return liveRecordsSinceCatchUp >= ConsumerClient.LIVE_RECORD_QUANTUM
                && !catchUpBackingOff.getAsBoolean();
    }

    boolean deliverLive(Delivery delivery) {
        synchronized (deliveryLock) {
            boolean queued = live.offer(delivery);
            if (queued) {
                liveAvailable.release();
                deliveryAvailable.release();
            }
            return queued;
        }
    }

    void pauseLiveForGap() {
        synchronized (deliveryLock) {
            livePausedForGap = true;
        }
    }

    void resumeLiveAfterGap() {
        synchronized (deliveryLock) {
            livePausedForGap = false;
        }
    }

    void beginCatchUp(java.util.UUID requestId, boolean gapRepair) {
        catchUp.begin(requestId);
        if (gapRepair) {
            gapReplayRequestId = requestId;
        }
    }

    void deliverCatchUp(java.util.UUID requestId, Delivery delivery)
            throws InterruptedException {
        catchUp.put(requestId, delivery);
        if (requestId.equals(gapReplayRequestId)) {
            gapReplayAvailable.release();
        }
    }

    boolean tryDeliverCatchUp(java.util.UUID requestId, Delivery delivery) {
        boolean queued = catchUp.tryPut(requestId, delivery);
        if (queued && requestId.equals(gapReplayRequestId)) {
            gapReplayAvailable.release();
        }
        return queued;
    }

    boolean completeCatchUp(java.util.UUID requestId) {
        boolean complete = catchUp.end(requestId);
        if (requestId.equals(gapReplayRequestId)) {
            gapReplayAvailable.release();
        }
        if (complete && requestId.equals(gapReplayRequestId)) {
            gapReplayRequestId = null;
        }
        return complete;
    }

    boolean catchUpComplete(java.util.UUID requestId) {
        return catchUp.complete(requestId);
    }

    int queuedLiveDeliveries() {
        return live.size();
    }

    java.util.Optional<ConsumerRecord> readNext(Duration timeout) throws InterruptedException {
        ConsumerRecord record = nextReadyRecord();
        if (record != null) {
            return java.util.Optional.of(record);
        }
        if (livePausedForGap) {
            if (!gapReplayAvailable.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                return java.util.Optional.empty();
            }
            return java.util.Optional.ofNullable(nextReadyRecord());
        }
        if (!deliveryAvailable.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            return java.util.Optional.empty();
        }
        loadAfterWake();
        return java.util.Optional.ofNullable(nextReadyRecord());
    }

    private ConsumerRecord nextReadyRecord() {
        while (true) {
            if (livePausedForGap) {
                if (readyCatchUp.isEmpty() && catchUp.queuedDeliveries() > 0) {
                    tryLoadCatchUp(false);
                }
                if (!readyCatchUp.isEmpty()) {
                    return handoffCatchUp();
                }
                return null;
            }
            if (gapReplayRequestId != null) {
                if (readyCatchUp.isEmpty() && catchUp.queuedDeliveries() > 0) {
                    tryLoadCatchUp(false);
                }
                if (!readyCatchUp.isEmpty()) {
                    return handoffCatchUp();
                }
                if (catchUp.complete(gapReplayRequestId)) {
                    gapReplayRequestId = null;
                } else {
                    return null;
                }
            }
            boolean catchUpDue = catchUpTurn();
            if (catchUpDue && readyCatchUp.isEmpty() && catchUp.queuedDeliveries() > 0) {
                tryLoadCatchUp(false);
            }
            if (!readyCatchUp.isEmpty() && (catchUpDue || !hasLivePending())) {
                liveRecordsSinceCatchUp = 0;
                return handoffCatchUp();
            }
            if (!readyLive.isEmpty()) {
                liveRecordsSinceCatchUp = Math.min(ConsumerClient.LIVE_RECORD_QUANTUM,
                        liveRecordsSinceCatchUp + 1);
                return readyLive.poll();
            }
            if (!readyCatchUp.isEmpty() && !hasLivePending()) {
                liveRecordsSinceCatchUp = 0;
                return handoffCatchUp();
            }
            if (!loadAvailableDelivery(false)) {
                return null;
            }
        }
    }

    private ConsumerRecord handoffCatchUp() {
        ConsumerRecord handed = catchUp.handoff(readyCatchUp);
        java.util.UUID gapRequest = gapReplayRequestId;
        if (gapRequest != null && catchUp.complete(gapRequest)) {
            gapReplayRequestId = null;
        }
        return handed;
    }

    private boolean hasLivePending() {
        return !readyLive.isEmpty() || !live.isEmpty();
    }

    private void loadAfterWake() {
        if (livePausedForGap) {
            if (!tryLoadCatchUp(true)) {
                // The wake may have belonged to a queued live delivery. Keep
                // its shared permit available until the gap replay releases it.
                deliveryAvailable.release();
            }
            return;
        }
        if (catchUpTurn() && tryLoadCatchUp(true)) {
            return;
        }
        if (!tryLoadLive(true)) {
            tryLoadCatchUp(true);
        }
    }

    private boolean loadAvailableDelivery(boolean globalPermitHeld) {
        if (catchUpTurn()
                && (readyCatchUp.size() > 0 || catchUp.queuedDeliveries() > 0)
                && tryLoadCatchUp(globalPermitHeld)) {
            return true;
        }
        return tryLoadLive(globalPermitHeld) || tryLoadCatchUp(globalPermitHeld);
    }

    private boolean tryLoadLive(boolean globalPermitHeld) {
        synchronized (liveDecodeLock) {
            Delivery delivery;
            synchronized (deliveryLock) {
                if (!liveAvailable.tryAcquire()) {
                    return false;
                }
                if (!globalPermitHeld && !deliveryAvailable.tryAcquire()) {
                    liveAvailable.release();
                    return false;
                }
                delivery = live.peek();
                if (delivery == null) {
                    liveAvailable.release();
                    deliveryAvailable.release();
                    return false;
                }
            }
            boolean consumed;
            try {
                consumed = decoder.decode(delivery, readyLive, false);
            } catch (RuntimeException failed) {
                // ⚠️ M10.23, THE M10.2 REVIEW's R3: A THROWING DECODE RETURNS
                // ITS PERMITS. The delivery is still at the head, and without
                // them nothing could reach it again -- the next poll would be
                // an ordinary empty poll over a queue that is not empty. So a
                // fetch that will be retried, and a corrupt segment the caller
                // resumes past the pause of, are both read again from here.
                synchronized (deliveryLock) {
                    liveAvailable.release();
                    deliveryAvailable.release();
                }
                throw failed;
            }
            synchronized (deliveryLock) {
                if (consumed) {
                    if (live.poll() != delivery) {
                        throw new IllegalStateException("live delivery queue head changed");
                    }
                } else {
                    // The gap-triggering delivery remains at the head until the
                    // coordinator resumes this lane after replay is complete.
                    liveAvailable.release();
                    deliveryAvailable.release();
                }
            }
            return true;
        }
    }

    private boolean tryLoadCatchUp(boolean globalPermitHeld) {
        // ⚠️ ONE DECODE OF THE HEAD AT A TIME, now that the head stays queued
        // while it decodes: two readers would otherwise both peek it.
        synchronized (catchUpDecodeLock) {
            return loadCatchUpHead(globalPermitHeld);
        }
    }

    private boolean loadCatchUpHead(boolean globalPermitHeld) {
        Delivery delivery;
        synchronized (deliveryLock) {
            if (!catchUp.tryAcquireDelivery()) {
                return false;
            }
            if (!globalPermitHeld && !deliveryAvailable.tryAcquire()) {
                catchUp.restoreDeliveryPermit();
                return false;
            }
            delivery = catchUp.peek();
            if (delivery == null) {
                catchUp.restoreDeliveryPermit();
                if (!globalPermitHeld) {
                    deliveryAvailable.release();
                }
                return false;
            }
        }
        // ⚠️ PEEKED, AND TAKEN ONLY ONCE IT DECODED (M10.23), as the live lane
        // does: polled first, a replay delivery whose fetch failed was gone,
        // and its records with it.
        try {
            decoder.decode(delivery, readyCatchUp, true);
        } catch (RuntimeException failed) {
            synchronized (deliveryLock) {
                catchUp.restoreDeliveryPermit();
                deliveryAvailable.release();
            }
            throw failed;
        }
        synchronized (deliveryLock) {
            if (catchUp.poll() != delivery) {
                throw new IllegalStateException("catch-up delivery queue head changed");
            }
        }
        return true;
    }
}
