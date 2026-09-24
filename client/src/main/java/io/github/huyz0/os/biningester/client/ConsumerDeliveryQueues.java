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
    private final CatchUpDeliveryLane catchUp;
    private final Deque<ConsumerRecord> readyLive = new ArrayDeque<>();
    private final Deque<ConsumerRecord> readyCatchUp = new ArrayDeque<>();
    private final Decoder decoder;
    private int liveRecordsSinceCatchUp;
    private volatile boolean livePausedForGap;
    private volatile java.util.UUID gapReplayRequestId;

    ConsumerDeliveryQueues(int capacity, Decoder decoder) {
        live = new ArrayBlockingQueue<>(capacity);
        catchUp = new CatchUpDeliveryLane(capacity, deliveryLock, deliveryAvailable);
        this.decoder = decoder;
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
            boolean catchUpDue = liveRecordsSinceCatchUp >= ConsumerClient.LIVE_RECORD_QUANTUM;
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
        if (liveRecordsSinceCatchUp >= ConsumerClient.LIVE_RECORD_QUANTUM
                && tryLoadCatchUp(true)) {
            return;
        }
        if (!tryLoadLive(true)) {
            tryLoadCatchUp(true);
        }
    }

    private boolean loadAvailableDelivery(boolean globalPermitHeld) {
        if (liveRecordsSinceCatchUp >= ConsumerClient.LIVE_RECORD_QUANTUM
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
            boolean consumed = decoder.decode(delivery, readyLive, false);
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
        Delivery delivery;
        synchronized (deliveryLock) {
            if (!catchUp.tryAcquireDelivery()) {
                return false;
            }
            if (!globalPermitHeld && !deliveryAvailable.tryAcquire()) {
                catchUp.restoreDeliveryPermit();
                return false;
            }
            delivery = catchUp.poll();
            if (delivery == null) {
                catchUp.restoreDeliveryPermit();
                if (!globalPermitHeld) {
                    deliveryAvailable.release();
                }
                return false;
            }
        }
        decoder.decode(delivery, readyCatchUp, true);
        return true;
    }
}
