// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/** Bounded live/replay queues with a finite live-service quantum. */
final class ConsumerDeliveryQueues {

    private final BlockingQueue<Delivery> live;
    private final Semaphore liveAvailable = new Semaphore(0);
    private final Semaphore deliveryAvailable = new Semaphore(0);
    private final Object deliveryLock = new Object();
    private final CatchUpDeliveryLane catchUp;
    private final Deque<ConsumerRecord> readyLive = new ArrayDeque<>();
    private final Deque<ConsumerRecord> readyCatchUp = new ArrayDeque<>();
    private final BiConsumer<Delivery, Deque<ConsumerRecord>> decoder;
    private int liveRecordsSinceCatchUp;

    ConsumerDeliveryQueues(int capacity,
            BiConsumer<Delivery, Deque<ConsumerRecord>> decoder) {
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

    void beginCatchUp(java.util.UUID requestId) {
        catchUp.begin(requestId);
    }

    void deliverCatchUp(java.util.UUID requestId, Delivery delivery)
            throws InterruptedException {
        catchUp.put(requestId, delivery);
    }

    boolean tryDeliverCatchUp(java.util.UUID requestId, Delivery delivery) {
        return catchUp.tryPut(requestId, delivery);
    }

    boolean completeCatchUp(java.util.UUID requestId) {
        return catchUp.end(requestId);
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
        if (!deliveryAvailable.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            return java.util.Optional.empty();
        }
        loadAfterWake();
        return java.util.Optional.ofNullable(nextReadyRecord());
    }

    private ConsumerRecord nextReadyRecord() {
        while (true) {
            boolean catchUpDue = liveRecordsSinceCatchUp >= ConsumerClient.LIVE_RECORD_QUANTUM;
            if (catchUpDue && readyCatchUp.isEmpty() && catchUp.queuedDeliveries() > 0) {
                tryLoadCatchUp(false);
            }
            if (!readyCatchUp.isEmpty() && (catchUpDue || !hasLivePending())) {
                liveRecordsSinceCatchUp = 0;
                return catchUp.handoff(readyCatchUp);
            }
            if (!readyLive.isEmpty()) {
                liveRecordsSinceCatchUp = Math.min(ConsumerClient.LIVE_RECORD_QUANTUM,
                        liveRecordsSinceCatchUp + 1);
                return readyLive.poll();
            }
            if (!readyCatchUp.isEmpty() && !hasLivePending()) {
                liveRecordsSinceCatchUp = 0;
                return catchUp.handoff(readyCatchUp);
            }
            if (!loadAvailableDelivery(false)) {
                return null;
            }
        }
    }

    private boolean hasLivePending() {
        return !readyLive.isEmpty() || !live.isEmpty();
    }

    private void loadAfterWake() {
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
        Delivery delivery;
        synchronized (deliveryLock) {
            if (!liveAvailable.tryAcquire()) {
                return false;
            }
            if (!globalPermitHeld && !deliveryAvailable.tryAcquire()) {
                liveAvailable.release();
                return false;
            }
            delivery = live.poll();
            if (delivery == null) {
                liveAvailable.release();
                if (!globalPermitHeld) {
                    deliveryAvailable.release();
                }
                return false;
            }
        }
        decoder.accept(delivery, readyLive);
        return true;
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
        decoder.accept(delivery, readyCatchUp);
        return true;
    }
}
