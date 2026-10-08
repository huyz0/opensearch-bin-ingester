// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.util.Objects;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

/** Bounded replay deliveries and the matching exchange's completion accounting. */
final class CatchUpDeliveryLane {

    private final BlockingQueue<Delivery> deliveries;
    private final Semaphore available = new Semaphore(0);
    private final Object deliveryLock;
    private final Semaphore deliveryAvailable;
    private UUID requestId;
    private int pendingRecords;
    private boolean endSeen;

    CatchUpDeliveryLane(int capacity, Object deliveryLock, Semaphore deliveryAvailable) {
        this(new ArrayBlockingQueue<>(capacity), deliveryLock, deliveryAvailable);
    }

    CatchUpDeliveryLane(BlockingQueue<Delivery> deliveries, Object deliveryLock,
            Semaphore deliveryAvailable) {
        this.deliveries = Objects.requireNonNull(deliveries, "deliveries");
        this.deliveryLock = deliveryLock;
        this.deliveryAvailable = deliveryAvailable;
    }

    synchronized void begin(UUID id) {
        Objects.requireNonNull(id, "requestId");
        if (requestId != null && !isComplete()) {
            throw new IllegalStateException("a catch-up exchange is already active");
        }
        requestId = id;
        pendingRecords = 0;
        endSeen = false;
    }

    void put(UUID id, Delivery delivery) throws InterruptedException {
        Objects.requireNonNull(delivery, "delivery");
        reserve(id, delivery.recordCount());
        boolean queued = false;
        try {
            deliveries.put(delivery);
            synchronized (deliveryLock) {
                available.release();
                deliveryAvailable.release();
            }
            queued = true;
        } finally {
            if (!queued) {
                rollback(delivery.recordCount());
            }
        }
    }

    boolean tryPut(UUID id, Delivery delivery) {
        Objects.requireNonNull(delivery, "delivery");
        reserve(id, delivery.recordCount());
        if (!deliveries.offer(delivery)) {
            rollback(delivery.recordCount());
            return false;
        }
        synchronized (deliveryLock) {
            available.release();
            deliveryAvailable.release();
        }
        return true;
    }

    private synchronized void reserve(UUID id, int records) {
        requireActive(id);
        if (endSeen) {
            throw new IllegalStateException("catch-up event received after matching end");
        }
        pendingRecords = Math.addExact(pendingRecords, records);
    }

    private synchronized void rollback(int records) {
        pendingRecords -= records;
    }

    synchronized boolean end(UUID id) {
        if (!matches(id)) {
            return false;
        }
        endSeen = true;
        return isComplete();
    }

    synchronized boolean complete(UUID id) {
        return matches(id) && isComplete();
    }

    synchronized ConsumerRecord handoff(Deque<ConsumerRecord> readyRecords) {
        if (pendingRecords <= 0) {
            throw new IllegalStateException("catch-up record accounting underflow");
        }
        ConsumerRecord record = readyRecords.poll();
        if (record == null) {
            throw new IllegalStateException("catch-up record queue was empty during handoff");
        }
        pendingRecords--;
        return record;
    }

    /**
     * Releases what {@code taken} reserved and will never hand off: a void's
     * offsets, which decode to no record (M13.25e). ⚠️ AFTER ITS COMMIT, never
     * at reservation: released early, the exchange completed before the void
     * was committed, and the live tail met the hole as a gap again.
     */
    synchronized void settle(Delivery taken) {
        if (taken.voided()) {
            if (pendingRecords < taken.recordCount()) {
                throw new IllegalStateException("catch-up record accounting underflow");
            }
            pendingRecords -= taken.recordCount();
        }
    }

    int queuedDeliveries() {
        return deliveries.size();
    }

    boolean tryAcquireDelivery() {
        return available.tryAcquire();
    }

    void restoreDeliveryPermit() {
        available.release();
    }

    Delivery peek() {
        return deliveries.peek();
    }

    Delivery poll() {
        return deliveries.poll();
    }

    private boolean matches(UUID id) {
        return requestId != null && requestId.equals(id);
    }

    private void requireActive(UUID id) {
        Objects.requireNonNull(id, "requestId");
        if (!matches(id)) {
            throw new IllegalArgumentException("delivery does not match the active catch-up request");
        }
    }

    private boolean isComplete() {
        return endSeen && pendingRecords == 0;
    }
}
