// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import io.github.huyz0.os.biningester.format.DeltaPushFrame;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/** The leaseholder's fan-out: push within its AZ, hint across (M10.19, ADR-0075). */
class DeltaFanOutTest {

    private static final int PORT = 8080;
    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 0);

    /** Self is a1; a2 shares its AZ; b and c each have two ready pods. */
    private static final List<EndpointSliceView.Endpoint> FLEET = List.of(
            new EndpointSliceView.Endpoint("a1", "10.0.0.1", "az-a"),
            new EndpointSliceView.Endpoint("a2", "10.0.0.2", "az-a"),
            new EndpointSliceView.Endpoint("b2", "10.0.1.2", "az-b"),
            new EndpointSliceView.Endpoint("b1", "10.0.1.1", "az-b"),
            new EndpointSliceView.Endpoint("c9", "10.0.2.9", "az-c"),
            new EndpointSliceView.Endpoint("c3", "10.0.2.3", "az-c"));

    private record Sent(String endpoint, String path, byte[] body) {
    }

    private record Local(long epoch, long sequence) {
    }

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, "bins/cluster-a/data/s" + sequence + ".bseg",
                List.of(new RunCommit(KEY, 2, 2 * sequence)));
    }

    private static void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void aDeltaIsPublishedLocallyPushedWithinTheAzAndHintedToEachRemoteRelay() throws Exception {
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        List<Sent> sent = new CopyOnWriteArrayList<>();
        List<Local> local = new CopyOnWriteArrayList<>();
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, crossAz, () -> FLEET,
                (delta, epoch) -> local.add(new Local(epoch, delta.sequence())),
                (endpoint, path, body) -> sent.add(new Sent(endpoint, path, body)),
                Duration.ofMillis(1))) {
            fanOut.committed(delta(7), 3);
            await(() -> fanOut.delivered() == 3, "three frames");

            assertThat(local).containsExactly(new Local(3, 7));
            assertThat(sent).extracting(Sent::endpoint).containsExactlyInAnyOrder(
                    "http://10.0.0.2:8080", "http://10.0.1.1:8080", "http://10.0.2.3:8080");
            Sent push = sent.stream().filter(s -> s.endpoint().contains("10.0.0.2")).findFirst()
                    .orElseThrow();
            assertThat(push.path()).isEqualTo(DeltaFanOut.PUSH_PATH);
            DeltaPushFrame pushed = DeltaPushFrame.decode(push.body());
            assertThat(pushed.epoch()).isEqualTo(3);
            assertThat(pushed.delta().encode()).isEqualTo(delta(7).encode());
            for (Sent hint : sent) {
                if (hint != push) {
                    assertThat(hint.path()).isEqualTo(DeltaFanOut.HINT_PATH);
                    assertThat(DeltaHintFrame.decode(hint.body()))
                            .isEqualTo(new DeltaHintFrame(3, 7));
                }
            }
            assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.DELTA_PUSH))
                    .as("a push never leaves its AZ").isZero();
            assertThat(crossAz.sameAzBytes(CrossAzBytes.Transport.DELTA_PUSH))
                    .isEqualTo(push.body().length);
            assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.DELTA_HINT))
                    .as("24 bytes per remote AZ").isEqualTo(2L * DeltaHintFrame.BYTES);
        }
    }

    @Test
    void aFailingPeerIsRetriedInOrderAndOthersAreNotHeldUp() throws Exception {
        List<Sent> sent = new CopyOnWriteArrayList<>();
        // ⚠️ ONLY THE FIRST FRAME FAILS, twice, and only once all three are
        // queued behind it: a lane that moved a failed frame to the back
        // would deliver 1, 2, 0 -- and the peer's publisher drops 0 as stale.
        AtomicInteger firstFailures = new AtomicInteger(2);
        java.util.concurrent.CountDownLatch allQueued = new java.util.concurrent.CountDownLatch(1);
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> FLEET, (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    if (endpoint.contains("10.0.0.2")) {
                        try {
                            allQueued.await();
                        } catch (InterruptedException e) {
                            throw new IOException(e);
                        }
                        if (decodePush(body).delta().sequence() == 0
                                && firstFailures.getAndDecrement() > 0) {
                            throw new IOException("a2 is restarting");
                        }
                    }
                    sent.add(new Sent(endpoint, path, body));
                },
                Duration.ofMillis(1))) {
            for (long sequence = 0; sequence < 3; sequence++) {
                fanOut.committed(delta(sequence), 1);
            }
            allQueued.countDown();
            await(() -> fanOut.delivered() == 9, "every frame, after the retries");

            List<Long> toA2 = sent.stream().filter(s -> s.endpoint().contains("10.0.0.2"))
                    .map(s -> decodePush(s.body()).delta().sequence()).toList();
            assertThat(toA2).as("in commit order, the first after two failed attempts")
                    .containsExactly(0L, 1L, 2L);
            assertThat(fanOut.dropped()).isZero();
        }
    }

    @Test
    void aFrameIsRetriedUntilItsPeerLeavesTheReadySetThenDroppedAndCounted() throws Exception {
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        AtomicInteger attempts = new AtomicInteger();
        List<Long> toA3 = new CopyOnWriteArrayList<>();
        EndpointSliceView.Endpoint a3 = new EndpointSliceView.Endpoint("a3", "10.0.0.3", "az-a");
        List<List<EndpointSliceView.Endpoint>> members =
                new CopyOnWriteArrayList<>(List.of(FLEET.subList(0, 2)));
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, crossAz,
                () -> members.get(0), (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    if (endpoint.contains("10.0.0.2")) {
                        attempts.incrementAndGet();
                        throw new IOException("a2 never answers");
                    }
                    toA3.add(decodePush(body).delta().sequence());
                },
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            await(() -> attempts.get() >= 8, "more attempts than any fixed budget would make");
            assertThat(fanOut.dropped()).as("still ready: still trying").isZero();

            members.set(0, List.of(FLEET.get(0), a3)); // a2 left; a3 joined
            fanOut.committed(delta(1), 1);
            await(() -> fanOut.dropped() == 1, "a2's frame, dropped when it left");

            await(() -> toA3.size() == 1, "the pod that joined");
            assertThat(toA3).containsExactly(1L);
            assertThat(fanOut.lanes()).as("the departed peer's lane is gone").isEqualTo(1);
            long frame = new DeltaPushFrame(1, delta(0)).encode().length;
            assertThat(crossAz.sameAzBytes(CrossAzBytes.Transport.DELTA_PUSH))
                    .as("every attempt crossed the wire and is counted")
                    .isGreaterThanOrEqualTo(8 * frame);
        }
    }

    @Test
    void closeDeliversWhatTheLanesHoldAndCountsWhatItCannot() throws Exception {
        List<Long> toA2 = new CopyOnWriteArrayList<>();
        List<EndpointSliceView.Endpoint> threeInAzA = List.of(FLEET.get(0), FLEET.get(1),
                new EndpointSliceView.Endpoint("a4", "10.0.0.4", "az-a"));
        DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> threeInAzA, (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    if (endpoint.contains("10.0.0.4")) {
                        throw new IOException("a4 never answers");
                    }
                    toA2.add(decodePush(body).delta().sequence());
                },
                Duration.ofMillis(1));
        for (long sequence = 0; sequence < 5; sequence++) {
            fanOut.committed(delta(sequence), 1);
        }
        long started = System.nanoTime();
        fanOut.close();

        assertThat(toA2).as("a healthy peer gets everything queued, in order")
                .containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(fanOut.dropped()).as("a4's five, counted rather than lost silently")
                .isEqualTo(5);
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
                .as("bounded by the close wait, plus the stop")
                .isLessThan(2 * DeltaFanOut.CLOSE_WAIT.toMillis() + 1000);
        fanOut.committed(delta(9), 1);
        assertThat(toA2).as("nothing is sent after close").hasSize(5);
        assertThat(fanOut.dropped()).as("and the two peers it would have gone to are counted")
                .isEqualTo(7);
        assertThat(fanOut.lanes()).as("no lane outlives close").isZero();
    }

    @Test
    void aFullLaneRefusesAndCountsRatherThanBlockingTheCommit() throws Exception {
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        List<EndpointSliceView.Endpoint> pair = FLEET.subList(0, 2);
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> pair, (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                },
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue(); // one frame in flight
            for (long sequence = 1; sequence <= DeltaFanOut.LANE_DEPTH + 2; sequence++) {
                fanOut.committed(delta(sequence), 1);
            }

            assertThat(fanOut.overflowed()).as("the lane holds LANE_DEPTH; two more are refused")
                    .isEqualTo(2);
            release.countDown();
        }
    }

    @Test
    void aLaneIsBoundedInBytesAsWellAsFrames() throws Exception {
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        List<RunCommit> runs = new java.util.ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            runs.add(new RunCommit(new RunKey(UUID.randomUUID(), 0), 1, i));
        }
        List<EndpointSliceView.Endpoint> pair = FLEET.subList(0, 2);
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> pair, (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                },
                Duration.ofMillis(1))) {
            CommitDelta first = new CommitDelta(0, "bins/cluster-a/data/big.bseg", runs);
            long frame = new DeltaPushFrame(1, first).encode().length;
            assertThat(frame).as("the premise: a large delta").isGreaterThan(256 * 1024);
            fanOut.committed(first, 1);
            assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue();
            int offered = 200;
            for (int i = 1; i <= offered; i++) {
                fanOut.committed(new CommitDelta(i, "bins/cluster-a/data/big.bseg", runs), 1);
            }

            long fits = DeltaFanOut.LANE_BYTES / frame;
            assertThat(fanOut.overflowed()).as("far fewer than LANE_DEPTH frames of this size")
                    .isEqualTo(offered - fits);
            release.countDown();
        }
    }

    @Test
    void aDeltaTooLargeToPushIsStillHintedAndIsCounted() throws Exception {
        List<Sent> sent = new CopyOnWriteArrayList<>();
        List<Local> local = new CopyOnWriteArrayList<>();
        List<RunCommit> runs = new java.util.ArrayList<>();
        for (int i = 0; i < 500_000; i++) {
            runs.add(new RunCommit(new RunKey(KEY.indexId(), i), 1, i));
        }
        CommitDelta huge = new CommitDelta(3, "bins/cluster-a/data/huge.bseg", runs);
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> FLEET, (delta, epoch) -> local.add(new Local(epoch, delta.sequence())),
                (endpoint, path, body) -> sent.add(new Sent(endpoint, path, body)),
                Duration.ofMillis(1))) {
            fanOut.committed(huge, 2);
            await(() -> fanOut.delivered() == 2, "the two hints");

            assertThat(local).containsExactly(new Local(2, 3));
            assertThat(sent).extracting(Sent::path).containsOnly(DeltaFanOut.HINT_PATH);
            assertThat(fanOut.unpushable()).isEqualTo(1);
        }
    }

    @Test
    void aPeerThatLeavesIsNoticedWithoutAnotherCommit() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<List<EndpointSliceView.Endpoint>> members =
                new CopyOnWriteArrayList<>(List.of(FLEET.subList(0, 2)));
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> members.get(0), (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    attempts.incrementAndGet();
                    throw new IOException("a2 is gone");
                },
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            await(() -> attempts.get() >= 3, "a few attempts");
            members.set(0, List.of(FLEET.get(0))); // a2 left; nothing else commits

            await(() -> fanOut.dropped() == 1, "the frame, dropped once a2 is gone");
            int settled = attempts.get();
            await(() -> fanOut.dropped() == 1 && attempts.get() == settled, "no more attempts");
        }
    }

    @Test
    void anUnpushableDeltaLeavesAStillReadyPeersBacklogAlone() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        List<Long> toA2 = new CopyOnWriteArrayList<>();
        List<RunCommit> runs = new java.util.ArrayList<>();
        for (int i = 0; i < 500_000; i++) {
            runs.add(new RunCommit(new RunKey(KEY.indexId(), i), 1, i));
        }
        List<EndpointSliceView.Endpoint> pair = FLEET.subList(0, 2);
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> pair, (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    toA2.add(decodePush(body).delta().sequence());
                },
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            fanOut.committed(delta(1), 1);
            fanOut.committed(new CommitDelta(2, "bins/cluster-a/data/huge.bseg", runs), 1);
            release.countDown();
            await(() -> toA2.size() == 2, "the backlog, still delivered");

            assertThat(toA2).containsExactly(0L, 1L);
            assertThat(fanOut.unpushable()).isEqualTo(1);
            assertThat(fanOut.dropped()).isZero();
        }
    }

    @Test
    void aPeerThatLeavesLosesItsWholeQueueAtOnceNotOneAttemptPerFrame() throws Exception {
        List<Long> attempted = new CopyOnWriteArrayList<>();
        java.util.concurrent.CountDownLatch allQueued = new java.util.concurrent.CountDownLatch(1);
        List<List<EndpointSliceView.Endpoint>> members =
                new CopyOnWriteArrayList<>(List.of(FLEET.subList(0, 2)));
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> members.get(0), (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    try {
                        allQueued.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    attempted.add(decodePush(body).delta().sequence());
                    throw new IOException("a2 is gone");
                },
                Duration.ofMillis(1))) {
            for (long sequence = 0; sequence < 3; sequence++) {
                fanOut.committed(delta(sequence), 1);
            }
            members.set(0, List.of(FLEET.get(0))); // a2 left; nothing else commits
            allQueued.countDown();

            await(() -> fanOut.dropped() == 3, "all three, once a2 is gone");
            assertThat(attempted).as("nothing behind the first is sent to a departed peer")
                    .containsOnly(0L);
        }
    }

    @Test
    void aRemovedLaneIsStoppedSoAReturningPeerHasOneSender() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        List<Long> toA2 = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        List<List<EndpointSliceView.Endpoint>> members =
                new CopyOnWriteArrayList<>(List.of(FLEET.subList(0, 2)));
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> members.get(0), (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    if (calls.getAndIncrement() == 0) {
                        try {
                            release.await(); // the first frame hangs in flight
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                    toA2.add(decodePush(body).delta().sequence());
                },
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            await(() -> calls.get() == 1, "the first frame in flight");
            members.set(0, List.of(FLEET.get(0))); // a2 leaves: its lane is removed
            fanOut.committed(delta(1), 1);
            members.set(0, FLEET.subList(0, 2)); // and comes back: a new lane
            fanOut.committed(delta(2), 1);
            await(() -> toA2.contains(2L), "the new lane's frame");
            release.countDown();
            fanOut.committed(delta(3), 1);
            await(() -> toA2.contains(3L), "the next frame on the new lane");

            assertThat(toA2).as("the removed lane was stopped: its frame never lands late")
                    .containsExactly(2L, 3L);
            assertThat(fanOut.dropped()).isEqualTo(1);
        }
    }

    @Test
    void aLaneBacksOffDoublingToTheCap() throws Exception {
        List<Long> pauses = new CopyOnWriteArrayList<>();
        AtomicInteger failures = new AtomicInteger(8);
        List<EndpointSliceView.Endpoint> pair = FLEET.subList(0, 2);
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> pair, (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    if (failures.getAndDecrement() > 0) {
                        throw new IOException("not yet");
                    }
                },
                Duration.ofMillis(100), pauses::add)) {
            fanOut.committed(delta(0), 1);
            await(() -> fanOut.delivered() == 1, "delivery after eight failures");

            assertThat(pauses).containsExactly(100L, 200L, 400L, 800L, 1600L, 2000L, 2000L, 2000L);
        }
    }

    @Test
    void aRelaySupersededByALowerPodIdDropsItsQueueWithoutAnotherCommit() throws Exception {
        List<String> hinted = new CopyOnWriteArrayList<>();
        EndpointSliceView.Endpoint b1 = new EndpointSliceView.Endpoint("b1", "10.0.1.1", "az-b");
        EndpointSliceView.Endpoint b0 = new EndpointSliceView.Endpoint("b0", "10.0.1.0", "az-b");
        List<List<EndpointSliceView.Endpoint>> members =
                new CopyOnWriteArrayList<>(List.of(List.of(FLEET.get(0), b1)));
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> members.get(0), (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    hinted.add(endpoint);
                    if (endpoint.contains("10.0.1.1")) {
                        throw new IOException("b1 refuses: it is not the relay any more");
                    }
                },
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            fanOut.committed(delta(1), 1);
            await(() -> !hinted.isEmpty(), "the first attempt at b1");
            members.set(0, List.of(FLEET.get(0), b1, b0)); // b0 is now az-b's relay; b1 is still ready

            await(() -> fanOut.dropped() == 2, "b1's queue, dropped without a commit");
            int settled = hinted.size();
            fanOut.committed(delta(2), 1);
            await(() -> hinted.size() > settled, "the next hint");
            assertThat(hinted.get(hinted.size() - 1)).as("to the new relay")
                    .isEqualTo("http://10.0.1.0:8080");
        }
    }

    @Test
    void anUncheckedFailureFromThePeerIsRetriedLikeAnyOther() throws Exception {
        AtomicInteger failures = new AtomicInteger(2);
        List<Long> toA2 = new CopyOnWriteArrayList<>();
        List<EndpointSliceView.Endpoint> pair = FLEET.subList(0, 2);
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> pair, (delta, epoch) -> { },
                (endpoint, path, body) -> {
                    if (failures.getAndDecrement() > 0) {
                        throw new IllegalStateException("the client pool broke");
                    }
                    toA2.add(decodePush(body).delta().sequence());
                },
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            await(() -> toA2.size() == 1, "delivery after two unchecked failures");
            assertThat(fanOut.dropped()).isZero();
        }
    }

    @Test
    void closeReturnsAsSoonAsHealthyLanesAreDrained() throws Exception {
        List<EndpointSliceView.Endpoint> pair = FLEET.subList(0, 2);
        List<Long> toA2 = new CopyOnWriteArrayList<>();
        DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> pair, (delta, epoch) -> { },
                (endpoint, path, body) -> toA2.add(decodePush(body).delta().sequence()),
                Duration.ofMillis(1));
        fanOut.committed(delta(0), 1);
        long started = System.nanoTime();
        fanOut.close();

        assertThat(toA2).containsExactly(0L);
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
                .as("no healthy lane waits out the close bound")
                .isLessThan(DeltaFanOut.CLOSE_WAIT.toMillis());
    }

    @Test
    void aRelayedDeltaIsPublishedAndPushedWithinTheAzButNeverHintedOnward() throws Exception {
        List<Sent> sent = new CopyOnWriteArrayList<>();
        List<Local> local = new CopyOnWriteArrayList<>();
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, crossAz, () -> FLEET,
                (delta, epoch) -> local.add(new Local(epoch, delta.sequence())),
                (endpoint, path, body) -> sent.add(new Sent(endpoint, path, body)),
                Duration.ofMillis(1))) {
            fanOut.relayed(delta(5), 2);
            await(() -> fanOut.delivered() == 1, "the one same-AZ push");

            assertThat(local).containsExactly(new Local(2, 5));
            assertThat(sent).extracting(Sent::endpoint).containsExactly("http://10.0.0.2:8080");
            assertThat(sent).extracting(Sent::path).containsOnly(DeltaFanOut.PUSH_PATH);
            assertThat(crossAz.crossAzBytes()).as("nothing leaves the AZ").isZero();
            assertThat(fanOut.lanes()).as("no lane to a remote relay is even opened").isEqualTo(1);
        }
    }

    @Test
    void theBackoffDoublesAndIsCapped() {
        assertThat(DeltaFanOut.nextBackoff(100)).isEqualTo(200);
        assertThat(DeltaFanOut.nextBackoff(1500)).isEqualTo(DeltaFanOut.MAX_BACKOFF.toMillis());
        assertThat(DeltaFanOut.nextBackoff(DeltaFanOut.MAX_BACKOFF.toMillis()))
                .isEqualTo(DeltaFanOut.MAX_BACKOFF.toMillis());
    }

    @Test
    void thePortMustBeAPort() {
        for (int port : new int[] {0, 65_536}) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> new DeltaFanOut("a1", "az-a",
                    port, new CrossAzBytes("az-a"), List::of, (delta, epoch) -> { },
                    (endpoint, path, body) -> { }, Duration.ofMillis(1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (int port : new int[] {1, 65_535}) {
            new DeltaFanOut("a1", "az-a", port, new CrossAzBytes("az-a"), List::of,
                    (delta, epoch) -> { }, (endpoint, path, body) -> { }, Duration.ofMillis(1))
                    .close();
        }
    }

    @Test
    void theRelayIsTheLowestReadyPodIdOfItsAz() {
        assertThat(DeltaFanOut.relayOf(FLEET, "az-b")).map(EndpointSliceView.Endpoint::podId)
                .contains("b1");
        assertThat(DeltaFanOut.relayOf(FLEET, "az-c")).map(EndpointSliceView.Endpoint::podId)
                .contains("c3");
        assertThat(DeltaFanOut.relayOf(FLEET, "az-z")).isEmpty();
    }

    @Test
    void aPeerThatLeftTheMembershipIsNoLongerSentTo() throws Exception {
        Map<String, List<Long>> perPeer = new ConcurrentHashMap<>();
        EndpointSliceView.Endpoint a3 = new EndpointSliceView.Endpoint("a3", "10.0.0.3", "az-a");
        List<List<EndpointSliceView.Endpoint>> members =
                new CopyOnWriteArrayList<>(List.of(FLEET.subList(0, 2)));
        try (DeltaFanOut fanOut = new DeltaFanOut("a1", "az-a", PORT, new CrossAzBytes("az-a"),
                () -> members.get(0), (delta, epoch) -> { },
                (endpoint, path, body) -> perPeer
                        .computeIfAbsent(endpoint, e -> new CopyOnWriteArrayList<>())
                        .add(decodePush(body).delta().sequence()),
                Duration.ofMillis(1))) {
            fanOut.committed(delta(0), 1);
            await(() -> fanOut.delivered() == 1, "the first push");
            members.set(0, List.of(FLEET.get(0), a3)); // a2 left; a3 joined
            fanOut.committed(delta(1), 1);
            fanOut.committed(delta(2), 1);
            await(() -> fanOut.delivered() == 3, "both pushes to the pod that joined");

            assertThat(perPeer.get("http://10.0.0.3:8080")).containsExactly(1L, 2L);
            assertThat(perPeer.get("http://10.0.0.2:8080"))
                    .as("nothing after it left").containsExactly(0L);
            assertThat(fanOut.lanes()).as("and its lane is gone").isEqualTo(1);
        }
    }

    private static DeltaPushFrame decodePush(byte[] body) {
        try {
            return DeltaPushFrame.decode(body);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
