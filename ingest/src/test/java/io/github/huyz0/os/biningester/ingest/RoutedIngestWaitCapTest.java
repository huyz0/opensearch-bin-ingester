// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M13.11 (M12 harvest R9): the cap on explicit-partition writes waiting for an
 * unregistered index's registration, pinned where M12.10's review left it
 * open. The count is kept per index (T2), bounded across every name as well
 * as per name (P3), leaves no entry behind (T1), and gives a slot back when
 * the wait is interrupted (T3).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RoutedIngestWaitCapTest {

    private static final Principal PRINCIPAL = new Principal("cluster-a", "producer-1",
            Set.of("a", "b", "c", "d", "e"));

    private final IndexCatalog catalog = new IndexCatalog();
    private final RoutedIngest routed = new RoutedIngest(new Accepting(), catalog,
            new PendingPool(Clock.systemUTC(), Duration.ofSeconds(30), 1 << 20),
            Duration.ofSeconds(30), Clock.systemUTC());

    /** Accepts every append its router passes on. */
    private static final class Accepting extends ForwardingIngest {
        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) throws java.io.IOException {
            records.forEachRecord(record -> { });
            return new AppendResult(1, 0L, 0L);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records, Runnable buffered) throws java.io.IOException {
            try {
                return append(principal, index, partition, records);
            } finally {
                buffered.run();
            }
        }

        @Override
        public AppendResult appendRouted(Principal principal, String indexOrAlias,
                String routing, byte lane, RecordSource records, Runnable buffered) {
            throw new UnsupportedOperationException("explicit partitions only");
        }

        @Override
        public void close() {
        }
    }

    @Test
    void anotherIndexStillWaitsWhileOneIsAtItsCap() throws Exception {
        List<CompletableFuture<AppendResult>> writes = waiters("a",
                RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX);
        assertThat(routed.registrationWaitFull("a")).as("a is at its cap").isTrue();
        assertThat(routed.registrationWaitFull("b"))
                .as("⚠️ PER INDEX (review T2): b's count is not a's").isFalse();

        writes.addAll(waiters("b", 1));
        assertThat(routed.waitingForRegistration("b")).isEqualTo(1);

        register("a");
        register("b");
        for (CompletableFuture<AppendResult> write : writes) {
            assertThat(write.get(10, TimeUnit.SECONDS).recordCount()).isEqualTo(1);
        }
        assertThat(routed.registrationWaitFull("a")).as("a registered waits for nothing")
                .isFalse();
    }

    @Test
    void theWaitsAcrossEveryNameAreCappedPodWide() throws Exception {
        List<CompletableFuture<AppendResult>> writes = new ArrayList<>();
        int names = RoutedIngest.MAX_EXPLICIT_WAITERS / RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX;
        for (int i = 0; i < names; i++) {
            writes.addAll(waiters(String.valueOf((char) ('a' + i)),
                    RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX));
        }
        assertThat(routed.waitingForRegistration()).as("the premise: the pod's cap is waiting")
                .isEqualTo(RoutedIngest.MAX_EXPLICIT_WAITERS);

        String fresh = String.valueOf((char) ('a' + names));
        assertThat(routed.registrationWaitFull(fresh))
                .as("⚠️ ACROSS NAMES (review P3): a name nobody waits for is refused too")
                .isTrue();
        assertThatThrownBy(() -> routed.append(PRINCIPAL, fresh, 0, (byte) 0, one(), () -> { }))
                .isInstanceOf(RegistrationWaitFullException.class)
                .hasMessageContaining(RoutedIngest.MAX_EXPLICIT_WAITERS + " writes wait");
        assertThat(routed.waitingForRegistration(fresh)).as("the refused write holds no slot")
                .isZero();

        for (int i = 0; i < names; i++) {
            register(String.valueOf((char) ('a' + i)));
        }
        for (CompletableFuture<AppendResult> write : writes) {
            write.get(10, TimeUnit.SECONDS);
        }
        assertThat(routed.waitingForRegistration()).isZero();
    }

    /**
     * ⚠️ THE CAP IS ON WAITS, NOT ON WRITES (M13.11 review T1): with the pod's
     * cap full of waits on invented names, a write to a REGISTERED index is
     * neither refused nor pre-refused -- the flood the cap contains must not
     * become an outage of every explicit-partition write.
     */
    @Test
    void aRegisteredIndexIsAcceptedWhileThePodsCapIsFull() throws Exception {
        register("e");
        List<CompletableFuture<AppendResult>> writes = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            writes.addAll(waiters(String.valueOf((char) ('a' + i)),
                    RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX));
        }
        assertThat(routed.waitingForRegistration()).as("the premise: the pod's cap is full")
                .isEqualTo(RoutedIngest.MAX_EXPLICIT_WAITERS);

        assertThat(routed.registrationWaitFull("e")).isFalse();
        assertThat(routed.append(PRINCIPAL, "e", 0, (byte) 0, one(), () -> { }).recordCount())
                .isEqualTo(1);

        for (int i = 0; i < 4; i++) {
            register(String.valueOf((char) ('a' + i)));
        }
        for (CompletableFuture<AppendResult> write : writes) {
            write.get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * THE WAIT ITSELF REFUSES THE NINTH (M13.11 review T3): the front door's
     * pre-check is advice, so a write that reaches the wait past the per-index
     * cap is refused there.
     */
    @Test
    void theWaitItselfRefusesOnePastTheIndexCap() throws Exception {
        List<CompletableFuture<AppendResult>> writes = waiters("a",
                RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX);

        assertThatThrownBy(() -> routed.append(PRINCIPAL, "a", 0, (byte) 0, one(), () -> { }))
                .isInstanceOf(RegistrationWaitFullException.class)
                .hasMessageContaining("already has " + RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX
                        + " writes waiting");
        assertThat(routed.waitingForRegistration("a"))
                .isEqualTo(RoutedIngest.MAX_EXPLICIT_WAITERS_PER_INDEX);

        register("a");
        for (CompletableFuture<AppendResult> write : writes) {
            write.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aFinishedWaitLeavesNoEntryBehind() throws Exception {
        List<CompletableFuture<AppendResult>> writes = waiters("a", 1);
        assertThat(routed.indicesWaitedFor()).as("the premise: one index is waited for")
                .isEqualTo(1);

        register("a");
        writes.getFirst().get(10, TimeUnit.SECONDS);

        assertThat(routed.indicesWaitedFor())
                .as("⚠️ NO ZERO-COUNT ENTRY (review T1): a name a producer invents leaves"
                        + " nothing behind").isZero();
    }

    @Test
    void anInterruptedWaitGivesItsSlotBack() throws Exception {
        CompletableFuture<Throwable> thrown = new CompletableFuture<>();
        Thread waiter = Thread.ofPlatform().start(() -> {
            try {
                routed.append(PRINCIPAL, "a", 0, (byte) 0, one(), () -> { });
                thrown.complete(null);
            } catch (Throwable t) {
                thrown.complete(t);
            }
        });
        awaitCount(() -> routed.waitingForRegistration("a"), 1);

        waiter.interrupt();

        assertThat(thrown.get(10, TimeUnit.SECONDS))
                .isInstanceOf(RegistrationTimeoutException.class);
        waiter.join(10_000);
        assertThat(routed.waitingForRegistration("a"))
                .as("⚠️ THE INTERRUPT PATH (review T3) gives the slot back").isZero();
        assertThat(routed.indicesWaitedFor()).isZero();
    }

    /** {@code n} writes to {@code index}, each observed waiting before the next starts. */
    private List<CompletableFuture<AppendResult>> waiters(String index, int n) {
        List<CompletableFuture<AppendResult>> writes = new ArrayList<>();
        int before = routed.waitingForRegistration(index);
        for (int i = 0; i < n; i++) {
            CompletableFuture<AppendResult> write = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                try {
                    write.complete(routed.append(PRINCIPAL, index, 0, (byte) 0, one(), () -> { }));
                } catch (Throwable t) {
                    write.completeExceptionally(t);
                }
            });
            writes.add(write);
            awaitCount(() -> routed.waitingForRegistration(index), before + i + 1);
        }
        return writes;
    }

    private static void awaitCount(IntSupplier count, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (count.getAsInt() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(count.getAsInt()).as("waiting").isEqualTo(expected);
    }

    private void register(String index) {
        catalog.register(new IndexRegistration(base64Url(UUID.randomUUID()), index, List.of(),
                4, 4, 1, 1));
    }

    private static Ingest.RecordSource one() {
        return sink -> sink.accept(new SegmentRecord("a", OpType.INDEX, OptionalLong.of(1),
                new byte[8]));
    }

    private static String base64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }
}
