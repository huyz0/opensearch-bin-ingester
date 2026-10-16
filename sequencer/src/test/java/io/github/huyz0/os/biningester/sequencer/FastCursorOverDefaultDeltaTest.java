// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.Q1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A switch to fast mode with a default-path delta PUT in flight, and with one
 * ambiguous (M13.34ac; ADR-0081 §8; SPEC criterion 15): the fast cursor of a
 * stream first committed at the leader opens at the chain's committed next
 * offset, read only once that PUT has returned or been reconciled -- never
 * under records the default path committed, so every offset is committed once.
 *
 * <p>⚠️ THROUGH THE DESK, wired to the sequencer as the server wires it
 * ({@code term::committedNext}): {@code FastCursorAfterReconcileTest} pins the
 * chain's read, this pins that the cursor takes it.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastCursorOverDefaultDeltaTest {

    private static final RunKey FRESH = new RunKey(Q1, 3);
    private static final CommitRequest DEFAULT_RUN =
            new CommitRequest("poda", "i1", 0, "seg/0", Map.of(FRESH, 4));

    private static LocalSequencer leaderOver(BinStore store) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
    }

    private static long firstOffset(FastFrame.Body answer) {
        return ((FastWriteFrame.Assigned) answer).runs().get(0).firstOffset();
    }

    @Test
    void aCURSOROpenedWhileADefaultDeltaPUTIsInFlightOpensAfterIt() throws Exception {
        AtomicBoolean armed = new AtomicBoolean();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // ⚠️ PARKED INSIDE the delta's PUT, before it reaches the store: the
        // key predicate is the hook, and answers false, so the write passes.
        BinStore store = new AmbiguousPutStore(new MemoryBinStore(),
                AmbiguousPutStore.Mode.LOST, AmbiguousPutStore.Target.PUT_IF_ABSENT, false,
                key -> {
                    if (armed.get() && key.endsWith(".delta")) {
                        entered.countDown();
                        try {
                            release.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return false;
                });
        LocalSequencer sequencer = leaderOver(store);
        try {
            FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
            CommitDesk desk = new CommitDesk(l.leader(), sequencer::committedNext,
                    Duration.ofSeconds(10));
            armed.set(true);
            CompletableFuture<Void> inFlight = CompletableFuture.runAsync(() -> {
                try {
                    sequencer.commitAll(List.of(DEFAULT_RUN));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            assertThat(entered.await(10, TimeUnit.SECONDS)).as("the PUT in flight").isTrue();

            CompletableFuture<FastFrame.Body> fast = CompletableFuture.supplyAsync(() -> {
                try {
                    return desk.answer(B, commit(1, FRESH), roster(), ALL);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            Thread.sleep(300);
            assertThat(fast).as("no offset while the PUT is in flight").isNotDone();
            armed.set(false);
            release.countDown();
            inFlight.get(10, TimeUnit.SECONDS);

            assertThat(firstOffset(fast.get(10, TimeUnit.SECONDS)))
                    .as("after the 4 records the default path committed").isEqualTo(4);
        } finally {
            release.countDown();
            sequencer.close();
        }
    }

    @Test
    void aCURSOROpenedAfterAnAMBIGUOUSDefaultDeltaOpensAfterWhatLanded() throws Exception {
        AtomicBoolean armed = new AtomicBoolean();
        BinStore store = new AmbiguousPutStore(new MemoryBinStore(),
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT, false,
                key -> armed.get());
        LocalSequencer sequencer = leaderOver(store);
        try {
            FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
            CommitDesk desk = new CommitDesk(l.leader(), sequencer::committedNext,
                    Duration.ofSeconds(10));
            armed.set(true);
            assertThatThrownBy(() -> sequencer.commitAll(List.of(DEFAULT_RUN)))
                    .isInstanceOf(AmbiguousAppendException.class);
            armed.set(false);

            FastFrame.Body answer = desk.answer(B, commit(1, FRESH), roster(), ALL);

            assertThat(firstOffset(answer)).as("after the 4 records that landed unanswered")
                    .isEqualTo(4);
        } finally {
            sequencer.close();
        }
    }
}
