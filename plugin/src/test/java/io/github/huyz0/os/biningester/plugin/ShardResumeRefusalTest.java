// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.PositionCollectedException;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A shard that resumes below the retained floor is REFUSED out of
 * {@code readNext} (M8.6, M7.16, M7.18, ADR-0056).
 *
 * <p>⚠️ **REVIEW FOUND THIS WAS NEVER WIRED.** {@code ConsumerClient} could
 * refuse since M7.16, and nothing in production asked it to -- every refusal
 * assertion in the tree called {@code refuseIfCollected} from a TEST. The floor
 * arrived, was stored, and was read by nobody, so a shard resuming from a
 * collected position still met the 404 ADR-0020 describes. These cases go
 * through {@code readNext}, which is the SPI the OpenSearch engine calls.
 *
 * <p>⚠️ **THROWN, BECAUSE THE SPI PAUSES A SHARD THAT THROWS**, and that is the
 * point: research 02 §6 says an exception from the poll path pauses that shard
 * until an operator intervenes, readable in {@code _ingestion/_state}. Records
 * below the floor will never arrive however long the shard waits.
 */
class ShardResumeRefusalTest {

    private static final RunKey STREAM = new RunKey(new UUID(9, 9), 1);

    /** A transport that keeps the listener, so a test can hand it a floor. */
    private static final class CapturingTransport implements SubscriptionTransport {
        final List<Listener> listeners = new ArrayList<>();

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            listeners.add(listener);
            return () -> { };
        }
    }

    private final CapturingTransport transport = new CapturingTransport();

    private BinStoreShardConsumer shard() {
        return new BinStoreShardConsumer(0, new ConsumerClient(transport, STREAM, 16, null, TestRetries.noFailedFetch()));
    }

    /** Hands the floor to every client subscribed so far. */
    private void floor(long oldestRetained) {
        transport.listeners.forEach(l -> l.onRetainedFloor(STREAM, oldestRetained));
    }

    @Test
    void aRESUMEBelowAKnownFloorIsRefusedOutOfReadNextWithTheLostRange() {
        BinStoreShardConsumer shard = shard();
        floor(500);

        assertThatThrownBy(() -> shard.readNext(new BinStoreOffset(100), true, 10, 0))
                .as("⚠️ OUT OF readNext, so the ENGINE pauses the shard")
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(PositionCollectedException.class)
                .hasMessageContaining("100")
                .hasMessageContaining("500");
    }

    @Test
    void aFloorThatArrivesAFTERTheResumesFirstReadStillRefusesTheNEXTRead() {
        // ⚠️ THE FLOOR IS ASYNCHRONOUS. It rides the subscription's first
        // answer, which can land after the engine's first `readNext`. A check
        // made once, before it arrived, would refuse nothing for the session
        // -- and the engine calls the pointer overload only on a resume, and
        // the plain one after that.
        BinStoreShardConsumer shard = shard();
        assertThatCode(() -> shard.readNext(new BinStoreOffset(100), true, 10, 0))
                .as("no floor known yet: nothing to refuse with").doesNotThrowAnyException();

        floor(500);

        assertThatThrownBy(() -> shard.readNext(10, 0))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(PositionCollectedException.class);
    }

    @Test
    void anEXCLUSIVEResumeAsksForThePOSITIONAfterThePointer() {
        // ⚠️ `includeStart=false` RESUMES AT pointer + 1, so a pointer exactly
        // one below the floor is fine -- and refusing it would pause a healthy
        // shard for a record it had already indexed.
        BinStoreShardConsumer shard = shard();
        floor(500);

        assertThatCode(() -> shard.readNext(new BinStoreOffset(499), false, 10, 0))
                .doesNotThrowAnyException();
        BinStoreShardConsumer inclusive = shard();
        floor(500);
        assertThatThrownBy(() -> inclusive.readNext(new BinStoreOffset(499), true, 10, 0))
                .as("and the inclusive form at the same pointer IS below it")
                .isInstanceOf(UncheckedIOException.class);
    }

    @Test
    void aRESUMEAtOrAboveTheFloorIsNOTRefused() {
        BinStoreShardConsumer shard = shard();
        floor(500);

        assertThatCode(() -> shard.readNext(new BinStoreOffset(500), true, 10, 0))
                .doesNotThrowAnyException();
        assertThatCode(() -> shard.readNext(10, 0)).doesNotThrowAnyException();
    }

    @Test
    void aShardThatNEVERResumedIsNeverRefusedWhateverTheFloor() {
        // ⚠️ LIVE TAILING IS AT THE HEAD. A shard that only ever called the
        // plain overload asked for no position, and refusing it would pause a
        // healthy shard.
        BinStoreShardConsumer shard = shard();
        floor(1_000_000);

        assertThatCode(() -> shard.readNext(10, 0)).doesNotThrowAnyException();
    }

    @Test
    void anINGESTERThatNeverSendsAFloorLeavesAResumedShardUNREFUSED() {
        // ⚠️ THE OLD INGESTER: it predates ADR-0056 and never sends a floor, so
        // the shard keeps asking and refuses nothing -- the behaviour before
        // the floor existed, not a new failure.
        BinStoreShardConsumer shard = shard();

        assertThatCode(() -> {
            shard.readNext(new BinStoreOffset(100), true, 10, 0);
            shard.readNext(10, 0);
            shard.readNext(10, 0);
        }).doesNotThrowAnyException();
        assertThat(transport.listeners).isNotEmpty();
    }

    @Test
    void aPASSEDResumeIsNotRefusedWhenTheFloorLATERRises() {
        // ⚠️ REVIEW MEASURED THIS UNPINNED: a resume that was checked and
        // passed must not be checked again, or the next GC pass that raises the
        // floor past it pauses a HEALTHY shard -- one that resumed correctly and
        // has been reading ever since.
        BinStoreShardConsumer shard = shard();
        assertThatCode(() -> shard.readNext(new BinStoreOffset(500), true, 10, 0))
                .doesNotThrowAnyException();
        floor(100); // fresh: reported after the resume asked
        assertThatCode(() -> shard.readNext(10, 0)).doesNotThrowAnyException();

        floor(10_000);

        assertThatCode(() -> shard.readNext(10, 0))
                .as("⚠️ PASSED ONCE, AND NOT RE-JUDGED AGAINST A FLOOR THAT ROSE AFTER")
                .doesNotThrowAnyException();
    }

    @Test
    void aSTALEFloorCannotCLEARAResumeOnlyAFRESHOneCan() {
        // ⚠️ REVIEW's EXAMPLE, VERBATIM: a node learned floor 100 at 09:00 and
        // GC reached 5000 by 15:00. A reset to 1000 checked against the held
        // 100 passes, and the shard meets the unnamed 404 this change exists
        // to replace. So a held floor may refuse but may not clear.
        BinStoreShardConsumer shard = shard();
        floor(100); // held since "09:00"

        assertThatCode(() -> shard.readNext(new BinStoreOffset(1000), true, 10, 0))
                .as("the held floor proves nothing collected at 1000 -- and nothing clean")
                .doesNotThrowAnyException();

        floor(5000); // the fresh one the resume asked for

        assertThatThrownBy(() -> shard.readNext(10, 0))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(PositionCollectedException.class);
    }

    @Test
    void aRESUMEMakesTheClientASKTailingDoesNotAndTheAsksAreBOUNDED() {
        // ⚠️ THE ASK IS WHAT COSTS THE SERVING POD A READ. A shard tailing the
        // stream must not ask at all, and a resume whose floor never comes --
        // an ingester with none to give -- must stop asking, or the pod reads
        // the store once a refresh interval for ever: a timer on an idle pod,
        // which review found in the version that asked until a floor arrived.
        BinStoreShardConsumer shard = shard();
        SubscriptionTransport.Listener listener = transport.listeners.get(0);
        assertThatCode(() -> shard.readNext(10, 0)).doesNotThrowAnyException();
        assertThat(listener.wantsRetainedFloor(STREAM)).as("tailing asks nothing").isFalse();

        assertThatCode(() -> shard.readNext(new BinStoreOffset(100), true, 10, 0))
                .doesNotThrowAnyException();
        int asked = 0;
        while (listener.wantsRetainedFloor(STREAM)) {
            asked++;
            assertThat(asked).as("the asks must run out").isLessThanOrEqualTo(1000);
        }
        assertThat(asked).isEqualTo(ConsumerClient.MAX_FLOOR_ASKS);

        assertThatCode(() -> shard.readNext(new BinStoreOffset(100), true, 10, 0))
                .doesNotThrowAnyException();
        assertThat(listener.wantsRetainedFloor(STREAM)).as("a NEW resume asks again").isTrue();
        floor(50);
        assertThat(listener.wantsRetainedFloor(STREAM))
                .as("and a floor, once it arrives, ends the asking").isFalse();
    }
}
