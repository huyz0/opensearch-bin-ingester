// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a consumer meets when its position has been collected (M7.16, FR-9,
 * FR-10).
 *
 * <p>⚠️ SILENCE HERE REPRODUCES ADR-0020's FAILURE MODE ONE LEVEL UP: a shard
 * that starts, reports healthy and indexes nothing. So the answer is
 * distinguishable from BOTH neighbours — an empty read means "nothing new", a
 * {@link DeliveryGapException} means "records were dropped and here is the
 * range", and this means "your position is gone and data was lost", which is an
 * operator incident rather than a retry.
 */
class RetainedOffsetRefusalTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0);

    private static byte[] segmentOf(String... ids) throws Exception {
        io.github.huyz0.os.biningester.format.SegmentWriter w = new io.github.huyz0.os.biningester.format.SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new io.github.huyz0.os.biningester.format.SegmentRecord(id, io.github.huyz0.os.biningester.format.OpType.INDEX,
                    java.util.OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(
                            java.nio.charset.StandardCharsets.UTF_8)), 1L);
        }
        return w.toByteArray(1L);
    }

    private static Delivery delivery(long firstOffset, String... ids) throws Exception {
        return new Delivery(KEY, "seg-" + firstOffset, ids.length, firstOffset,
                io.github.huyz0.os.biningester.format.FetchMode.INLINE, segmentOf(ids));
    }

    /** ⚠️ FED, holding no subscription: the node-scoped shape since M5.62. */
    private static ConsumerClient client() {
        return new ConsumerClient(KEY, 16, null);
    }

    @Test
    void aPositionBELOWTheFloorIsREFUSED() {
        ConsumerClient client = client();
        client.retainedFrom(500);
        assertThatThrownBy(() -> client.refuseIfCollected(499))
                .as("the records this consumer would resume from have been deleted, and "
                        + "the answer must SAY so -- a 404 on the segment it was about to "
                        + "fetch is the same fact with no name on it")
                .isInstanceOf(PositionCollectedException.class)
                .hasMessageContaining("499")
                .hasMessageContaining("500");
    }

    @Test
    void aPositionATTheFloorIsFINE() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        client.refuseIfCollected(500);
        assertThat(client.positionsRefused())
                .as("the floor is the OLDEST RETAINED offset, so a consumer sitting "
                        + "exactly on it has lost nothing -- refusing here would refuse "
                        + "every consumer of a stream the moment anything was collected")
                .isZero();
    }

    @Test
    void aPositionABOVETheFloorIsFINE() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        client.refuseIfCollected(900);
        assertThat(client.positionsRefused()).isZero();
    }

    @Test
    void aClientNOBODYHasToldRefusesNOTHING() throws Exception {
        ConsumerClient client = client();
        client.refuseIfCollected(0);
        assertThat(client.positionsRefused())
                .as("⚠️ AN UNKNOWN FLOOR IS NOT A FLOOR OF ZERO AND NOT ONE OF MAX. A "
                        + "deployment whose ingester never reports the boundary must keep "
                        + "working exactly as it did before this commit -- refusing on an "
                        + "unknown floor would stop every consumer in it")
                .isZero();
    }

    @Test
    void theREFUSALIsDistinguishableFromAGAP() {
        ConsumerClient client = client();
        client.retainedFrom(500);
        assertThatThrownBy(() -> client.refuseIfCollected(10))
                .as("a GAP means records were dropped BETWEEN two deliveries and the "
                        + "consumer can carry on; this means the consumer's own starting "
                        + "point is gone and carrying on would silently skip everything "
                        + "below the floor")
                .isNotInstanceOf(DeliveryGapException.class)
                .isInstanceOf(PositionCollectedException.class);
    }

    @Test
    void theREFUSALCarriesTheRANGEThatWasLost() {
        ConsumerClient client = client();
        client.retainedFrom(500);
        try {
            client.refuseIfCollected(120);
            throw new AssertionError("expected a refusal");
        } catch (PositionCollectedException refused) {
            assertThat(refused.requestedOffset()).isEqualTo(120);
            assertThat(refused.oldestRetainedOffset()).isEqualTo(500);
            assertThat(refused.lostRecords())
                    .as("an operator needs the SIZE of the loss, not only that there was "
                            + "one -- 380 records is a different incident from 3")
                    .isEqualTo(380);
            assertThat(refused.key()).isEqualTo(KEY);
        }
    }

    @Test
    void EVERYRefusalIsCounted() {
        ConsumerClient client = client();
        client.retainedFrom(500);
        for (int i = 0; i < 3; i++) {
            try {
                client.refuseIfCollected(1);
            } catch (PositionCollectedException expected) {
                // counted below
            }
        }
        assertThat(client.positionsRefused())
                .as("a shard that retries forever produces one incident per retry, and a "
                        + "counter that stopped at the first would make a permanent "
                        + "condition look like a blip")
                .isEqualTo(3);
    }

    @Test
    void theFIRSTReportTakesEffectWhateverItsValue() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        assertThatThrownBy(() -> client.refuseIfCollected(1))
                .as("the sentinel is MAX_VALUE, so a plain max() would keep it forever and "
                        + "the first report would be swallowed -- no consumer would ever "
                        + "be refused, which is the silent failure this whole task exists "
                        + "to replace")
                .isInstanceOf(PositionCollectedException.class);
    }

    @Test
    void aHIGHERReportADVANCESTheFloor() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        client.retainedFrom(900);
        assertThatThrownBy(() -> client.refuseIfCollected(700))
                .as("⚠️ THE RISING HALF, WHICH ITS SIBLING DOES NOT PIN: delete the "
                        + "accumulate outright and a client LATCHES the first floor it is "
                        + "ever told, never advancing -- so every position collected after "
                        + "the consumer started is silently accepted, which is the "
                        + "under-refusal direction. Measured by review: all thirteen other "
                        + "cases stay green under that mutation")
                .isInstanceOf(PositionCollectedException.class);
    }

    @Test
    void theFLOOROnlyEverRISES() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        client.retainedFrom(100);
        assertThatThrownBy(() -> client.refuseIfCollected(300))
                .as("⚠️ A LOWER REPORT IS STALE, NOT A CORRECTION. Retention does not give "
                        + "records back, so a floor that walked backwards would let a "
                        + "consumer resume into records that are already gone and meet a "
                        + "404 with no name on it")
                .isInstanceOf(PositionCollectedException.class);
    }

    @Test
    void aNEGATIVEFloorIsIGNOREDAndCOUNTED() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(-1);
        client.refuseIfCollected(0);
        assertThat(client.positionsRefused()).isZero();
        assertThat(client.floorReportsIgnored())
                .as("⚠️ AN IMPOSSIBLE REPORT IS A DEFECT UPSTREAM, and a guard that "
                        + "silently returns makes it undiscoverable anywhere -- the "
                        + "counter is what distinguishes 'nobody reported' from 'somebody "
                        + "reported nonsense'")
                .isEqualTo(1);
    }

    @Test
    void aNEGATIVEPositionIsACallerDEFECTRatherThanALoss() {
        ConsumerClient client = client();
        client.retainedFrom(500);
        assertThatThrownBy(() -> client.refuseIfCollected(-1))
                .as("reporting this as a collected position would be an incident report "
                        + "with a fabricated size -- 501 records 'lost' from a stream that "
                        + "never had a -1")
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(PositionCollectedException.class);
        assertThat(client.positionsRefused()).isZero();
    }

    @Test
    void anUNKNOWNFloorIsNOTAFloorOfZEROAndNotOneOfMAX() throws Exception {
        ConsumerClient client = client();
        client.refuseIfCollected(0);
        client.refuseIfCollected(Long.MAX_VALUE - 1);
        assertThat(client.positionsRefused())
                .as("⚠️ THE UNKNOWN CASE MUST BE CARRIED BY THE CHECK, NOT BY THE "
                        + "SENTINEL'S ARITHMETIC. With a negative sentinel every real "
                        + "offset compares above it, so deleting the check changes nothing "
                        + "and the sentinel's VALUE silently becomes the rule -- this case "
                        + "reds only because the sentinel is MAX_VALUE and the check is "
                        + "what lets anything through")
                .isZero();
    }

    @Test
    void READINGStillWorksAboveTheFloor() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(0);
        client.deliver(delivery(0, "a", "b"));
        assertThat(client.readNext(Duration.ofMillis(50)))
                .as("the floor guards the SEEK, not the read -- a client that refused "
                        + "deliveries too would stop a consumer that is perfectly healthy")
                .isPresent();
    }
}
