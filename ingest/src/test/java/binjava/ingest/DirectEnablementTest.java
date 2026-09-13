// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.format.FetchMode;
import binjava.binstore.backend.MemoryBinStore;
import java.time.Clock;
import org.junit.jupiter.api.Test;

/**
 * "This deployment enables {@code direct}" — the setting M5.13's refusal needed.
 *
 * <p>⚠️ THE TWO HALVES DISAGREED AND THAT WAS THE DECISION. {@code FetchPolicy}
 * silently DEGRADES to {@code proxy} when the backend cannot presign, while
 * {@code GrantIssuer} REFUSES TO EXIST. Wiring the refusal unconditionally
 * fails every pod to start on both shipping backends; wiring it lazily puts it
 * back at the first fetch, which is what it was built to prevent. A setting
 * resolves it: enabled and unable to sign is a startup failure, not enabled
 * means the policy never chooses the mode and nothing needs signing.
 */
class DirectEnablementTest {

    private static IngestConfig withDirect(boolean enabled) {
        IngestConfig base = IngestTestSupport.pinnedIntervalConfig(
                IngestTestSupport.NEVER, 8L << 20);
        return new IngestConfig(base.intervalFloor(), base.maxSegmentBytes(), base.trustDomain(),
                base.maxQueuedPushBytes(), base.intervalCeiling(), base.fillRatioLowThreshold(),
                base.fillRatioHighThreshold(), base.intervalLengthenDelay(),
                base.intervalShortenDelay(), enabled);
    }

    private static DefaultIngest pod(IngestConfig config, CountingBinStore store,
            SubscriptionHub hub) throws java.io.IOException {
        return new DefaultIngest(config, store, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(store, "pod1"), hub, Clock.systemUTC(),
                index -> IngestTestSupport.LOGS);
    }

    /**
     * A deployment asking for {@code direct} on a backend that cannot sign does
     * not start.
     *
     * <p>⚠️ AT STARTUP, WHICH IS THE WHOLE CRITERION. The alternative a lazy
     * check gives is a pod that runs, accepts writes, acknowledges them, and
     * then fails the first consumer that asks for a grant -- after the data is
     * durable and the producer has been told so.
     */
    @Test
    void aPodThatWantsDIRECTOnABackendThatCannotSIGNRefusesToSTART() {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();

        assertThat(store.capabilities().presignedUrls())
                .as("the fixture only means anything if this backend really cannot sign")
                .isFalse();
        assertThatThrownBy(() -> pod(withDirect(true), store, hub))
                .as("enabled, and the backend cannot sign -- so the pod does not come up")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("presign");
    }

    /**
     * And the same pod starts normally when it has not asked for {@code direct}.
     *
     * <p>⚠️ WITHOUT THIS THE REFUSAL COULD BE UNCONDITIONAL and look correct.
     * Both shipping backends report {@code presignedUrls=false}, so a check
     * that ignored the setting would stop every deployment in existence.
     */
    @Test
    void aPodThatDidNotAskForDIRECTStartsOnTheSameBackend() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();

        assertThatCode(() -> {
            try (DefaultIngest ingest = pod(withDirect(false), store, hub)) {
                assertThat(ingest).isNotNull();
            }
        }).doesNotThrowAnyException();
    }

    /**
     * An ENABLED pod's policy actually answers {@code DIRECT}.
     *
     * <p>⚠️ THE OTHER HALF OF THE WIRING, AND NOTHING REACHED IT. Review
     * measured `DefaultIngest` passing a literal {@code false} to
     * {@code defaultsFor(costs, ...)} surviving the whole suite: both shipping
     * backends refuse at {@code requirePresignedUrls}, so no test could
     * construct an enabled pod and the enabled arm was never executed. When a
     * real signer lands, an operator setting {@code directEnabled=true} would
     * have got a pod that starts and answers {@code PROXY} forever, with every
     * test green -- the flag wired to the refusal and not to the policy.
     */
    @Test
    void anENABLEDPodsPolicyReallyAnswersDIRECT() throws Exception {
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();

        assertThat(store.capabilities().presignedUrls())
                .as("this fixture signs, which is what lets an enabled pod start at all")
                .isTrue();
        try (DefaultIngest ingest = pod(withDirect(true), store, hub)) {
            SegmentDelivery coldAndAlone = new SegmentDelivery(4L << 20, false, 1, false);
            assertThat(ingest.serving().policy().modeFor(coldAndAlone, store.capabilities()))
                    .as("enabled, capable, cold and fan-out one -- the policy the POD built "
                            + "must answer DIRECT, or the flag reached the refusal and not "
                            + "the policy")
                    .isEqualTo(FetchMode.DIRECT);
        }
    }

    /**
     * And a DISABLED pod's policy answers {@code PROXY} on a backend that CAN sign.
     *
     * <p>⚠️ THE OTHER DIRECTION, AND THE FIRST FIX ONLY BOUGHT ONE. Review
     * measured {@code defaultsFor(costs, true)} -- the flag hardcoded ON --
     * surviving the whole suite, because every disabled-pod case ran against a
     * store that cannot presign, where {@code modeFor}'s
     * {@code capabilities.presignedUrls() &&} removes {@code DIRECT} before the
     * flag is ever read. {@code StoreFakes.CanPresign} exists to escape exactly
     * that and was used by the enabled case alone.
     *
     * <p>⚠️ WHAT IT WOULD COST: on a signing backend a deployment that never
     * opted in gets {@code DIRECT}, having skipped the startup refusal -- and
     * M5.45b builds {@code GrantIssuer} under the same
     * {@code config.directEnabled()} guard, so the policy would elect a mode
     * for which no issuer exists.
     */
    @Test
    void aDISABLEDPodsPolicyAnswersPROXYEvenOnABackendThatCanSIGN() throws Exception {
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();

        try (DefaultIngest ingest = pod(withDirect(false), store, hub)) {
            SegmentDelivery coldAndAlone = new SegmentDelivery(4L << 20, false, 1, false);
            assertThat(ingest.serving().policy().modeFor(coldAndAlone, store.capabilities()))
                    .as("capable, cold and fan-out one -- every condition DIRECT wants except "
                            + "the one the deployment never gave")
                    .isEqualTo(FetchMode.PROXY);
        }
    }

    /** Off is the default, so an existing deployment does not start refusing. */
    @Test
    void DIRECTIsOFFUnlessAskedFor() {
        assertThat(IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20)
                .directEnabled())
                .as("the nine-argument constructor every existing site uses means OFF")
                .isFalse();
    }
}
