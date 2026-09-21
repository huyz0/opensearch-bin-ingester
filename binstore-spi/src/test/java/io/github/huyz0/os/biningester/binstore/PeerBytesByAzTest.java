// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Cross-AZ bytes are counted by LABEL (M9.2, NFR-5, M9 criterion 6).
 *
 * <p>⚠️ **THE MUTATION THIS FILE EXISTS TO KILL** is M9's test plan's own: a
 * counter keyed by the POD rather than by the label. It passes every
 * single-peer case — one peer, one pod, "not me" and "not my zone" agree — so
 * the cases below put TWO pods in the local zone and ask that both are same-AZ.
 */
class PeerBytesByAzTest {

    private static final String LOCAL = "az-a";

    @Test
    void bytesToAPeerInAnotherZoneAreCrossAz() {
        CrossAzBytes counter = new CrossAzBytes(LOCAL);
        counter.sent(Transport.PROXY_READ, "az-b", 4_096);
        assertThat(counter.crossAzBytes()).isEqualTo(4_096);
        assertThat(counter.sameAzBytes()).isZero();
        assertThat(counter.unknownPeerBytes()).isZero();
    }

    @Test
    void bytesToAPeerInThisZoneAreNotCrossAz() {
        CrossAzBytes counter = new CrossAzBytes(LOCAL);
        counter.sent(Transport.INLINE_PUSH, LOCAL, 1_000);
        assertThat(counter.crossAzBytes()).isZero();
        assertThat(counter.sameAzBytes()).isEqualTo(1_000);
        assertThat(counter.sameAzBytes(Transport.INLINE_PUSH))
                .as("⚠️ THE SAME-AZ SIDE IS ATTRIBUTED PER TRANSPORT TOO, and it is "
                        + "what a report divides by to say which socket a pod spends "
                        + "its bytes on")
                .isEqualTo(1_000);
        assertThat(counter.sameAzBytes(Transport.COMMIT_FORWARD)).isZero();
    }

    @Test
    void twoDifferentPodsInThisZoneAreBothSameAz() {
        // ⚠️ THE CASE THAT KILLS "KEYED BY POD". Both peers are other pods --
        // different ids, different endpoints -- and both are in this pod's
        // zone, so nothing here may be counted cross-AZ.
        CrossAzBytes counter = new CrossAzBytes(LOCAL,
                endpoint -> Optional.of(endpoint.equals("http://pod-9:8080") ? LOCAL : "az-c"));
        counter.sentTo(Transport.COMMIT_FORWARD, "http://pod-9:8080", 300);
        counter.sentTo(Transport.COMMIT_FORWARD, "http://pod-4:8080", 700);
        assertThat(counter.sameAzBytes()).isEqualTo(300);
        assertThat(counter.crossAzBytes()).isEqualTo(700);
    }

    @Test
    void anUnlabelledPeerIsCountedCrossAzAndReportedUnknown() {
        // ⚠️ A SILENT ZERO IS A LIE IN A COST REPORT: NFR-5 is an upper bound,
        // so bytes nobody could attribute count against it and say so.
        CrossAzBytes counter = new CrossAzBytes(LOCAL);
        counter.sent(Transport.CONSUMER_POLL, null, 11);
        counter.sent(Transport.CONSUMER_POLL, "   ", 9);
        assertThat(counter.crossAzBytes()).isEqualTo(20);
        assertThat(counter.unknownPeerBytes()).isEqualTo(20);
        assertThat(counter.sameAzBytes()).isZero();
    }

    @Test
    void anEndpointTheDirectoryDoesNotKnowIsCrossAzAndUnknown() {
        CrossAzBytes counter = new CrossAzBytes(LOCAL, endpoint -> Optional.empty());
        counter.sentTo(Transport.INBOX_DRAIN, "http://stranger:8080", 64);
        assertThat(counter.crossAzBytes(Transport.INBOX_DRAIN)).isEqualTo(64);
        assertThat(counter.unknownPeerBytes()).isEqualTo(64);
    }

    @Test
    void everyTransportIsAttributedSeparatelyAndSumsToTheTotal() {
        CrossAzBytes counter = new CrossAzBytes(LOCAL);
        counter.sent(Transport.PROXY_READ, "az-b", 1);
        counter.sent(Transport.INLINE_PUSH, "az-b", 2);
        counter.sent(Transport.CONSUMER_POLL, "az-b", 4);
        counter.sent(Transport.COMMIT_FORWARD, "az-b", 8);
        counter.sent(Transport.INBOX_DRAIN, "az-b", 16);
        assertThat(counter.crossAzBytes(Transport.PROXY_READ)).isEqualTo(1);
        assertThat(counter.crossAzBytes(Transport.INLINE_PUSH)).isEqualTo(2);
        assertThat(counter.crossAzBytes(Transport.CONSUMER_POLL)).isEqualTo(4);
        assertThat(counter.crossAzBytes(Transport.COMMIT_FORWARD)).isEqualTo(8);
        assertThat(counter.crossAzBytes(Transport.INBOX_DRAIN)).isEqualTo(16);
        assertThat(counter.crossAzBytes()).isEqualTo(31);
    }

    @Test
    void aLabelIsComparedTrimmed() {
        CrossAzBytes counter = new CrossAzBytes(" az-a ");
        assertThat(counter.localAz()).isEqualTo(LOCAL);
        counter.sent(Transport.INLINE_PUSH, " az-a ", 5);
        assertThat(counter.sameAzBytes()).isEqualTo(5);
        assertThat(counter.crossAzBytes()).isZero();
    }

    @Test
    void aPodWithNoZoneIsRefusedRatherThanDefaulted() {
        assertThatThrownBy(() -> new CrossAzBytes("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("az label");
        assertThatThrownBy(() -> new CrossAzBytes(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeBytesAreRefusedAndZeroIsNot() {
        CrossAzBytes counter = new CrossAzBytes(LOCAL);
        assertThatThrownBy(() -> counter.sent(Transport.PROXY_READ, "az-b", -1))
                .isInstanceOf(IllegalArgumentException.class);
        counter.sent(Transport.PROXY_READ, "az-b", 0);
        assertThat(counter.crossAzBytes()).isZero();
        assertThat(counter.unknownPeerBytes()).isZero();
    }

    @Test
    void theUntrackedCounterACCEPTSBytesAndREFUSESEveryReading() {
        // ⚠️ A ZERO FROM AN INSTANCE THAT COUNTED NOTHING IS THE DEFECT, not
        // the safe answer: the constructions that install this one are the
        // ones with no `pod.az`, and a report taken from a node built that way
        // would print "0 cross-AZ bytes" having measured none. Every reading
        // refuses, so that zero cannot be reached at all.
        CrossAzBytes untracked = CrossAzBytes.untracked();
        untracked.sent(Transport.COMMIT_FORWARD, "az-b", 1_000);
        untracked.sentTo(Transport.INBOX_DRAIN, "http://peer:1", 1_000);

        assertThatThrownBy(untracked::crossAzBytes)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recorded nothing");
        assertThatThrownBy(() -> untracked.crossAzBytes(Transport.COMMIT_FORWARD))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(untracked::sameAzBytes)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> untracked.sameAzBytes(Transport.COMMIT_FORWARD))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(untracked::unknownPeerBytes)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(untracked::localAz)
                .as("⚠️ THE LABEL TOO: `untracked` is not a zone, and a report that "
                        + "printed it would name a pod's zone that no operator configured")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aTrackingCounterANSWERSEveryReadingIncludingAGenuineZero() {
        // ⚠️ THE OTHER HALF, AND IT IS WHAT KEEPS THE REFUSAL ABOVE FROM BEING
        // "every reading throws": a counter that HAS a zone and was sent
        // nothing has genuinely measured zero, and must say so.
        CrossAzBytes counted = new CrossAzBytes(LOCAL);
        assertThat(counted.crossAzBytes()).isZero();
        assertThat(counted.sameAzBytes()).isZero();
        assertThat(counted.unknownPeerBytes()).isZero();
        assertThat(counted.crossAzBytes(Transport.INLINE_PUSH)).isZero();
        assertThat(counted.sameAzBytes(Transport.INLINE_PUSH)).isZero();
        assertThat(counted.localAz()).isEqualTo(LOCAL);
    }
}
