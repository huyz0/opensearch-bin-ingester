// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A hint whose post fails is counted, once, and told to the node (M13.70;
 * M13.52d review, T4): swallowed, it leaves no other trace.
 */
class DurableSegmentSignalSenderLostTest {

    private static final DurableSegmentSignalFrame FRAME = new DurableSegmentSignalFrame(
            "writera", "az-a", new SegmentKey("bins/cluster-a", 1, "writera", 1, 48).key());
    private static final List<EndpointSliceView.Endpoint> OWNER =
            List.of(new EndpointSliceView.Endpoint("ownerb", "10.0.0.2", "az-b"));

    @Test
    void aFAILEDPostIsCountedOnceAndToldToTheNode() {
        AtomicInteger told = new AtomicInteger();
        DurableSegmentSignalSender sender = new DurableSegmentSignalSender(
                CrossAzBytes.untracked(), 7001, (endpoint, body) -> {
                    throw new IOException("refused");
                }, false, told::incrementAndGet);

        sender.send(FRAME, OWNER);

        assertThat(sender.lost()).isEqualTo(1);
        assertThat(told).hasValue(1);
    }

    @Test
    void aDELIVEREDPostIsNotCounted() {
        AtomicInteger told = new AtomicInteger();
        DurableSegmentSignalSender sender = new DurableSegmentSignalSender(
                CrossAzBytes.untracked(), 7001, (endpoint, body) -> { }, false,
                told::incrementAndGet);

        sender.send(FRAME, OWNER);

        assertThat(sender.lost()).isZero();
        assertThat(told).hasValue(0);
    }
}
