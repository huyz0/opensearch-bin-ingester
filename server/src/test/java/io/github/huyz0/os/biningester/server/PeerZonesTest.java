// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.EpochFence;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Every fast frame is counted against its peer's zone (M13.64): the zone of a
 * frame's target learned from the roster its epoch names, once, and the zone
 * of a frame's sender learned from the incarnation a JOIN or DEPART carries.
 */
class PeerZonesTest {

    private static final Roster.Incarnation LEADER =
            new Roster.Incarnation("l", "uid-l", "az-b", "http://leader:1");
    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-a", "http://pod:1");

    private static MemoryBinStore withRoster(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster roster = new Roster(epoch, -1, LEADER, List.of(
                new Roster.Member(LEADER, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>())), List.of(), 0, 0, false);
        store.put(Roster.key("p", epoch), Body.ofBytes(roster.encode()));
        return store;
    }

    /** {@code inner}, every GET counted into {@code gets}. */
    private static BinStore counting(MemoryBinStore inner, AtomicInteger gets) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("get")) {
                        gets.incrementAndGet();
                    }
                    try {
                        return method.invoke(inner, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static byte[] join(long epoch) {
        return FastFrame.encode(epoch, POD.podUid(), LEADER.podUid(),
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    @Test
    void aSENTFrameIsCountedAtItsTargetsZoneFromTheRosterItsEpochNames() throws Exception {
        AtomicInteger gets = new AtomicInteger();
        BinStore store = counting(withRoster(4), gets);
        PeerZones zones = new PeerZones();
        CrossAzBytes crossAz = new CrossAzBytes("az-a", zones::ofEndpoint);
        List<String> sentTo = new ArrayList<>();
        TermJoiner.Transport transport = zones.learning((endpoint, frame) -> {
            crossAz.sentTo(CrossAzBytes.Transport.FAST_CONTROL, endpoint, frame.length);
            sentTo.add(endpoint);
            return new byte[0];
        }, store, "p");

        transport.exchange(LEADER.endpoint(), join(4));
        transport.exchange(LEADER.endpoint(), join(4));

        assertThat(sentTo).hasSize(2);
        assertThat(crossAz.unknownPeerBytes()).as("its zone was known").isZero();
        assertThat(crossAz.crossAzBytes()).as("az-a to az-b").isPositive();
        assertThat(gets.get()).as("one roster read, not one per send").isEqualTo(1);
    }

    @Test
    void anANSWEREDFrameIsCountedAtItsSendersZoneLearnedFromItsJOIN() throws Exception {
        PeerZones zones = new PeerZones();
        CrossAzBytes crossAz = new CrossAzBytes("az-b", zones::ofEndpoint);
        FastFrameRouter router = FastPeer.router("uid-l",
                EpochFence.start(new MemoryBinStore(), "lease", Optional.empty()), crossAz,
                zones);
        router.handle(FastFrame.KIND_JOIN, (header, body) ->
                new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        router.handle(FastFrame.KIND_HELD, (header, body) ->
                new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        router.answer(join(4));

        router.answer(FastFrame.encode(4, POD.podUid(), LEADER.podUid(),
                new FastFrame.HeldReport(FastFrame.Held.NONE)));

        assertThat(crossAz.unknownPeerBytes()).as("a HELD names no zone; its sender's is known")
                .isZero();
        assertThat(zones.ofUid(POD.podUid())).contains("az-a");
        assertThat(zones.ofEndpoint(POD.endpoint())).contains("az-a");
    }
}
