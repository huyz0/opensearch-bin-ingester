// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * When a peer's zone is read (M13.64 review round 1, P1, P2, T2, T3): again
 * after a roster not yet written, again for a newer term, once for a roster
 * that does not name the endpoint, and for every member the roster lists.
 */
class PeerZonesRosterTest {

    private static final String ENDPOINT = "http://leader:1";
    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-a", "http://pod:1");

    private static Roster roster(long epoch, Roster.Incarnation leader,
            Roster.Incarnation... others) {
        List<Roster.Member> members = new java.util.ArrayList<>();
        members.add(new Roster.Member(leader, Roster.State.ROSTERED));
        for (Roster.Incarnation other : others) {
            members.add(new Roster.Member(other, Roster.State.ROSTERED));
        }
        return new Roster(epoch, -1, leader, members,
                List.of(new Roster.TermRecord(0, new TreeMap<>())), List.of(), 0, 0, false);
    }

    private static void put(MemoryBinStore store, Roster roster) throws Exception {
        store.put(Roster.key("p", roster.epoch()), Body.ofBytes(roster.encode()));
    }

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
        return FastFrame.encode(epoch, POD.podUid(), "uid-l",
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    private static TermJoiner.Transport counted(PeerZones zones, CrossAzBytes crossAz,
            BinStore store) {
        return zones.learning((endpoint, frame) -> {
            crossAz.sentTo(CrossAzBytes.Transport.FAST_FRAME, endpoint, frame.length);
            return new byte[0];
        }, store, "p");
    }

    @Test
    void aROSTERNotYetWrittenIsReadAgainOnTheNextSend() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        AtomicInteger gets = new AtomicInteger();
        PeerZones zones = new PeerZones();
        CrossAzBytes crossAz = new CrossAzBytes("az-a", zones::ofEndpoint);
        TermJoiner.Transport transport = counted(zones, crossAz, counting(memory, gets));

        transport.exchange(ENDPOINT, join(4));
        long unknownBefore = crossAz.unknownPeerBytes();
        put(memory, roster(4, new Roster.Incarnation("l", "uid-l", "az-b", ENDPOINT)));
        transport.exchange(ENDPOINT, join(4));

        assertThat(unknownBefore).as("the premise: the term's lease came first").isPositive();
        assertThat(crossAz.unknownPeerBytes()).as("the retry was counted at az-b")
                .isEqualTo(unknownBefore);
        assertThat(zones.ofEndpoint(ENDPOINT)).contains("az-b");
        assertThat(gets.get()).isEqualTo(2);
    }

    @Test
    void aNEWERTermsRosterIsReadForAnEndpointLearnedEarlier() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        put(memory, roster(4, new Roster.Incarnation("l", "uid-l", "az-b", ENDPOINT)));
        // ⚠️ THE SAME ENDPOINT, REUSED BY A POD IN ANOTHER ZONE
        put(memory, roster(5, new Roster.Incarnation("m", "uid-m", "az-a", ENDPOINT)));
        PeerZones zones = new PeerZones();
        CrossAzBytes crossAz = new CrossAzBytes("az-a", zones::ofEndpoint);
        TermJoiner.Transport transport = counted(zones, crossAz, memory);
        transport.exchange(ENDPOINT, join(4));
        assertThat(zones.ofEndpoint(ENDPOINT)).as("the premise").contains("az-b");

        transport.exchange(ENDPOINT, join(5));

        assertThat(zones.ofEndpoint(ENDPOINT)).contains("az-a");
    }

    @Test
    void aROSTERNotNamingTheEndpointIsReadOncePerEpoch() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        put(memory, roster(4, new Roster.Incarnation("l", "uid-l", "az-b", "http://other:1")));
        AtomicInteger gets = new AtomicInteger();
        PeerZones zones = new PeerZones();
        CrossAzBytes crossAz = new CrossAzBytes("az-a", zones::ofEndpoint);
        TermJoiner.Transport transport = counted(zones, crossAz, counting(memory, gets));

        for (int i = 0; i < 5; i++) {
            transport.exchange(ENDPOINT, join(4));
        }

        assertThat(crossAz.unknownPeerBytes()).isPositive();
        assertThat(gets.get()).as("never a read per send").isEqualTo(1);
    }

    @Test
    void everyMEMBERTheRosterListsIsLearned() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        Roster.Incarnation member = new Roster.Incarnation("q", "uid-q", "az-c", "http://q:1");
        put(memory, roster(4, new Roster.Incarnation("l", "uid-l", "az-b", ENDPOINT), member));
        PeerZones zones = new PeerZones();
        TermJoiner.Transport transport = counted(zones,
                new CrossAzBytes("az-a", zones::ofEndpoint), memory);

        transport.exchange(ENDPOINT, join(4));

        assertThat(zones.ofEndpoint(member.endpoint())).contains("az-c");
        assertThat(zones.ofUid(member.podUid())).contains("az-c");
    }
}
