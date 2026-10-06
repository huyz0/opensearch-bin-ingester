// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The zones of the pods this node exchanges fast frames with (M13.64): by
 * endpoint, for a frame it sends, and by pod UID, for a frame it answers --
 * so {@code CrossAzBytes} counts each against its peer's zone, never as
 * unknown. ⚠️ THE NODE's WHOLE {@code CrossAzBytes} DIRECTORY: a commit
 * forward or an inbox drain to an endpoint learned here is attributed too.
 *
 * <p>⚠️ THE LOOKUPS ARE MAPS, NEVER I/O ({@code CrossAzBytes.PeerAz}'s rule):
 * what they hold is learned from the incarnation a JOIN or DEPART carries, and
 * from a roster read before a send.
 *
 * <p>⚠️ A ZONE IS AS NEW AS THE EPOCH IT WAS LEARNED AT (its review round 1,
 * P2): a frame of a newer term to the same endpoint reads that term's roster,
 * so an endpoint reused by a pod in another zone is not counted at the old
 * one -- same-AZ for a cross-AZ byte is the unsafe direction.
 *
 * <p>⚠️ A MISSING ROSTER IS READ AGAIN ON THE NEXT SEND (its review round 1,
 * P1): a term's lease is taken before its roster is written, and a JOIN sent
 * in between must not leave the term counted unknown. A roster that exists
 * and does not name the endpoint is read once per (endpoint, epoch); either
 * way the reads follow the sends to an unlearned endpoint -- the joiner's,
 * paced by the lease watch -- never the records.
 */
final class PeerZones {

    private record Zone(String az, long epoch) {
    }

    private final Map<String, Zone> byEndpoint = new ConcurrentHashMap<>();
    private final Map<String, String> byUid = new ConcurrentHashMap<>();
    private final Set<String> readWithout = ConcurrentHashMap.newKeySet();

    /** Learns {@code pod}'s zone, as of {@code epoch}, under its endpoint and its UID. */
    void learn(Roster.Incarnation pod, long epoch) {
        if (pod.az() == null || pod.az().isBlank()) {
            return;
        }
        if (pod.endpoint() != null && !pod.endpoint().isBlank()) {
            byEndpoint.merge(pod.endpoint(), new Zone(pod.az(), epoch),
                    (old, now) -> now.epoch() >= old.epoch() ? now : old);
        }
        byUid.put(pod.podUid(), pod.az());
    }

    Optional<String> ofEndpoint(String endpoint) {
        return endpoint == null ? Optional.empty()
                : Optional.ofNullable(byEndpoint.get(endpoint)).map(Zone::az);
    }

    Optional<String> ofUid(String podUid) {
        return podUid == null ? Optional.empty() : Optional.ofNullable(byUid.get(podUid));
    }

    /**
     * {@code inner}, learning before a send the zones of the roster the
     * frame's epoch names, unless the endpoint's zone is already that new.
     */
    TermJoiner.Transport learning(TermJoiner.Transport inner, BinStore store, String prefix) {
        return (endpoint, frame) -> {
            long epoch = FastFrame.header(frame).epoch();
            Zone known = endpoint == null ? null : byEndpoint.get(endpoint);
            if (known == null || known.epoch() < epoch) {
                learnRoster(endpoint, epoch, store, prefix);
            }
            return inner.exchange(endpoint, frame);
        };
    }

    private void learnRoster(String endpoint, long epoch, BinStore store, String prefix) {
        if (readWithout.contains(endpoint + "@" + epoch)) {
            return;
        }
        Roster roster;
        try (InputStream in = store.get(Roster.key(prefix, epoch))) {
            roster = Roster.decode(in.readAllBytes());
        } catch (IOException | RuntimeException notYet) {
            return; // not written yet, or unreadable: counted unknown, read again next send
        }
        learn(roster.leader(), epoch);
        for (Roster.Member member : roster.members()) {
            learn(member.incarnation(), epoch);
        }
        Zone learned = byEndpoint.get(endpoint);
        if (learned == null || learned.epoch() < epoch) {
            readWithout.add(endpoint + "@" + epoch);
        }
    }
}
