// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What the Kubernetes {@code EndpointSlice} watch says about which ingester
 * pods are up, as evidence for the early lease challenge (M8.13, NFR-9,
 * ADR-0012, ADR-0040).
 *
 * <p>⚠️ **PURE: EVENTS IN, EVIDENCE OUT.** It is fed one watch-event line at a
 * time by {@link EndpointSliceWatch}, which owns the socket, and it touches no
 * I/O, so every rule below is testable without a cluster.
 *
 * <p>⚠️ **A HOLDER IS GONE ONLY IF IT WAS ONCE SEEN READY.** A pod this view
 * has never seen ready gives no evidence either way. A leader that started
 * before the watch's first event arrived, or one whose slice has not
 * propagated yet, would otherwise be challenged the moment it took the term,
 * and leadership would flap for as long as the view lagged.
 *
 * <p>⚠️ **TERMINATING IS NOT GONE.** A pod being deleted keeps its endpoint,
 * marked {@code terminating}, while it drains. That is research 08 §7's
 * graceful shutdown, which ends by RELEASING the lease, and a failover
 * sub-second anyway. Challenging it would fence its last flush and fail the
 * requests the drain exists to finish. What IS evidence: the endpoint has left
 * every slice, or it is not ready and not terminating. That is what a crashed
 * container looks like while the kubelet restarts it, and, since M8.15, a pod
 * whose store calls have stalled or keep failing: a leader in that state
 * cannot commit, so its term going early to a follower that can is intended.
 */
public final class EndpointSliceView implements LeaseChallenge {

    /** A ready endpoint suitable for the peer ring, with one address for a node. */
    public record Endpoint(String podId, String address, String az) {
    }

    private record Member(String name, String uid, List<String> addresses, String az,
            boolean ready, boolean terminating) {
    }

    private final Map<String, List<Member>> slices = new HashMap<>();
    private final Set<String> uidsSeenReady = new HashSet<>();

    /**
     * Applies one line of a watch stream: {@code {"type":..., "object":{...}}}.
     *
     * <p>⚠️ **A LINE IT CANNOT READ IS IGNORED**, not thrown. The view then
     * holds the last state it could read, so the challenge errs towards no
     * evidence, which is waiting out the TTL: the safe direction.
     *
     * @return whether the line was understood
     */
    public synchronized boolean apply(String line) {
        Object parsed;
        try {
            parsed = Json.parse(line);
        } catch (RuntimeException unreadable) {
            return false;
        }
        if (!(parsed instanceof Map<?, ?> event) || !(event.get("type") instanceof String type)
                || !(event.get("object") instanceof Map<?, ?> slice)
                || !(slice.get("metadata") instanceof Map<?, ?> metadata)
                || !(metadata.get("name") instanceof String name)) {
            return false;
        }
        switch (type) {
            case "ADDED", "MODIFIED" -> {
                List<Member> members = members(slice);
                slices.put(name, members);
                for (Member member : members) {
                    if (member.ready()) {
                        if (!member.uid().isBlank()) {
                            uidsSeenReady.add(member.uid());
                        }
                    }
                }
            }
            case "DELETED" -> slices.remove(name);
            default -> {
                // BOOKMARK and ERROR carry no membership
            }
        }
        return true;
    }

    /**
     * Whether any slice lists the incarnation {@code uid}, ready or not
     * (M13.71). ⚠️ NOT READY COUNTS: a starting pod joins before it is ready.
     */
    public synchronized boolean lists(String uid) {
        for (List<Member> members : slices.values()) {
            for (Member member : members) {
                if (member.uid().equals(uid)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Whether the view lists anyone at all: false before its first event. */
    public synchronized boolean hasMembers() {
        for (List<Member> members : slices.values()) {
            if (!members.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /** Current ready endpoints with an AZ label, in a stable order. */
    public synchronized List<Endpoint> readyEndpoints() {
        List<Endpoint> ready = new ArrayList<>();
        for (List<Member> members : slices.values()) {
            for (Member member : members) {
                if (!member.ready() || member.terminating() || member.name().isBlank()
                        || member.az().isBlank()) {
                    continue;
                }
                for (String address : member.addresses()) {
                    ready.add(new Endpoint(member.name(), address, member.az()));
                }
            }
        }
        return ready.stream().sorted(java.util.Comparator.comparing(Endpoint::podId)
                .thenComparing(Endpoint::address).thenComparing(Endpoint::az)).toList();
    }

    private static List<Member> members(Map<?, ?> slice) {
        List<Member> members = new ArrayList<>();
        if (!(slice.get("endpoints") instanceof List<?> endpoints)) {
            return members;
        }
        for (Object entry : endpoints) {
            if (!(entry instanceof Map<?, ?> endpoint)) {
                continue;
            }
            String name = endpoint.get("targetRef") instanceof Map<?, ?> ref
                    && ref.get("name") instanceof String n ? n : "";
            String uid = endpoint.get("targetRef") instanceof Map<?, ?> ref
                    && ref.get("uid") instanceof String u ? u : "";
            String az = endpoint.get("zone") instanceof String zone ? zone : "";
            List<String> addresses = new ArrayList<>();
            if (endpoint.get("addresses") instanceof List<?> list) {
                for (Object address : list) {
                    if (address instanceof String a) {
                        addresses.add(a);
                    }
                }
            }
            Map<?, ?> conditions = endpoint.get("conditions") instanceof Map<?, ?> c ? c : Map.of();
            // ⚠️ AN ABSENT `ready` IS READY: the EndpointSlice API says a nil
            // condition is to be read as true.
            boolean ready = !Boolean.FALSE.equals(conditions.get("ready"));
            boolean terminating = Boolean.TRUE.equals(conditions.get("terminating"));
            members.add(new Member(name, uid, List.copyOf(addresses), az, ready, terminating));
        }
        return members;
    }

    /**
     * {@inheritDoc}
     *
     * <p>⚠️ **BY IMMUTABLE POD UID ONLY.** Pod names and addresses can both be
     * reused by a replacement. A lease written before UIDs were added has no
     * reliable identity and therefore cannot produce early-challenge evidence;
     * it safely waits for expiry.
     */
    @Override
    public synchronized boolean holderGone(Lease current) {
        String uid = current.holderPodUid();
        return !uid.isBlank() && uidsSeenReady.contains(uid)
                && goneBy(member -> member.uid().equals(uid));
    }

    /**
     * Whether no endpoint matching {@code holder} is ready or terminating:
     * absent from every slice, or present and neither.
     */
    private boolean goneBy(java.util.function.Predicate<Member> holder) {
        for (List<Member> members : slices.values()) {
            for (Member member : members) {
                if (holder.test(member) && (member.ready() || member.terminating())) {
                    return false;
                }
            }
        }
        return true;
    }
}
