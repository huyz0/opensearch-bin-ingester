// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import binjava.format.Lease;
import binjava.sequencer.LeaseChallenge;
import java.net.URI;
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
 * every slice, or it is not ready and not terminating, which is what a
 * crashed container looks like while the kubelet restarts it.
 */
public final class EndpointSliceView implements LeaseChallenge {

    private record Member(String name, List<String> addresses, boolean ready,
            boolean terminating) {
    }

    private final Map<String, List<Member>> slices = new HashMap<>();
    private final Set<String> namesSeenReady = new HashSet<>();
    private final Set<String> addressesSeenReady = new HashSet<>();

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
                        namesSeenReady.add(member.name());
                        addressesSeenReady.addAll(member.addresses());
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
            members.add(new Member(name, List.copyOf(addresses), ready, terminating));
        }
        return members;
    }

    /**
     * {@inheritDoc}
     *
     * <p>⚠️ **BY POD NAME FIRST, AND BY ADDRESS ONLY WHERE NO ENDPOINT CARRIES
     * THE HOLDER'S NAME.** An address outlives its pod: a new pod can be given
     * the IP of one that died, and matched by address alone it would inherit
     * the dead pod's "seen ready" and be challenged the moment it took the
     * term, before its own readiness had passed.
     */
    @Override
    public synchronized boolean holderGone(Lease current) {
        String name = current.holderPodId();
        String host = hostOf(current.holderEndpoint());
        boolean named = slices.values().stream().flatMap(List::stream)
                .anyMatch(member -> member.name().equals(name));
        if (named || namesSeenReady.contains(name)) {
            return namesSeenReady.contains(name) && goneBy(member -> member.name().equals(name));
        }
        return !host.isEmpty() && addressesSeenReady.contains(host)
                && goneBy(member -> member.addresses().contains(host));
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

    private static String hostOf(String endpoint) {
        try {
            String host = URI.create(endpoint).getHost();
            return host == null ? "" : host;
        } catch (IllegalArgumentException notAUri) {
            return "";
        }
    }
}
