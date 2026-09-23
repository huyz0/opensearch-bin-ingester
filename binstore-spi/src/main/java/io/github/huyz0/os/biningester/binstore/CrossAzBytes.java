// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bytes this pod sent to a peer whose availability-zone LABEL differs from its
 * own (M9.2, NFR-5, cost.md R12).
 *
 * <p>⚠️ **IT IS KEYED BY LABEL, NEVER BY POD.** Two pods in this pod's own AZ
 * are both same-AZ however different their ids and endpoints are, and one pod
 * in another AZ is cross-AZ however similar they look. A counter keyed by the
 * peer is the defect M9's test plan names for this row, and it is invisible in
 * a single-peer run: every byte to "some other pod" reads as cross-AZ, so a
 * steady-state number comes out plausible and wrong.
 *
 * <p>⚠️ **THE COUNTER IS NODE-WIDE, NOT PER-TRANSPORT**, and that is a
 * decision rather than an accident. NFR-5 is one ratio over one pod — bytes to
 * a differing AZ against producer bytes accepted — so a per-transport counter
 * would have to be summed by whoever reports it, and the sum is where a
 * transport nobody remembered to add goes missing. One instance per ingester
 * process is constructed at the composition root and handed to every peer
 * transport; the per-transport split below is an ATTRIBUTION of that one
 * total, not a second source of it.
 *
 * <p>⚠️ **AN UNKNOWN PEER AZ IS COUNTED AS CROSS-AZ, AND SAID TO BE UNKNOWN.**
 * The alternative — not counting it — is a silent zero in a cost report, which
 * is a lie in the one direction that matters: NFR-5 is an upper bound, and
 * bytes nobody could attribute are exactly the bytes a build would drop to
 * stay under it. So {@link #sent} with a null or blank {@code peerAz} adds to
 * {@link #crossAzBytes()} AND to {@link #unknownPeerBytes()}, and a report that
 * does not say how much of its total was assumed rather than attributed is
 * reporting a number it did not measure. ⚠️ A consumer that sends no AZ on its
 * poll is the common case today, so on an unlabelled fleet this counter reads
 * "everything is cross-AZ" rather than "nothing is" — loud, and the safe side.
 *
 * <p>⚠️ **NO AZ EVER BECOMES A METRIC LABEL HERE.** observability.md rule 1's
 * allow-list has no {@code az} and no {@code transport}: the exported series is
 * {@code binstore_cross_az_bytes_total{direction}}, and everything this class
 * holds is an in-memory counter read by a report or a test (rule 2 — count
 * everywhere, export almost nothing). ⚠️ And it costs no object-store request:
 * it observes bytes a socket was going to carry anyway.
 *
 * <p>⚠️ **WHY A PEER-SOCKET COUNTER LIVES IN THE OBJECT-STORE SPI.** It is not
 * a store thing, and the module's name says store. It is here because this is
 * the only module that {@code sequencer}, {@code ingest}, {@code http} and the
 * composition root can all see (architecture.md § Modules), because the metric
 * observability.md rule 3 names for it is {@code binstore_cross_az_bytes_total}
 * beside {@code binstore_requests_total}, and because the report M9.10 writes
 * puts these bytes and {@link StoreCounts} in one file — a second home for the
 * second half of one cost report is how the two start disagreeing. The
 * alternative was {@code format}, which is the pure FORMATS module and would
 * have been a worse fit for a mutable counter.
 *
 * <p>⚠️ **IT COUNTS WHAT LEAVES A PEER SOCKET, NOT WHAT LEAVES THE POD.**
 * Object-store traffic is counted by {@link CountingBinStore} and priced by
 * {@link CostMeter}; a store read that happens to cross a zone is the store's
 * arithmetic, not this one's. Adding it here would double-count the largest
 * term in both reports.
 */
public final class CrossAzBytes {

    /**
     * The peer sockets M9's criterion 6 names, one constant each.
     *
     * <p>⚠️ **THE SPLIT IS CRITERION 6's OWN LIST**, so that "which transport
     * spends the bytes" is answerable without a second instrument: the proxy
     * read path a consumer in another AZ takes (cost.md rules 10-11, the
     * largest term), {@code inline} push payloads, the commit forward, and the
     * inbox drain.
     */
    public enum Transport {
        /**
         * A segment served to a consumer in {@code proxy} mode.
         *
         * <p>⚠️ **TODAY THIS IS THE EVENT FRAME, NOT THE SEGMENT.** A proxy
         * push carries no inline bytes and no route yet serves the segment
         * itself to a consumer, so what is counted here is the frame that
         * names it. When that route lands its bytes are counted here, on the
         * same socket, by the same call.
         */
        PROXY_READ,

        /** A segment carried inline in a poll answer ({@code inline} mode). */
        INLINE_PUSH,

        /**
         * Everything else on a consumer's poll answer: the retained-floor
         * frame, a {@code direct} grant, and the framing.
         */
        CONSUMER_POLL,

        /** A commit forwarded to the leaseholder ({@code SequencerTransport}). */
        COMMIT_FORWARD,

        /** A peer asked to drain its inbox (ADR-0058). */
        INBOX_DRAIN,

        /** A key-only durable-segment hint sent to a remote-AZ cache owner. */
        DURABLE_SEGMENT_SIGNAL
    }

    /**
     * Where a peer's endpoint gets its AZ label, for a transport that holds an
     * address rather than a zone.
     *
     * <p>⚠️ **NOT A SEAM AND NOT I/O.** It is a lookup over membership the
     * composition root already holds; an implementation that opened a socket
     * would be doing it on the commit path.
     */
    @FunctionalInterface
    public interface PeerAz {

        /** ⚠️ Empty means "not known", which {@link #sentTo} counts as cross-AZ. */
        Optional<String> of(String endpoint);
    }

    /**
     * A counter that records nothing and ⚠️ **REFUSES EVERY READING**, for the
     * constructions that predate this class.
     *
     * <p>⚠️ **REFUSES, RATHER THAN ANSWERING 0.** Review MEASURED the earlier
     * shape and it was the defect this class's header names: the pre-M9.2
     * constructors of {@code SubscriptionService} and
     * {@code HttpSequencerTransport} install this instance, and with readable
     * accessors a report taken from a node built that way would print
     * "0 cross-AZ bytes" having counted none — the silent zero, reached
     * through the very escape hatch documented as preventing it. So every
     * accessor on THIS instance throws, which is the same choice
     * {@link CostMeter} makes for a per-MiB figure with no bytes behind it:
     * ABSENT is a reading a caller must handle, 0 is one it will publish.
     *
     * <p>⚠️ **THE LEGACY CONSTRUCTORS STAY, AND THIS IS WHY.** Twenty-odd
     * tests and one soak harness build those transports without a zone, and
     * requiring a label from each of them would put a fabricated {@code az-a}
     * in every one — a test fixture that looks like a measurement. What must
     * never exist is a node that SERVES traffic with one, and that is closed
     * where it matters: {@code IngesterNode} builds a real counter from
     * {@code pod.az} and passes it to both the transport and the door.
     */
    public static CrossAzBytes untracked() {
        // ⚠️ A FRESH ONE EACH TIME, NOT A STATIC SINGLETON. It holds no state
        // anyone reads, so sharing buys nothing -- and a static field holding
        // an instance of this class is what the mutation engine could not
        // install a mutant over: its analysis JVM died in <clinit>, and a
        // mutant nobody can run is a hole in the gate rather than a score.
        return new CrossAzBytes("untracked", false);
    }

    private final String localAz;
    private final PeerAz directory;
    private final boolean tracking;
    private final Map<Transport, LongAdder> cross = new EnumMap<>(Transport.class);
    private final Map<Transport, LongAdder> same = new EnumMap<>(Transport.class);
    private final LongAdder unknownPeer = new LongAdder();

    /** With nothing known about any endpoint, so {@link #sentTo} counts cross-AZ. */
    public CrossAzBytes(String localAz) {
        this(localAz, endpoint -> Optional.empty());
    }

    /**
     * @param localAz this pod's zone label, as {@code pod.az} configured it
     * @param directory where an endpoint's zone comes from
     */
    public CrossAzBytes(String localAz, PeerAz directory) {
        this(requireLabel(localAz), directory, true);
    }

    private CrossAzBytes(String localAz, boolean tracking) {
        this(localAz, endpoint -> Optional.empty(), tracking);
    }

    private CrossAzBytes(String localAz, PeerAz directory, boolean tracking) {
        this.localAz = localAz;
        this.directory = Objects.requireNonNull(directory, "directory");
        this.tracking = tracking;
        for (Transport transport : Transport.values()) {
            cross.put(transport, new LongAdder());
            same.put(transport, new LongAdder());
        }
    }

    private static String requireLabel(String localAz) {
        if (localAz == null || localAz.isBlank()) {
            // ⚠️ REFUSED RATHER THAN DEFAULTED. A pod with no zone cannot tell
            // a cross-AZ byte from a same-AZ one, and a counter that guessed
            // would answer 0 for a fleet spread over three zones.
            throw new IllegalArgumentException("a cross-AZ counter needs this pod's az label");
        }
        return localAz.trim();
    }

    /** This pod's zone label. */
    public String localAz() {
        requireTracking();
        return localAz;
    }

    /**
     * Records {@code bytes} sent over {@code transport} to a peer labelled
     * {@code peerAz}.
     *
     * <p>⚠️ **A NULL OR BLANK {@code peerAz} IS CROSS-AZ AND UNKNOWN**, never
     * silently same-AZ — see this class's header.
     *
     * @throws IllegalArgumentException if {@code bytes} is negative; zero is
     *     legal, because an empty poll answer is a real event
     */
    public void sent(Transport transport, String peerAz, long bytes) {
        Objects.requireNonNull(transport, "transport");
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes sent is not negative: " + bytes);
        }
        if (!tracking || bytes == 0) {
            return;
        }
        if (peerAz == null || peerAz.isBlank()) {
            unattributed(transport, bytes);
            return;
        }
        (localAz.equals(peerAz.trim()) ? same : cross).get(transport).add(bytes);
    }

    /**
     * Bytes to a peer that named no zone: cross-AZ, and separately reported as
     * ASSUMED rather than measured.
     */
    private void unattributed(Transport transport, long bytes) {
        unknownPeer.add(bytes);
        cross.get(transport).add(bytes);
    }

    /** The same, resolving {@code endpoint} through the directory first. */
    public void sentTo(Transport transport, String endpoint, long bytes) {
        sent(transport, endpoint == null ? null : directory.of(endpoint).orElse(null), bytes);
    }

    /** Bytes sent to a peer whose label differs from {@link #localAz()}. */
    public long crossAzBytes() {
        requireTracking();
        return total(cross);
    }

    /** The same, over one transport. */
    public long crossAzBytes(Transport transport) {
        requireTracking();
        return cross.get(Objects.requireNonNull(transport, "transport")).sum();
    }

    /** Bytes sent to a peer carrying this pod's own label. */
    public long sameAzBytes() {
        requireTracking();
        return total(same);
    }

    /** The same, over one transport. */
    public long sameAzBytes(Transport transport) {
        requireTracking();
        return same.get(Objects.requireNonNull(transport, "transport")).sum();
    }

    /**
     * The part of {@link #crossAzBytes()} that was ASSUMED cross-AZ because the
     * peer carried no label. ⚠️ A report quoting the total without this one is
     * quoting an upper bound as a measurement.
     */
    public long unknownPeerBytes() {
        requireTracking();
        return unknownPeer.sum();
    }

    /**
     * ⚠️ Refuses on an untracked counter, so no path can report a
     * measured-looking zero from an instance that counted nothing.
     */
    private void requireTracking() {
        if (!tracking) {
            throw new IllegalStateException("this counter recorded nothing (it was built by a "
                    + "construction that carries no pod.az), so it has no reading to give -- "
                    + "0 here would be a measurement nobody took");
        }
    }

    private static long total(Map<Transport, LongAdder> by) {
        long sum = 0;
        for (LongAdder adder : by.values()) {
            sum += adder.sum();
        }
        return sum;
    }
}
