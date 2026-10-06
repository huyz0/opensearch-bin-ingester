// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.PendingPool;
import io.github.huyz0.os.biningester.ingest.RoutedIngest;
import io.github.huyz0.os.biningester.ingest.SegmentPrefetcher;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;

/**
 * The default write path's construction for {@link Assembly}: the writer, its
 * durable-segment hints to peers, and the prefetcher reading through its proxy
 * (M13.1b).
 *
 * <p>⚠️ SPLIT OUT so fast mode's write path lands as a sibling rather than
 * growing {@code Assembly} past its ceiling (M13 criterion 1).
 */
final class WritePathAssembly {

    /** What {@link #create} built. */
    record WritePath(DefaultIngest ingest, SegmentPrefetcher prefetcher,
            DurableSegmentSignalSender signalSender) {
    }

    /** How long a routed write waits for its index's registration: ADR-0015's default. */
    static final Duration PENDING_TIMEOUT = Duration.ofSeconds(5);

    /**
     * ⚠️ THE BOUND ON WHAT ONE UNREGISTERED INDEX MAY HOLD (ADR-0015): one
     * default segment's worth, so a producer racing the plugin is absorbed and
     * one writing to an index nobody registers is refused rather than grown.
     */
    static final long PENDING_BYTES_PER_INDEX = IngestConfig.DEFAULT_MAX_SEGMENT_BYTES;

    private WritePathAssembly() {
    }

    /**
     * ⚠️ THE ROUTED PATH IS WHAT THE FRONT DOOR IS HANDED (M8.32, FR-13).
     * Plain `DefaultIngest` has no catalog: it refuses every routed write,
     * and accepts an explicit partition the index does not have.
     */
    static RoutedIngest routed(DefaultIngest ingest, IndexCatalog catalog, Clock clock) {
        return new RoutedIngest(ingest, catalog,
                new PendingPool(clock, PENDING_TIMEOUT, PENDING_BYTES_PER_INDEX),
                PENDING_TIMEOUT, clock);
    }

    /**
     * ⚠️ {@code peerHints} IS THE VIEW AS PASSED, possibly null, for the signal
     * sender's own choice; {@code peers} is the one the graph uses, never null.
     */
    static WritePath create(ServerConfig config, BinStore store, FleetSequencer sequencer,
            SubscriptionHub hub, Clock clock, IndexCatalog catalog, EndpointSliceView peerHints,
            EndpointSliceView peers, CrossAzBytes crossAz,
            DurableSegmentSignalSender.PeerPost signalPost, IndexCostLedger costLedger,
            CostGovernor governor) throws IOException {
        try {
            DurableSegmentSignalSender signalSender =
                    EndpointMembership.signalSender(config, peerHints, crossAz, signalPost);
            DefaultIngest ingest = new DefaultIngest(config.ingest(), store, config.prefix(),
                    config.podId(), sequencer, hub, clock,
                    name -> CatalogStreams.streamFor(catalog, name),
                    segmentKey -> {
                        if (signalSender != null) {
                            signalSender.send(new DurableSegmentSignalFrame(config.podId(),
                                    config.az(), segmentKey), peers.readyEndpoints());
                        }
                    }, costLedger);
            SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                    new EndpointMembership(config, peers), ingest.segmentProxy(),
                    governor::discretionaryAllowed);
            return new WritePath(ingest, prefetcher, signalSender);
        } catch (RuntimeException | IOException failed) {
            // ⚠️ THE TERM IS ALREADY TAKEN AT THIS POINT, and if this throws
            // nothing will ever hold a reference to the sequencer again.
            // `new Leadership(election)` ELECTS IN ITS CONSTRUCTOR -- it
            // acquires the lease and starts the renewer -- so a failure here
            // leaves the lease naming a node that failed to start, renewed
            // every interval for the life of the JVM. That is the failure
            // `FleetSequencer`'s own constructor comment guards a null argument
            // against, one level up: the fleet stops committing and nothing
            // says why. It is also an NFR-2 defect -- one `putIfMatch` per
            // renew interval, for ever, from a node that is not running.
            // ⚠️ AND THE TRIGGER IS CONFIGURED RATHER THAN HYPOTHETICAL:
            // `DefaultIngest` refuses at startup when `direct` is enabled over
            // a backend that cannot sign (M5.43), as the memory and
            // local-filesystem backends cannot.
            try {
                sequencer.close();
            } catch (Exception suppressed) {
                failed.addSuppressed(suppressed);
            }
            throw failed;
        }
    }
}
