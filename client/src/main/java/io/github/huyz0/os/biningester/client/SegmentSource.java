// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.Grant;
import java.io.IOException;

/**
 * Where a {@code direct} consumer gets a segment's bytes (M5.45g, FR-6).
 *
 * <p>⚠️ NOT A {@code BinStore}, AND THAT IS THE WHOLE REASON THIS TYPE EXISTS.
 * ADR-0023 keeps {@code io.github.huyz0.os.biningester.binstore} out of {@code client} and
 * {@code plugin} — the plugin ships inside the OpenSearch node process and its
 * dependency surface is deliberately minimal — so {@code direct} needs a byte
 * source that is not the store SPI. M5.44 made the value carryable:
 * {@link Grant} lives in {@code format}, which {@code client} already depends
 * on.
 *
 * <p>⚠️ NO RANGE PARAMETER, AND THAT IS ADR-0044's DECISION RATHER THAN AN
 * OVERSIGHT. An earlier draft of M5.45c wrote the signature as a grant PLUS a
 * byte range, and no caller can fill one: {@code SegmentReader.open} checks the
 * footer magic BEFORE the directory, so a prefix ending at the last wanted run
 * throws, and {@code [0, objectLen)} is not nameable because nothing the
 * consumer holds carries the object length — {@code SegmentKey} carries
 * {@code h<headerLen>}, the HEADER's extent. A parameter no caller can fill
 * reads as an implemented capability. ⚠️ A range DOES become nameable for the
 * header alone, and a bounded read is two requests; that is **M5.66**, which
 * also owns whether the event's coordinates stay on the wire at all.
 *
 * <p>⚠️ THE PRODUCTION IMPLEMENTATION IS {@code HttpSegmentSource} (M8.31),
 * and it is only ever handed out wrapped in the plugin's per-node cache. None
 * shipped in M5 (ADR-0044 (a)): no backend could presign until M8.18. It was
 * also gated on **M5.45h**, because one {@code Delivery} per RUN means one fetch per
 * run however well an implementation merges internally, and a catch-up node
 * holding ~400 runs of one segment would issue ~400 whole-object GETs —
 * shards-per-node, which non-negotiable 6 forbids by name and which nothing in
 * the tree can see, since consumer-side GETs are invisible to the ingester's
 * {@code CountingBinStore}.
 */
@FunctionalInterface
public interface SegmentSource {

    /**
     * The whole segment the grant is scoped to.
     *
     * <p>⚠️ A FAILURE MUST THROW RATHER THAN YIELD NOTHING. An expired grant, a
     * 403 and a truncated body are the normal failures here, and an
     * implementation that swallowed one and returned an empty array would have
     * {@code ConsumerClient} report a healthy stream that silently skipped a
     * window.
     *
     * @param grant the signed URL from the delivery, never {@code null}
     * @throws IOException if the bytes could not be fetched
     */
    byte[] fetch(Grant grant) throws IOException;
}
