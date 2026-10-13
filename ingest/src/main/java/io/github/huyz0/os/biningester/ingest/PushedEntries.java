// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastKey;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;

/**
 * PUBLISHed fast entries, delivered to this pod's subscribers at their
 * offsets under their fast keys before their upload (ADR-0081 §6, ADR-0082
 * §6; M13.30j): each one run in a segment of its own, held in memory --
 * inline within the zone, proxied across it (M13.30f serves the key), and
 * never {@code direct}, since the key names no object to sign. The committed
 * run that follows overlaps it, and the consumer trims (ADR-0085).
 *
 * <p>⚠️ SPLIT FROM {@link DefaultIngest}, which is held at its split ceiling.
 */
final class PushedEntries {

    private final String prefix;
    private final Clock clock;
    private final SubscriptionHub hub;
    private final SegmentServing serving;

    PushedEntries(String prefix, Clock clock, SubscriptionHub hub, SegmentServing serving) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.hub = Objects.requireNonNull(hub, "hub");
        this.serving = withoutDirect(Objects.requireNonNull(serving, "serving"));
    }

    void deliver(FastJournalRecord.Entry entry) throws IOException {
        long now = clock.millis();
        SegmentWriter writer = new SegmentWriter();
        entry.records().forEach(r -> writer.add(entry.key(), r, now));
        hub.publishPushed(FastKey.of(prefix, entry.epoch(), entry.assignedAfter(), entry.key(),
                entry.firstOffset()), entry.key(), entry.firstOffset(), entry.records().size(),
                writer.toByteArray(now), serving);
    }

    /** {@code serving} as a backend that cannot presign: never {@code direct}. */
    static SegmentServing withoutDirect(SegmentServing serving) {
        Capabilities c = serving.capabilities();
        return new SegmentServing(serving.policy(), new Capabilities(c.conditionalWrites(),
                c.batchDelete(), false, c.maxKeyBytes(), c.minPartSize(), c.costs()),
                serving.proxy());
    }
}
