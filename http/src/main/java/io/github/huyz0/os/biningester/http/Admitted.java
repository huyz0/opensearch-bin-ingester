// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import java.util.List;

/**
 * A bulk request's admission permit, given back while it waits for
 * durability and taken again for its next chunk (M11.7, M10 review F2),
 * and its index's quota slot, held until the request ends (M11.8). Moved out
 * of {@link BulkService} unchanged by M11.24c.
 *
 * <p>⚠️ THE PERMIT BOUNDS LOAD, NOT WAITING. Held across the durable-ack
 * wait, a budget of 256 capped a pod at 256 producers parked on a flush --
 * at a low rate, where a flush is seconds away, that is a cap on
 * concurrency with nothing to protect. So each chunk gives the permit back
 * the moment its records are buffered.
 *
 * <p>⚠️ A LATER CHUNK WAITS FOR ONE RATHER THAN BEING REFUSED: the prefix
 * already appended is durable-bound, so a mid-body {@code 429} would have
 * the producer resend records that landed. It queues under the same fair
 * share instead ({@link LaneAdmission#acquire}). The permit for a chunk
 * is taken before the chunk's first record is held, so a request waiting
 * for its permit holds no parsed records.
 *
 * <p>⚠️ THE INDEX's SLOT IS NOT GIVEN BACK AT BUFFERED (M11.8 review P1).
 * It is what bounds a quota'd index's debt: a request admitted with tokens
 * is never cut off, so the debt concurrent admission can build is at most
 * {@code maxInFlightPerIndex} bodies only while the cap counts every
 * request not yet finished (ADR-0078 decision 2a). Returned at buffered,
 * it would count only the requests parsing at one instant, and an index
 * could be admitted thousands of bodies deep.
 */
final class Admitted {
    private final LaneAdmission admission;
    private final byte lane;
    private final IndexQuotas.Ticket quota;
    private LaneAdmission.Permit current;

    Admitted(LaneAdmission admission, LaneAdmission.Permit first, byte lane,
            IndexQuotas.Ticket quota) {
        this.admission = admission;
        this.current = first;
        this.lane = lane;
        this.quota = quota;
    }

    void holdForNextChunk() throws InterruptedException {
        if (current == null) {
            current = admission.acquire(lane);
        }
    }

    /** Charges a chunk to the index's quota as it is appended (ADR-0078 decision 2). */
    void charge(List<SegmentRecord> chunk) {
        quota.charge(chunk);
    }

    /** A chunk is buffered: the pod's permit goes back, the index's slot does not. */
    void buffered() {
        if (current != null) {
            current.release();
            current = null;
        }
    }

    /** The request has ended, however: everything it holds goes back. */
    void release() {
        buffered();
        quota.release();
    }
}
