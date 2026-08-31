// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One page of a listing — one request, one page.
 *
 * <p>⚠️ EXPLICIT PAGING, not a lazy {@code Stream}. A stream hides paging inside
 * the backend's spliterator, so the counting decorator (R9) sees a single
 * invocation whether the backend issued one request or ten thousand — and
 * {@code assertThat(listRequests()).isZero()} would pass over thousands of
 * billed LISTs. Cost rule R9 requires every store op to be counted, and a
 * request the meter cannot see is the one that shows up on the bill.
 *
 * <p>⚠️ It also restores the error contract. Errors after the first page of a
 * stream can only surface as an unchecked exception from a terminal operation,
 * which is exactly what declaring {@code IOException} on the SPI exists to
 * prevent; here each page is a checked call.
 *
 * <p>⚠️ And a LIST ceiling can refuse (ADR/FR-21, M1.3b). A governor cannot
 * refuse page 500 of a stream without throwing from inside a terminal op.
 *
 * @param objects this page, in lexicographic key order
 * @param nextStartAfter resume point, or empty when the listing is complete
 */
public record ListPage(List<ObjectStat> objects, Optional<String> nextStartAfter) {

    public ListPage {
        objects = List.copyOf(Objects.requireNonNull(objects, "objects"));
        Objects.requireNonNull(nextStartAfter, "nextStartAfter");
    }

    /** The final page. */
    public static ListPage last(List<ObjectStat> objects) {
        return new ListPage(objects, Optional.empty());
    }
}
