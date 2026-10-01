// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A SUPERSEDED answer drops only from the superseding offset (M13.26h review
 * round 2, P8, T19): an entry of the group below it, appended after the
 * report -- REPLICA batches of one group may arrive out of order -- is not
 * superseded (ADR-0081 §5 invariant (b)), and may be acked.
 */
class HeldReportsSupersededTest {

    @Test
    void anENTRYBelowTheSupersedingOffsetAppendedAfterTheReportIsKept() throws Exception {
        FastJournal journal = HeldReportsTest.journal(
                HeldReportsTest.entry(HeldReportsTest.A, 7, 0, 12, 2));
        journal.append(HeldReportsTest.entry(HeldReportsTest.A, 7, 0, 10, 2));
        journal.sync();

        HeldReports.apply(journal, new FastFrame.HeldStatus(List.of(new FastFrame.StreamStatus(
                HeldReportsTest.A, 0, List.of(new FastFrame.GroupStatus(7, 0, 12,
                        FastFrame.Status.SUPERSEDED))))));

        assertThat(journal.held()).extracting(e -> e.firstOffset())
                .as("10-11 is below the decision's resume offset: not superseded")
                .containsExactly(10L);
    }
}
