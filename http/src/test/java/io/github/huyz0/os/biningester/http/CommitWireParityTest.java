// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitRequestFrame;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@code CommitRequest} and {@code CommitRequestFrame} stay in step (M8.34,
 * ADR-0053).
 *
 * <p>⚠️ **THE COMPILER IS SILENT IF THEY DRIFT**: neither record references the
 * other, because {@code format} may not depend on {@code sequencer}. A field
 * added to one alone is a forwarded commit that drops whatever it carried. So
 * this derives the check from the records themselves, in the one module that
 * sees both. {@code PeerCommitTest.theCONVERSIONBothWaysPreservesEveryField}
 * is the other half: that the conversion carries every component across.
 */
class CommitWireParityTest {

    private static List<String> shape(Class<? extends Record> type) {
        return Arrays.stream(type.getRecordComponents())
                .map((RecordComponent c) -> c.getName() + ":" + c.getGenericType().getTypeName())
                .toList();
    }

    @Test
    void theTWORecordsHaveTheSAMEComponentsInTheSAMEOrder() {
        assertThat(shape(CommitRequestFrame.class))
                .as("⚠️ A COMPONENT ON ONE RECORD AND NOT THE OTHER is a field a forwarded "
                        + "commit drops in silence; the wire record and the in-process one "
                        + "change together or not at all")
                .isEqualTo(shape(CommitRequest.class));
    }
}
