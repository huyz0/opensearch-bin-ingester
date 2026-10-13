// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastPublishFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A PUBLISH taken by a subscribing pod (M13.30i; ADR-0081 §6): from the
 * leader of a term it joined only, that term's entries only, each handed to
 * the hub in order -- and one the hub cannot take costs the rest nothing.
 */
class FastPublishHandlerTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);

    private static FastJournalRecord.Entry entry(long epoch, long offset) {
        return new FastJournalRecord.Entry(epoch, S, offset, 1, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), offset),
                List.of(new SegmentRecord("d" + offset, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    private static JoinedTerms joined() {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(3, 0, "uid-l", "l:1");
        return joined;
    }

    private static FastFrame.Header from(String sender, long epoch) {
        return new FastFrame.Header(FastPublishFrame.KIND_PUBLISH, epoch, sender, "uid-w");
    }

    @Test
    void theLEADERsPublishIsHandedToTheHubInOrder() {
        List<Long> delivered = new ArrayList<>();

        FastFrame.Body answer = FastPublishHandler.answer(joined(), e -> {
            if (e.firstOffset() == 4) {
                throw new IOException("the hub could not take it");
            }
            delivered.add(e.firstOffset());
        }, from("uid-l", 3), new FastPublishFrame.Publish(List.of(entry(3, 4), entry(3, 5),
                entry(3, 6))));

        assertThat(answer).isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(delivered).as("the rest, in order").containsExactly(5L, 6L);
    }

    @Test
    void aPUBLISHFromAnyoneButTheLeaderOrWithAnotherTermsEntryDeliversNothing() {
        List<Long> delivered = new ArrayList<>();
        FastPublishHandler.Pushed pushed = e -> delivered.add(e.firstOffset());

        FastFrame.Body stranger = FastPublishHandler.answer(joined(), pushed,
                from("uid-x", 3), new FastPublishFrame.Publish(List.of(entry(3, 4))));
        FastFrame.Body unjoined = FastPublishHandler.answer(joined(), pushed,
                from("uid-l", 4), new FastPublishFrame.Publish(List.of(entry(4, 4))));
        FastFrame.Body mixed = FastPublishHandler.answer(joined(), pushed,
                from("uid-l", 3), new FastPublishFrame.Publish(List.of(entry(3, 4),
                        entry(2, 5))));
        // ⚠️ review round 1, T1: a later term's entry too, not only an earlier one's.
        FastFrame.Body ahead = FastPublishHandler.answer(joined(), pushed,
                from("uid-l", 3), new FastPublishFrame.Publish(List.of(entry(3, 4),
                        entry(4, 5))));

        assertThat(List.of(stranger, unjoined, mixed, ahead)).allSatisfy(a ->
                assertThat(((FastFrame.Refused) a).reason())
                        .isEqualTo(FastFrame.Reason.NOT_ROSTERED));
        assertThat(delivered).isEmpty();
    }
}
