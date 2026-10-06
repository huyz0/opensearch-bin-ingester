// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A leader node answers a COMMIT through the term it leads (M13.27s): refused
 * {@code NOT_FAST} without a fast journal, and with one reaching the term --
 * held, until M13.27l records a {@code wal_quorum} to assign under.
 */
class FastCommitServedTest {

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static Map<String, String> settings() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "");

    private static FastWriteFrame.Commit commit() {
        return new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey("p",
                new UUID(9, 9), 1), List.of(new FastWriteFrame.CommitRun(
                        new RunKey(new UUID(1, 1), 0), List.of(new SegmentRecord("d",
                                OpType.INDEX, OptionalLong.empty(), new byte[] {1})))));
    }

    private FastFrame.Body send(FastFrame.Body body) throws Exception {
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());
        return FastFrame.decode(transport.exchange("http://localhost:" + node.port(),
                FastFrame.encode(1, POD.podUid(), "uid-pod1", body))).body();
    }

    @Test
    void aLEADERWithoutAFastJournalRefusesACOMMITNotFast() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings()), Clock.systemUTC());

        assertThat(((FastFrame.Refused) send(commit())).reason())
                .isEqualTo(FastFrame.Reason.NOT_FAST);
    }

    @Test
    void aLEADERWithAFastJournalHoldsARosteredWritersCOMMITUntilItsIndexIsRecorded()
            throws Exception {
        Map<String, String> settings = settings();
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
        assertThat(send(new FastFrame.Join(POD, FastFrame.Held.NONE)))
                .as("the premise: the writer is rostered").isInstanceOf(FastFrame.Joined.class);

        FastFrame.Refused held = (FastFrame.Refused) send(commit());

        // ⚠️ REACHED THE TERM: an unrouted kind gets no answer at all, and an
        // unrostered writer NOT_ROSTERED -- this is the term's empty record.
        assertThat(held.reason()).isEqualTo(FastFrame.Reason.BACKPRESSURE);
        assertThat(held.text()).contains("wal_quorum not yet recorded");
    }
}
