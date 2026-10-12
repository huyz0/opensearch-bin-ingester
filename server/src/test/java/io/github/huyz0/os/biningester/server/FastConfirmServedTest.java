// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A leader node routes a writer's CONFIRM to the term it leads (M13.27v):
 * refused {@code NOT_FAST} without a fast journal, and with one answered by
 * the term -- never left to the router's 501 for an unrouted kind.
 */
class FastConfirmServedTest {

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

    private static FastWriteFrame.Confirm confirm() {
        return confirm("p");
    }

    private static FastWriteFrame.Confirm confirm(String keyPod) {
        return new FastWriteFrame.Confirm(new FastJournalRecord.IdempotencyKey(keyPod,
                new UUID(9, 9), 1), 0, List.of(new FastWriteFrame.RunAt(
                        new RunKey(new UUID(1, 1), 0), 0)), true);
    }

    private FastFrame.Body send(FastFrame.Body body) throws Exception {
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());
        return FastFrame.decode(transport.exchange("http://localhost:" + node.peerPort(),
                FastFrame.encode(1, POD.podUid(), "uid-pod1", body))).body();
    }

    @Test
    void aLEADERWithoutAFastJournalRefusesACONFIRMNotFast() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings()), Clock.systemUTC());

        assertThat(((FastFrame.Refused) send(confirm())).reason())
                .isEqualTo(FastFrame.Reason.NOT_FAST);
    }

    @Test
    void aLEADERWithAFastJournalAnswersARosteredWritersCONFIRM() throws Exception {
        Map<String, String> settings = settings();
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
        assertThat(send(new FastFrame.Join(POD, FastFrame.Held.NONE)))
                .as("the premise: the writer is rostered").isInstanceOf(FastFrame.Joined.class);

        // ⚠️ REACHED THE TERM: a batch it never assigned counts nowhere, and
        // the writer is told its CONFIRM was received.
        assertThat(send(confirm()))
                .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
    }

    @Test
    void aLEADERRefusesACONFIRMUnderAnotherPodsKeyNotRostered() throws Exception {
        // ⚠️ M13.27v review round 1, T1: an acknowledging stub passed the case
        // above -- only the term itself can refuse this.
        Map<String, String> settings = settings();
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
        assertThat(send(new FastFrame.Join(POD, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);

        FastFrame.Refused refused = (FastFrame.Refused) send(confirm("another"));

        assertThat(refused.reason()).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(refused.text()).contains("another");
    }
}
