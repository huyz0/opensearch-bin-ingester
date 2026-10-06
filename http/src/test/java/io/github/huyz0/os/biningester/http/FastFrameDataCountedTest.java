// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.EpochFence;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A fast frame sent is counted under its kind's share (M13.66): a COMMIT as
 * data, inside NFR-5's ratio; a JOIN as control, under its own budget.
 */
class FastFrameDataCountedTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "");

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private String serve() throws Exception {
        FastFrameRouter router = new FastFrameRouter("uid-l",
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()));
        for (int kind : new int[] {FastFrame.KIND_JOIN, FastWriteFrame.KIND_COMMIT}) {
            router.handle(kind, (header, body) -> new FastFrame.Refused(
                    FastFrame.Reason.NOT_FAST, Optional.empty(), "no"));
        }
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new FastFrameService(router)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    @Test
    void aCOMMITIsCountedAsDataAndAJOINAsControl() throws Exception {
        String endpoint = serve();
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5), crossAz);
        byte[] commit = FastFrame.encode(3, POD.podUid(), "uid-l", new FastWriteFrame.Commit(
                new FastJournalRecord.IdempotencyKey("p", new UUID(9, 9), 1),
                List.of(new FastWriteFrame.CommitRun(new RunKey(new UUID(1, 1), 0),
                        List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                new byte[] {1}))))));
        byte[] join = FastFrame.encode(3, POD.podUid(), "uid-l",
                new FastFrame.Join(POD, FastFrame.Held.NONE));

        transport.exchange(endpoint, commit);
        transport.exchange(endpoint, join);

        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_DATA))
                .isEqualTo(commit.length);
        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL))
                .isEqualTo(join.length);
    }
}
