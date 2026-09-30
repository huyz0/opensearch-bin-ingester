// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.FencedException;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The forwarding hop, over a real socket (M8.20, M5.6e, FR-12).
 *
 * <p>⚠️ **WHAT THIS PINS IS THE THREE OUTCOMES, NOT THE HAPPY PATH.** A commit
 * that succeeds is the easy half; what a caller is allowed to do next depends
 * entirely on telling a REFUSAL (nothing was applied — re-read the lease and
 * resend) from an AMBIGUOUS FAILURE (it may have been applied and only the
 * answer lost — resending elsewhere gives one flush two ranges of offsets).
 * Every case below is about that line.
 */
class PeerCommitTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-000000000001");

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static CommitRequest request() {
        return new CommitRequest("poda", "inc-1", 7, "bins/c/data/seg-1",
                Map.of(new RunKey(LOGS, 3), 100));
    }

    private static CommitDelta delta() {
        return new CommitDelta(11, List.of(new SegmentCommit("bins/c/data/seg-1",
                List.of(new RunCommit(new RunKey(LOGS, 3), 100, 5000)),
                new SegmentCommit.Attribution("poda", "inc-1", 7))));
    }

    private String start(Sequencer local) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(() -> local)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    private static Sequencer answering(CommitDelta delta, List<CommitRequest> seen) {
        return new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                seen.addAll(requests);
                return delta;
            }

            @Override
            public void close() {
            }
        };
    }

    private static Sequencer failing(IOException failure) {
        return new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                throw failure;
            }

            @Override
            public void close() {
            }
        };
    }

    @Test
    void aCOMMITCrossesTheSocketAndTheDELTAComesBack() throws Exception {
        List<CommitRequest> seen = new CopyOnWriteArrayList<>();
        String endpoint = start(answering(delta(), seen));

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            CommitDelta got = transport.send(endpoint, request());

            assertThat(got.sequence()).isEqualTo(11);
            assertThat(got.segments().get(0).runs().get(0).firstOffset()).isEqualTo(5000);
        }
        assertThat(seen).hasSize(1);
        assertThat(seen.get(0))
                .as("⚠️ EVERY FIELD ARRIVES, AND `incarnationId` IS THE ONE TO WATCH: "
                        + "without it `(podId, flushSeq)` cannot tell a RESTART from a "
                        + "REPLAY (ADR-0036), and the peer would apply a flush twice or "
                        + "not at all")
                .isEqualTo(request());
    }

    @Test
    void aPEERThatDoesNotHoldTheLeaseREFUSESAndTheCallerMayRESEND() throws Exception {
        String endpoint = start(null);

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send(endpoint, request()))
                    .as("⚠️ A DISTINCT TYPE, because it says NOTHING WAS APPLIED -- which "
                            + "is what makes following the lease to a new holder safe. A "
                            + "plain IOException here would stop the caller forwarding at "
                            + "all, and every write on that node stalls")
                    .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);
        }
    }

    @Test
    void aFENCEDSequencerAlsoREFUSESRatherThanFailing() throws Exception {
        String endpoint = start(failing(new FencedException("epoch 3 was fenced by epoch 4")));

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send(endpoint, request()))
                    .as("⚠️ A FENCED SEQUENCER MEANS THE SAME THING TO THE CALLER AS 'not "
                            + "the leaseholder': nothing was applied and the lease has "
                            + "moved. Mapping it to 500 would turn a clean refusal into an "
                            + "ambiguous outcome and stall the node")
                    .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);
        }
    }

    @Test
    void aPEERWhoseOWNPeerREFUSEDPassesTheREFUSALOnRatherThanA500() throws Exception {
        // ⚠️ THE TWO-HOP CHAIN ADR-0012 PERMITS: this node forwarded too, and
        // ITS peer refused. Review MEASURED that arm executed by nothing --
        // replacing it with a 500 left all twelve cases green -- and with the
        // mutation live a chained CLEAN refusal reaches the first hop as an
        // ambiguous 500, stalling every write on that node behind a commit
        // that was never applied.
        String endpoint = start(failing(
                new SequencerTransport.NotTheLeaseholderException("my peer moved on")));

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send(endpoint, request()))
                    .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);
        }
    }

    @Test
    void aPEERThatHANGSFailsWITHINTheCONFIGUREDTimeout() throws Exception {
        // ⚠️ THE TIMEOUT WAS PINNED BY NOTHING, measured: deleting both
        // `connectTimeout` and `readTimeout` left every case green, because the
        // dead-address case is connection-REFUSED and never touches a clock. A
        // peer that ACCEPTS and then hangs is what the configured window is
        // for, and without it the append path blocks for Helidon's default.
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> {
                            try {
                                release.await();
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                            }
                            res.send(new byte[0]);
                        }))
                .build().start();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofMillis(300))) {
            // ⚠️ THE BOUND IS THE ASSERTION AND IT IS ENFORCED BY A TIMEOUT,
            // not measured after the fact: review found the elapsed-time check
            // unreachable, because without a configured read timeout the call
            // hangs and the case never returns -- which `check-tdd` cannot
            // record as a red at all (it looks like a crash, not a failure).
            // `assertTimeoutPreemptively` fails the case instead.
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(10),
                    () -> assertThatThrownBy(() ->
                            transport.send("http://localhost:" + server.port(), request()))
                            .isInstanceOf(IOException.class)
                            .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                            .hasMessageContaining("UNKNOWN"),
                    "the CONFIGURED 300 ms must reach the client; without it this call "
                            + "waits out Helidon's default and the append path blocks with it");
        } finally {
            release.countDown();
        }
    }

    /**
     * ⚠️ THE STATUS ITSELF (M13.19 review T1): a frame just past the cap -- small
     * enough to be read to the refusal -- is answered 413, and nothing is
     * committed. The 256 MiB case below sees its connection closed instead, so
     * without this one a wrong status there passed.
     */
    @Test
    void aFRAMEJustPastTheCapIsAnswered413() throws Exception {
        List<CommitRequest> seen = new CopyOnWriteArrayList<>();
        String endpoint = start(answering(delta(), seen));
        byte[] body = new byte[(int) CommitService.MAX_FRAME_BYTES + (100 << 10)];
        body[0] = 0x42;
        body[1] = 0x50;
        body[2] = 0x43;
        body[3] = 0x52;

        var client = io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();
        try (var response = client.post(HttpSequencerTransport.PATH).submit(body)) {
            assertThat(response.status().code()).isEqualTo(413);
        }
        assertThat(seen).as("and nothing was committed").isEmpty();
    }

    @Test
    void aFRAMEBiggerThanTheCapIsREFUSEDWHILEItIsREAD() throws Exception {
        // ⚠️ THE ALLOCATION IS THE ATTACK. Reading the whole entity first makes
        // M8.33's stream-count bound -- bought so a torn frame cannot become an
        // allocation -- apply only AFTER the allocation it exists to prevent,
        // and a `Content-Length: 8000000000` POST OOMs the leaseholder every
        // other node forwards to (security.md rule 5).
        List<CommitRequest> seen = new CopyOnWriteArrayList<>();
        String endpoint = start(answering(delta(), seen));

        // ⚠️ 256 MiB, GENERATED LAZILY AND NEVER HELD. Review MEASURED that a
        // cap CHECKED AFTER BUFFERING passes a 1 MiB body happily -- which is
        // round 1's defect verbatim -- so the body has to be one no
        // implementation can buffer inside this JVM's heap. A reader that
        // stops at the cap answers 413 in milliseconds; one that buffers dies.
        long huge = 256L << 20;
        var client = io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();
        long started = System.nanoTime();
        Integer status = null;
        RuntimeException refused = null;
        try (var response = client.post(HttpSequencerTransport.PATH)
                .outputStream(out -> {
                    byte[] chunk = new byte[64 << 10];
                    chunk[0] = 0x42;
                    chunk[1] = 0x50;
                    chunk[2] = 0x43;
                    chunk[3] = 0x52;
                    try {
                        for (long written = 0; written < huge; written += chunk.length) {
                            out.write(chunk);
                        }
                        out.close();
                    } catch (IOException stopped) {
                        // ⚠️ EXPECTED AND IGNORED: a server that refuses while
                        // reading closes the connection under us, which is the
                        // behaviour under test rather than a failure.
                    }
                })) {
            status = response.status().code();
        } catch (RuntimeException refusedMidStream) {
            refused = refusedMidStream;
        }
        long elapsed = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        // ⚠️ ONE OF TWO REFUSALS, EACH NAMED (M13.19, M12 harvest R17): the
        // status, or the connection the server closed under the sender --
        // measured as an UncheckedIOException over a SocketException in 120-240
        // ms. The old catch asserted only isNotNull, so any failure passed.
        if (status != null) {
            assertThat(status).isEqualTo(413);
        } else {
            assertThat(refused).as("refused mid-stream: the connection closed, nothing else")
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasCauseInstanceOf(IOException.class);
        }
        // ⚠️ AND WHILE IT IS STILL BEING SENT: a server that kept the connection
        // and read the rest took 8.5 s here and 38 s on M9's rig for the bytes
        // the cap refuses; closing it answered in well under a second.
        assertThat(elapsed).as("refused while the body was still being sent").isLessThan(5_000);
        assertThat(seen).as("and nothing was committed").isEmpty();
    }

    @Test
    void aREFUSALWithANONCanonicalREASONIsStillAREFUSAL() throws Exception {
        // ⚠️ HELIDON's `Status.equals` COMPARES THE REASON PHRASE AS WELL AS
        // THE CODE, and RFC 9112 permits an empty one -- measured:
        // `Status.create(409, "").equals(Status.CONFLICT_409)` is FALSE. With
        // `.equals`, a peer answering 409 with any other reason has its CLEAN
        // REFUSAL read as an ambiguous failure, so the append never follows the
        // lease and every write on the forwarding node stalls. Invisible to
        // every case whose peer is this repo's own `CommitService`.
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> res.status(io.helidon.http.Status.create(409, ""))
                                .send("not me")))
                .build().start();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() ->
                    transport.send("http://localhost:" + server.port(), request()))
                    .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);
        }
    }

    @Test
    void aNONCanonicalOKIsStillSUCCESS() throws Exception {
        // ⚠️ THE OTHER HALF, AND THE WORSE ONE: a 200 with an odd reason read
        // as a failure reports a commit that WAS APPLIED as UNKNOWN, and the
        // records are committed while the writer believes they may not be.
        byte[] encoded = delta().encode();
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> res.status(io.helidon.http.Status.create(200, "Fine"))
                                .send(encoded)))
                .build().start();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThat(transport.send("http://localhost:" + server.port(), request()).sequence())
                    .isEqualTo(11);
        }
    }

    @Test
    void aPEERThatREFUSESTheBytesSaysNOTHINGWasAppliedRatherThanUNKNOWN() throws Exception {
        // ⚠️ A 413 IS A KNOWN OUTCOME: the peer refused the BYTES and never
        // decoded a frame, so nothing was applied -- and resending it unchanged
        // anywhere will be refused again. Reporting it as UNKNOWN sends an
        // operator hunting for a commit that never happened; reporting it as
        // `NotTheLeaseholderException` sends the WRITER hunting for a
        // leaseholder that will refuse it just the same.
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> res.status(io.helidon.http.Status.REQUEST_ENTITY_TOO_LARGE_413)
                                .send("the request body exceeds 1048576 bytes")))
                .build().start();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() ->
                    transport.send("http://localhost:" + server.port(), request()))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                    .hasMessageContaining("nothing was applied")
                    .hasMessageNotContaining("UNKNOWN");
        }
    }

    @Test
    void aPEERThatFAILSGivesAnAMBIGUOUSIOExceptionAndSAYSSo() throws Exception {
        String endpoint = start(failing(new IOException("the store was unreachable")));

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send(endpoint, request()))
                    .as("⚠️ NOT a NotTheLeaseholderException. The commit may have reached "
                            + "the store with only the answer lost, so a caller that "
                            + "resent it elsewhere would give one flush two ranges of "
                            + "offsets")
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                    .hasMessageContaining("UNKNOWN")
                    .as("⚠️ AND THE PEER's OWN REASON TRAVELS. Without it the only operator "
                            + "who can see why the fleet stopped committing is the one on "
                            + "the node that failed, not the one watching writes stall")
                    .hasMessageContaining("the store was unreachable");
        }
    }

    @Test
    void aPEERThatIsNOTThereGivesAnAMBIGUOUSIOExceptionRatherThanARefusal() throws Exception {
        // ⚠️ THE CONVERSION THIS CLASS EXISTS TO REFUSE. A connect failure is
        // NOT a refusal: turning it into one licences a resend elsewhere, and
        // a peer that was merely slow to accept may already have committed.
        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofMillis(200))) {
            assertThatThrownBy(() -> transport.send("http://localhost:1", request()))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                    .hasMessageContaining("UNKNOWN");
        }
    }

    @Test
    void aPEERThatANSWERSGarbageIsAnIOExceptionRatherThanAnUncheckedThrow() throws Exception {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> res.send(new byte[] {1, 2, 3})))
                .build().start();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() ->
                    transport.send("http://localhost:" + server.port(), request()))
                    .as("⚠️ AN UNCHECKED THROW HERE ESCAPES THE APPEND PATH past its own "
                            + "`throws IOException` and takes down the node's writer. ⚠️ AND "
                            + "A TORN REPLY IS THE AMBIGUOUS CASE, not a refusal: the peer "
                            + "answered 200, so the commit very probably HAPPENED and only "
                            + "the answer is unreadable")
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                    .hasMessageContaining("UNKNOWN");
        }
    }

    @Test
    void aMALFORMEDFrameIsA400AndNOTA409() throws Exception {
        // ⚠️ A FRAME THIS NODE CANNOT READ SAYS NOTHING ABOUT WHO HOLDS THE
        // LEASE. Answering 409 would send the peer hunting for a leaseholder
        // over a version skew or a bug, for ever.
        List<CommitRequest> seen = new CopyOnWriteArrayList<>();
        String endpoint = start(answering(delta(), seen));

        var client = io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();
        try (var response = client.post(HttpSequencerTransport.PATH)
                .submit(new byte[] {9, 9, 9, 9})) {
            assertThat(response.status().code()).isEqualTo(400);
        }
        assertThat(seen).as("and nothing was committed").isEmpty();
    }

    @Test
    void theTRANSPORTREUSESOneClientPerEndpoint() throws Exception {
        List<CommitRequest> seen = new CopyOnWriteArrayList<>();
        String endpoint = start(answering(delta(), seen));

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                transport.send(endpoint, new CommitRequest("poda", "inc-1", i,
                        "bins/c/data/seg-" + i, Map.of(new RunKey(LOGS, 3), 1)));
            }
            assertThat(transport.pooledClients())
                    .as("⚠️ ONE CLIENT FOR FIVE COMMITS. A client per commit is a socket "
                            + "per flush per node, and asserting only that five commits "
                            + "ARRIVED is equally true of five clients -- measured, which "
                            + "is why this reaches for the pool's size. ⚠️ What is pooled "
                            + "is a CONNECTION, not a routing decision: the endpoint is "
                            + "still an argument every time, because the lease is the "
                            + "truth about who holds it (ADR-0012)")
                    .isOne();
        }
        assertThat(seen).hasSize(5);
    }

    @Test
    void TWOLivePeersGetTWOClientsAndTheSAMEOneEachTime() throws Exception {
        // ⚠️ THE CASE THAT MAKES THE POOL's SIZE MEAN SOMETHING. Review
        // MEASURED that `MAX_POOLED_CLIENTS = 1` left the whole suite green:
        // the reuse case uses ONE endpoint, so the count is 1 whether the pool
        // holds 64 peers or thrashes to one, and the bound case reads the
        // constant so the assertion moves with the mutation. Under it, a node
        // forwarding to two live peers rebuilds a client on EVERY alternating
        // commit -- a socket per flush per node, which is what the pool exists
        // to prevent.
        List<CommitRequest> seenA = new CopyOnWriteArrayList<>();
        List<CommitRequest> seenB = new CopyOnWriteArrayList<>();
        WebServer a = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(
                        new CommitService(() -> answering(delta(), seenA))))
                .build().start();
        WebServer b = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(
                        new CommitService(() -> answering(delta(), seenB))))
                .build().start();
        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            transport.send("http://localhost:" + a.port(), request());
            transport.send("http://localhost:" + b.port(), request());
            transport.send("http://localhost:" + a.port(), request());

            assertThat(transport.pooledClients())
                    .as("two peers, two clients -- and the third commit reused the first's")
                    .isEqualTo(2);
            assertThat(seenA).hasSize(2);
            assertThat(seenB).hasSize(1);
        } finally {
            a.stop();
            b.stop();
        }
    }

    @Test
    void aPEERWhoseREPLYIsENORMOUSDoesNOTTakeTheCallerDown() throws Exception {
        // ⚠️ THE ROUTE's ARGUMENT IN REVERSE, and review MEASURED it: a lazily
        // generated 2 GiB 200 response killed the CLIENT with an
        // `OutOfMemoryError` -- an Error, escaping `send` past its own
        // `throws IOException`, taking the forwarding node's writer with it.
        // The peer does not even have to be hostile; a peer running a build
        // with a different idea of a delta is enough.
        long enormous = 512L << 20;
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> {
                            res.header(io.helidon.http.HeaderNames.CONTENT_LENGTH,
                                    String.valueOf(enormous));
                            try (var out = res.outputStream()) {
                                byte[] chunk = new byte[64 << 10];
                                for (long w = 0; w < enormous; w += chunk.length) {
                                    out.write(chunk);
                                }
                            } catch (IOException stopped) {
                                // the client refused it mid-stream, as intended
                            }
                        }))
                .build().start();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(20))) {
            assertThatThrownBy(() ->
                    transport.send("http://localhost:" + server.port(), request()))
                    .as("an IOException, never an Error")
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void theCLIENTPoolIsBOUNDEDBecausePODSMOVE() throws Exception {
        // ⚠️ THE ENDPOINT COMES FROM THE LEASE, AND A RESCHEDULED POD RETURNS
        // AT A NEW ADDRESS. An unbounded map therefore grows with peer
        // INCARNATIONS for the process's life -- a cluster rolling its
        // ingesters daily leaves a node holding tens of dead pools and their
        // file descriptors. Every send below fails (nothing is listening),
        // which is the point: a client is built per endpoint whether or not
        // the peer answers.
        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofMillis(50))) {
            // ⚠️ EPHEMERAL PORTS TAKEN AND RELEASED, not a fixed range: 9000
            // is MinIO's default and this repository runs MinIO, so a fixed
            // block is a test that fails for a reason that has nothing to do
            // with it.
            for (int i = 0; i < HttpSequencerTransport.MAX_POOLED_CLIENTS + 5; i++) {
                int port;
                try (java.net.ServerSocket free = new java.net.ServerSocket(0)) {
                    port = free.getLocalPort();
                }
                try {
                    transport.send("http://127.0.0.1:" + port, request());
                } catch (IOException expected) {
                    // nothing is listening; the client was still built
                }
            }
            assertThat(transport.pooledClients())
                    .as("bounded, not unbounded")
                    .isLessThanOrEqualTo(HttpSequencerTransport.MAX_POOLED_CLIENTS);
        }
    }

    @Test
    void aCLOSEDTransportREFUSESWithAnIOExceptionRatherThanUnchecked() throws Exception {
        // ⚠️ AGAINST A LIVE PEER, and review measured why: sending to a dead
        // address after close() fails for the OTHER reason, so the closed check
        // could be deleted with this case green. A closed transport must refuse
        // a peer that would otherwise have answered.
        List<CommitRequest> seen = new CopyOnWriteArrayList<>();
        String endpoint = start(answering(delta(), seen));

        HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5));
        transport.close();

        assertThatThrownBy(() -> transport.send(endpoint, request()))
                .as("a caller racing a shutdown must treat the outcome as unknown like any "
                        + "other failure, and an unchecked throw would escape the append path")
                .isInstanceOf(IOException.class);
        assertThat(seen).as("and nothing reached the peer").isEmpty();
    }

    @Test
    void aNONPOSITIVETimeoutIsREFUSED() {
        assertThatThrownBy(() -> new HttpSequencerTransport(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HttpSequencerTransport(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theCONVERSIONBothWaysPreservesEveryField() {
        CommitRequest original = request();
        CommitRequest round = HttpSequencerTransport.requestOf(
                HttpSequencerTransport.frameOf(original));
        assertThat(round)
                .as("⚠️ `CommitRequest` AND `CommitRequestFrame` ARE TWO RECORDS WITH THE "
                        + "SAME FIELDS, which `CommitWireParityTest` keeps in step (M8.34); "
                        + "this is the other half, that the conversion drops none of them")
                .isEqualTo(original);
    }

    @Test
    void aLATESequencerIsTheONEUsedRatherThanTheOneAtConstruction() throws Exception {
        // ⚠️ LEADERSHIP MOVES. A captured reference commits through a sequencer
        // whose lease is gone -- which epoch fencing catches one layer too
        // late, after the node has already answered 200.
        AtomicReference<Sequencer> current = new AtomicReference<>(null);
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(current::get)))
                .build().start();
        String endpoint = "http://localhost:" + server.port();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send(endpoint, request()))
                    .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);

            List<CommitRequest> seen = new CopyOnWriteArrayList<>();
            current.set(answering(delta(), seen));
            assertThat(transport.send(endpoint, request()).sequence()).isEqualTo(11);
            assertThat(seen).hasSize(1);

            // ⚠️ AND BACK AGAIN, which is the direction that catches a
            // reference captured at construction: leadership is LOST as often
            // as it is gained, and a node that keeps committing through a
            // sequencer whose lease is gone is caught by epoch fencing one
            // layer too late -- after it has already answered 200.
            current.set(null);
            assertThatThrownBy(() -> transport.send(endpoint, request()))
                    .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);
            assertThat(seen).hasSize(1);
        }
    }
}
