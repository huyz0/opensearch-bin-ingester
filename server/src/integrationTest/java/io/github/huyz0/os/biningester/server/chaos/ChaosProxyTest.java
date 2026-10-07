// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A cut stalls a stream and a heal resumes it WHOLE, as TCP over a real
 * partition does (M8.23).
 *
 * <p>⚠️ **REVIEW FOUND THE FIRST DRAFT DISCARDING BYTES WHILE CUT**, so a heal
 * mid-transfer delivered a stream with a hole in it: a torn HTTP message that
 * a partition-and-heal row would have reported as the ingester's bug.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ChaosProxyTest {

    /** Echoes every byte back until the peer closes. */
    private static ServerSocket echo() throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread.ofVirtual().start(() -> {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Thread.ofVirtual().start(() -> {
                        try (socket; InputStream in = socket.getInputStream();
                                OutputStream out = socket.getOutputStream()) {
                            in.transferTo(out);
                        } catch (Exception gone) {
                            // peer closed
                        }
                    });
                } catch (Exception closed) {
                    return;
                }
            }
        });
        return server;
    }

    private static String read(InputStream in, int n) throws Exception {
        byte[] bytes = in.readNBytes(n);
        return new String(bytes, java.nio.charset.StandardCharsets.US_ASCII);
    }

    @Test
    void aHEALResumesTheStreamWHOLEWithNothingLostOrReordered() throws Exception {
        try (ServerSocket server = echo();
                ChaosProxy proxy = new ChaosProxy("localhost", server.getLocalPort());
                Socket client = new Socket("localhost", proxy.port())) {
            OutputStream out = client.getOutputStream();
            InputStream in = client.getInputStream();
            out.write("before|".getBytes());
            assertThat(read(in, 7)).isEqualTo("before|");

            proxy.cut();
            out.write("during|".getBytes());
            client.setSoTimeout(300);
            boolean heardAnything;
            try {
                heardAnything = in.read() >= 0;
            } catch (SocketTimeoutException silence) {
                heardAnything = false;
            }
            assertThat(heardAnything).as("⚠️ NOTHING CROSSES WHILE CUT").isFalse();

            proxy.heal();
            client.setSoTimeout(10_000);
            out.write("after".getBytes());
            assertThat(read(in, 12)).as("⚠️ THE BYTES SENT DURING THE CUT ARRIVE, IN ORDER")
                    .isEqualTo("during|after");
        }
    }

    @Test
    void aConnectionMADEWhileCutIsJOINEDAtTheHeal() throws Exception {
        try (ServerSocket server = echo();
                ChaosProxy proxy = new ChaosProxy("localhost", server.getLocalPort())) {
            proxy.cut();
            try (Socket client = new Socket("localhost", proxy.port())) {
                client.getOutputStream().write("held".getBytes());
                client.setSoTimeout(300);
                boolean heard;
                try {
                    heard = client.getInputStream().read() >= 0;
                } catch (SocketTimeoutException silence) {
                    heard = false;
                }
                assertThat(heard).as("accepted, and unanswered while cut").isFalse();

                proxy.heal();
                client.setSoTimeout(10_000);
                assertThat(read(client.getInputStream(), 4)).isEqualTo("held");
            }
        }
    }

    /** Records every byte the target hears, echoing it back. */
    private static ServerSocket recorder(StringBuffer heard) throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread.ofVirtual().start(() -> {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Thread.ofVirtual().start(() -> {
                        try (socket; InputStream in = socket.getInputStream();
                                OutputStream out = socket.getOutputStream()) {
                            byte[] buffer = new byte[256];
                            int n;
                            while ((n = in.read(buffer)) >= 0) {
                                heard.append(new String(buffer, 0, n,
                                        java.nio.charset.StandardCharsets.US_ASCII));
                                out.write(buffer, 0, n);
                            }
                        } catch (Exception gone) {
                            // peer closed
                        }
                    });
                } catch (Exception closed) {
                    return;
                }
            }
        });
        return server;
    }

    /** A client connects while cut, sends, and gives up before the heal. */
    private static void abandonDuringCut(ChaosProxy proxy, String sent) throws Exception {
        proxy.cut();
        int before = proxy.screened();
        try (Socket client = new Socket("localhost", proxy.port())) {
            client.getOutputStream().write(sent.getBytes());
            awaitScreened(proxy, before);
        }
        proxy.heal();
    }

    /**
     * ⚠️ WAITS FOR THE PROXY TO HAVE HELD THE CONNECTION WHILE CUT (M13.45
     * review T4): healed first, its accept could return after the heal and
     * deliver it unscreened, and the case would hold by scheduling.
     */
    private static void awaitScreened(ChaosProxy proxy, int before) {
        if (proxy.isCut()) {
            await().atMost(java.time.Duration.ofSeconds(10))
                    .until(() -> proxy.screened() > before);
        }
    }

    @Test
    void byDEFAULTAnAbandonedConnectionIsStillDeliveredAtTheHeal() throws Exception {
        // ⚠️ THE LATE FORWARD (M13.45 review): AzPartitionIT needs a request
        // whose client timed out during the cut to reach its server anyway.
        StringBuffer heard = new StringBuffer();
        try (ServerSocket server = recorder(heard);
                ChaosProxy proxy = new ChaosProxy("localhost", server.getLocalPort())) {
            proxy.cut();
            try (Socket client = new Socket("localhost", proxy.port())) {
                client.getOutputStream().write("late".getBytes());
                // the default screens nothing, so wait out the accept instead
                client.setSoTimeout(300);
                try {
                    client.getInputStream().read();
                } catch (SocketTimeoutException silence) {
                    // held, as it should be
                }
            }
            proxy.heal();
            await().atMost(java.time.Duration.ofSeconds(10))
                    .until(() -> heard.toString().contains("late"));
        }
    }

    @Test
    void whenASKEDAnAbandonedConnectionIsDroppedAtTheHeal() throws Exception {
        StringBuffer heard = new StringBuffer();
        try (ServerSocket server = recorder(heard);
                ChaosProxy proxy = new ChaosProxy("localhost", server.getLocalPort())) {
            proxy.dropAbandonedAtHeal();
            abandonDuringCut(proxy, "stale");
            try (Socket client = new Socket("localhost", proxy.port())) {
                client.getOutputStream().write("fresh".getBytes());
                client.setSoTimeout(10_000);
                assertThat(read(client.getInputStream(), 5)).isEqualTo("fresh");
            }
            await().during(java.time.Duration.ofMillis(500))
                    .atMost(java.time.Duration.ofSeconds(2))
                    .until(() -> !heard.toString().contains("stale"));
            assertThat(heard.toString()).as("⚠️ NOTHING THE CLIENT ABANDONED REACHES THE TARGET")
                    .isEqualTo("fresh");
        }
    }

    @Test
    void whenASKEDAWaitingClientsHeldBytesStillArriveInOrder() throws Exception {
        try (ServerSocket server = echo();
                ChaosProxy proxy = new ChaosProxy("localhost", server.getLocalPort())) {
            proxy.dropAbandonedAtHeal();
            proxy.cut();
            try (Socket client = new Socket("localhost", proxy.port())) {
                client.getOutputStream().write("held|".getBytes());
                awaitScreened(proxy, 0);
                proxy.heal();
                client.setSoTimeout(10_000);
                assertThat(read(client.getInputStream(), 5))
                        .as("⚠️ WHAT A STILL-WAITING CLIENT SENT DURING THE CUT IS DELIVERED")
                        .isEqualTo("held|");
                // ⚠️ SILENT LONGER THAN THE PROXY'S 50 MS SCREENING READ: a
                // connection still bound by it would end here. A timed read, not
                // a sleep: the echo has nothing more to send.
                client.setSoTimeout(300);
                try {
                    client.getInputStream().read();
                } catch (SocketTimeoutException silence) {
                    // nothing, as it should be
                }
                client.getOutputStream().write("more".getBytes());
                client.setSoTimeout(10_000);
                assertThat(read(client.getInputStream(), 4))
                        .as("and the connection lives on after the screen")
                        .isEqualTo("more");
            }
        }
    }

    @Test
    void whenASKEDAConnectionMADEOutsideACutIsNeverScreened() throws Exception {
        // ⚠️ M13.45 review T5: only a connection accepted while cut is screened.
        try (ServerSocket server = echo();
                ChaosProxy proxy = new ChaosProxy("localhost", server.getLocalPort())) {
            proxy.dropAbandonedAtHeal();
            try (Socket client = new Socket("localhost", proxy.port())) {
                client.getOutputStream().write("open".getBytes());
                client.setSoTimeout(10_000);
                assertThat(read(client.getInputStream(), 4)).isEqualTo("open");
            }
            assertThat(proxy.screened()).as("nothing accepted while healed is screened").isZero();
        }
    }
}
