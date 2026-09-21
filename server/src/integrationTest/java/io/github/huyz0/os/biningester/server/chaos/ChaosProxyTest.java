// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;

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
}
