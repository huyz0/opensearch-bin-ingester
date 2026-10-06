// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import io.helidon.http.Status;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sends a fast frame to a peer and reads its answer (ADR-0082 §2; M13.27h):
 * the HTTP adapter of {@link TermJoiner.Transport}.
 *
 * <p>⚠️ COUNTED BEFORE THE SEND AND NOT RETRACTED ON FAILURE, as the commit
 * transport counts: a frame whose answer was lost still spent its bytes, and
 * a counter of successes would under-report exactly when a zone misbehaves.
 *
 * <p>⚠️ ONLY A 200 IS AN ANSWER. Every other status throws: the protocol's own
 * refusals travel as {@code REFUSED} frames inside a 200, so a status is the
 * route failing, and the caller's retry is the one the protocol already has.
 *
 * <p>⚠️ THE ANSWER IS BOUNDED TOO, the route's argument in reverse: a peer's
 * endless reply must not become this pod's allocation.
 */
public final class HttpFastTransport implements TermJoiner.Transport {

    /** The peer route every fast frame is posted to. */
    public static final String PATH = "/ctl/fast";

    /** ⚠️ Bounded like the clients of the commit transport, and for its reason. */
    static final int MAX_POOLED_CLIENTS = 64;

    private static final long MAX_MESSAGE_BYTES = 4L << 10;

    private final Duration timeout;
    private final CrossAzBytes crossAz;
    private final Map<String, WebClient> clients = new ConcurrentHashMap<>();
    private final long maxAnswerBytes;

    public HttpFastTransport(Duration timeout, CrossAzBytes crossAz) {
        this(timeout, crossAz, FastFrameService.MAX_FRAME_BYTES);
    }

    /**
     * ⚠️ BY THE FRAME's KIND (M13.66): control or data, two shares of NFR-5.
     * A header that does not read is sent all the same -- the peer refuses it
     * -- and counted as data, never as control under its budget.
     */
    private static CrossAzBytes.Transport shareOf(byte[] frame) {
        try {
            return io.github.huyz0.os.biningester.format.FastFrame.isControl(
                    io.github.huyz0.os.biningester.format.FastFrame.header(frame).kind())
                    ? CrossAzBytes.Transport.FAST_CONTROL : CrossAzBytes.Transport.FAST_DATA;
        } catch (IOException unreadable) {
            return CrossAzBytes.Transport.FAST_DATA;
        }
    }

    HttpFastTransport(Duration timeout, CrossAzBytes crossAz, long maxAnswerBytes) {
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.crossAz = Objects.requireNonNull(crossAz, "crossAz");
        this.maxAnswerBytes = maxAnswerBytes;
    }

    @Override
    public byte[] exchange(String endpoint, byte[] frame) throws IOException {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(frame, "frame");
        crossAz.sentTo(shareOf(frame), endpoint, frame.length);
        try (HttpClientResponse response = clientFor(endpoint).post(PATH).submit(frame)) {
            // ⚠️ COMPARED BY CODE: Helidon's Status.equals compares the reason
            // phrase too (see HttpSequencerTransport).
            if (response.status().code() != Status.OK_200.code()) {
                throw new IOException("fast frame to " + endpoint + " answered "
                        + response.status().code() + ": " + message(response));
            }
            try (var in = new BoundedStream(response.inputStream(), maxAnswerBytes)) {
                return in.readAllBytes();
            } catch (BodyTooLargeException tooLarge) {
                throw new IOException("fast frame answer from " + endpoint + " passed "
                        + maxAnswerBytes + " bytes", tooLarge);
            }
        } catch (RuntimeException unreachable) {
            throw new IOException("fast frame to " + endpoint + " was not delivered",
                    unreachable);
        }
    }

    private static String message(HttpClientResponse response) {
        try (var in = new BoundedStream(response.inputStream(), MAX_MESSAGE_BYTES)) {
            String text = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            return text.length() <= 200 ? text : text.substring(0, 200) + "...";
        } catch (IOException | RuntimeException unreadable) {
            return "<no readable body>";
        }
    }

    private WebClient clientFor(String endpoint) {
        WebClient existing = clients.get(endpoint);
        if (existing != null) {
            return existing;
        }
        if (clients.size() >= MAX_POOLED_CLIENTS) {
            clients.clear();
        }
        // ⚠️ NO KEEP-ALIVE (M10.36), for the reason the commit transport has none.
        return clients.computeIfAbsent(endpoint, uri -> WebClient.builder()
                .baseUri(URI.create(uri))
                .connectTimeout(timeout)
                .readTimeout(timeout)
                .keepAlive(false)
                .build());
    }
}
