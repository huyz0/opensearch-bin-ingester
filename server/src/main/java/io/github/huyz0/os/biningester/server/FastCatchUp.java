// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.github.huyz0.os.biningester.http.CatchUpService;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A catch-up past the committed chain (ADR-0081 §6; M13.32c): after the chain's
 * own replay, each requested fast stream's exposed entries from the leader's
 * journal, from the offset the replay reached -- so a consumer's gap above the
 * chain is repaired before the upload rather than held until it commits.
 *
 * <p>⚠️ CUT OVER WHERE THE REPLAY ENDED, per stream: the next offset after the
 * last run it sent, or the request's own start when it sent none -- the same
 * snapshot's committed next offset, never the live chain's, so no entry the
 * replay sent is sent again (the plugin refuses an overlap). An entry the
 * upload released meanwhile leaves a hole, and the read stops there.
 *
 * <p>⚠️ BEFORE THE END FRAME, which the replay's last write is: held back,
 * the journal's events written, then it. ⚠️ Each an exposing send (ADR-0081
 * §3; M13.32g): {@code exposable} is asked before each, and none follows a no.
 */
final class FastCatchUp {

    /** A stream's exposed journal events from an offset, each send lease-checked. */
    @FunctionalInterface
    interface Journal {
        List<SubscriptionEvent> exposedFrom(String session, RunKey key, long from)
                throws IOException;
    }

    private FastCatchUp() {
    }

    static void respond(CatchUpRequestFrame request, CatchUpService.FrameSink sink,
            CatchUpService.StreamingResponder chain, Journal journal,
            java.util.function.BooleanSupplier exposable) throws IOException {
        Objects.requireNonNull(journal, "journal");
        Objects.requireNonNull(exposable, "exposable");
        Map<RunKey, Long> next = new HashMap<>();
        for (CatchUpRequestFrame.Stream s : request.streams()) {
            next.merge(s.key(), s.batchStart(), Math::max);
        }
        chain.respond(request, frame -> {
            CatchUpEventFrame event = asEvent(frame);
            if (event != null) {
                SubscriptionEvent e = event.event();
                next.merge(e.key(), e.firstOffset() + e.recordCount(), Math::max);
                sink.write(frame);
                return;
            }
            CatchUpEndFrame.decode(frame); // ⚠️ nothing else is ever written
            journal:
            for (Map.Entry<RunKey, Long> s : next.entrySet()) {
                for (SubscriptionEvent e : journal.exposedFrom(
                        request.requestId().toString(), s.getKey(), s.getValue())) {
                    if (!exposable.getAsBoolean()) {
                        break journal;
                    }
                    sink.write(new CatchUpEventFrame(request.requestId(), e).encode());
                }
            }
            sink.write(frame);
        });
    }

    private static CatchUpEventFrame asEvent(byte[] frame) {
        try {
            return CatchUpEventFrame.decode(frame);
        } catch (IOException | RuntimeException notAnEvent) {
            return null;
        }
    }
}
