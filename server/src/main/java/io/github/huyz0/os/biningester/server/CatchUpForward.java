// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.http.CatchUpService;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import io.github.huyz0.os.biningester.sequencer.LeaderWatch;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

/**
 * A catch-up this pod cannot serve, forwarded to the chain's leader (ADR-0081
 * §3, §6; M13.32i): a plugin asks its own node's ingester, usually a follower,
 * which serves no chain and answered every catch-up {@code 503} -- the default
 * path's included -- so a consumer's gap was held for good.
 *
 * <p>⚠️ THE LEADER: the newest joined term's, read from memory, else the
 * lease's holder, one GET per forward; never this pod itself -- with no other
 * leader it is not forwarded and this pod answers as before. ⚠️ ITS ANSWER's FAST
 * EPOCH at or above this pod's fence, or nothing is relayed (ADR-0082 §2).
 */
final class CatchUpForward {

    /** How long a forwarded catch-up may wait on any one read. */
    static final Duration READ_TIMEOUT = Duration.ofSeconds(30);

    private CatchUpForward() {
    }

    static Forward of(ServerConfig config, Assembly assembly,
            FastDisk disk, CrossAzBytes crossAz, Optional<io.helidon.common.tls.Tls> clientTls) {
        HttpFastTransport transport = new HttpFastTransport(READ_TIMEOUT, crossAz, clientTls);
        return of(config.podUid(), disk, FastPeer.leaseReader(config, assembly.store()),
                transport::forwardCatchUp);
    }

    /** A catch-up forwarded, or {@code false} when there is no other leader to ask. */
    @FunctionalInterface
    interface Forward {
        boolean forwarded(io.github.huyz0.os.biningester.format.CatchUpRequestFrame request,
                CatchUpService.FrameSink sink) throws IOException;
    }

    /** The forward itself, over {@code send}. */
    @FunctionalInterface
    interface Send {
        void send(String endpoint, io.github.huyz0.os.biningester.format.CatchUpRequestFrame request,
                CatchUpService.FrameSink sink, java.util.function.LongPredicate admitted)
                throws IOException;
    }

    static Forward of(String selfUid, FastDisk disk,
            LeaderWatch.LeaseReader lease, Send send) {
        return (request, sink) -> {
            Optional<JoinedTerms.Leader> joined = disk.joined().latest();
            String uid;
            String endpoint;
            if (joined.isPresent()) {
                uid = joined.get().uid();
                endpoint = joined.get().endpoint();
            } else {
                Lease current = lease.read();
                uid = current.holderPodUid();
                endpoint = current.holderEndpoint();
            }
            if (uid.isBlank() || endpoint.isBlank() || uid.equals(selfUid)) {
                return false;
            }
            send.send(endpoint, request, sink, epoch -> epoch >= disk.fence().highest());
            return true;
        };
    }
}
