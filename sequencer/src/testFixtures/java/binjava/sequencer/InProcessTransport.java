// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link SequencerTransport} fake: routes by endpoint, in one process.
 *
 * <p>⚠️ A SEAM SHIPS WITH ITS FAKE IN THE SAME COMMIT (code-structure.md rule
 * 5). The behaviours M5 must prove — following a moved lease, refusing a fenced
 * holder, not duplicating a forwarded commit — are properties of the CALLER,
 * and a socket would only make them slower to assert and flakier to trust.
 *
 * <p>⚠️ IT REFUSES RATHER THAN GUESSING. An endpoint it does not know is a
 * {@link SequencerTransport.NotTheLeaseholderException}, which is what a real
 * peer returns when it does not hold the lease. Routing to "whichever sequencer
 * we have" would make every test pass regardless of whether the caller read the
 * lease at all — the fake would be answering the question under test.
 */
public final class InProcessTransport implements SequencerTransport {

    private final Map<String, Sequencer> peers = new ConcurrentHashMap<>();
    private final List<String> sentTo = new ArrayList<>();
    private final List<CommitRequest> sent = new ArrayList<>();

    /** Puts {@code sequencer} behind {@code endpoint}. */
    public InProcessTransport at(String endpoint, Sequencer sequencer) {
        peers.put(endpoint, sequencer);
        return this;
    }

    /** Removes the peer at {@code endpoint}, as a pod going away does. */
    public void gone(String endpoint) {
        peers.remove(endpoint);
    }

    /** Every endpoint a send was addressed to, in order. */
    public List<String> sentTo() {
        synchronized (sentTo) {
            return List.copyOf(sentTo);
        }
    }

    /** Every request handed to this transport, in order. */
    public List<CommitRequest> sent() {
        synchronized (sent) {
            return List.copyOf(sent);
        }
    }

    @Override
    public CommitDelta send(String endpoint, CommitRequest request) throws IOException {
        synchronized (sentTo) {
            sentTo.add(endpoint);
        }
        synchronized (sent) {
            sent.add(request);
        }
        Sequencer peer = peers.get(endpoint);
        if (peer == null) {
            throw new NotTheLeaseholderException(
                    "no sequencer at " + endpoint + " -- it does not hold the lease");
        }
        return peer.commit(request);
    }

    @Override
    public void close() {
        peers.clear();
    }
}
