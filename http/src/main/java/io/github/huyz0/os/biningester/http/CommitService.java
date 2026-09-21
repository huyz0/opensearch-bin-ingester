// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.CommitRequestFrame;
import io.github.huyz0.os.biningester.sequencer.FencedException;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The receiving half of the forwarding hop (M8.20, M5.6e, FR-12).
 *
 * <p>⚠️ **THIS OWNS NO DECISION** (ADR-0019, architecture.md rule 4), the same
 * way {@link BulkService} owns none: it decodes, hands the request to the local
 * {@link Sequencer}, and maps an outcome to a status code. Whether this node
 * may commit is the lease's business and the sequencer's, never this class's.
 *
 * <p>⚠️ **A NODE THAT IS NOT THE LEASEHOLDER ANSWERS 409, NOT 500.** The
 * difference is what a caller may do next: a 409 says plainly that nothing was
 * applied, so the caller re-reads the lease and resends to whoever holds it
 * (M5.5). Any other failure leaves the outcome UNKNOWN, and a caller that
 * resent it elsewhere would give one flush two ranges of offsets.
 *
 * <p>⚠️ **THE SEQUENCER IS SUPPLIED LAZILY**, because leadership moves. A node
 * that held the term when this service was constructed may not hold it when a
 * request arrives, and a captured reference would commit through a sequencer
 * whose lease is gone — which is precisely what epoch fencing exists to catch,
 * one layer too late.
 */
public final class CommitService implements HttpService {

    private final Supplier<Sequencer> sequencer;

    /**
     * @param sequencer the node's current sequencer, or {@code null} when this
     *     node does not lead. ⚠️ **CALLED PER REQUEST**, not once.
     */
    public CommitService(Supplier<Sequencer> sequencer) {
        this(sequencer, (term, requester) -> {
            throw new IOException("this node was built without an inbox drain");
        });
    }

    /** Drains the inbox through the held term; see {@link #drain}. */
    @FunctionalInterface
    public interface Drainer {
        int drain(Sequencer term, String requester) throws IOException;
    }

    private final Drainer drainer;

    /** ⚠️ With the drain a deferring pod asks for when it can reach us again (M8.14a). */
    public CommitService(Supplier<Sequencer> sequencer, Drainer drainer) {
        this.sequencer = Objects.requireNonNull(sequencer, "sequencer");
        this.drainer = Objects.requireNonNull(drainer, "drainer");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post(HttpSequencerTransport.PATH, this::commit);
        rules.post(HttpSequencerTransport.DRAIN_PATH, this::drain);
    }

    /**
     * ⚠️ **200 ONLY WHEN EVERY INTENT WAS APPLIED**, so the pod that asked stops
     * deferring only once its own are in the chain; 409 where this node holds
     * no term, as for a commit.
     */
    private void drain(ServerRequest request, ServerResponse response) {
        Sequencer local = sequencer.get();
        if (local == null) {
            response.status(HttpSequencerTransport.NOT_THE_LEASEHOLDER)
                    .send("this node does not hold the lease");
            return;
        }
        try {
            String requester = request.query().first("pod").orElse(null);
            response.status(Status.OK_200)
                    .send(String.valueOf(drainer.drain(local, requester)));
        } catch (FencedException fenced) {
            response.status(HttpSequencerTransport.NOT_THE_LEASEHOLDER).send(fenced.getMessage());
        } catch (IOException failed) {
            response.status(Status.INTERNAL_SERVER_ERROR_500)
                    .send(String.valueOf(failed.getMessage()));
        }
    }

    /**
     * ⚠️ **A CAP ON THE BYTES, ENFORCED WHILE READING AND NOT AFTER.**
     * `request.content().as(byte[].class)` materialises the whole entity first,
     * so M8.33's stream-count bound — bought so that a torn frame cannot become
     * an allocation — would apply only after the allocation it exists to
     * prevent. A `Content-Length: 8000000000` POST here OOMs the leaseholder
     * that every other node forwards to (security.md rule 5). {@link
     * BulkService} already reads its body this way.
     *
     * <p>⚠️ 1 MiB is far above any real commit — the frame carries identities
     * and a count per stream, so a node with a thousand streams is tens of
     * kilobytes — and far below anything that threatens a heap.
     */
    static final long MAX_FRAME_BYTES = 1L << 20;

    private static byte[] bounded(ServerRequest request) throws IOException {
        try (var in = new BoundedStream(request.content().inputStream(), MAX_FRAME_BYTES)) {
            return in.readAllBytes();
        }
    }

    private void commit(ServerRequest request, ServerResponse response) {
        CommitRequestFrame frame;
        try {
            frame = CommitRequestFrame.decode(bounded(request));
        } catch (BodyTooLargeException tooLarge) {
            // ⚠️ 413 AND NOT 400: the frame may have been perfectly well
            // formed and simply enormous, and an operator needs to see which.
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send(tooLarge.getMessage());
            return;
        } catch (IOException malformed) {
            // ⚠️ 400 AND NOT 409: a frame this node cannot read says nothing
            // about who holds the lease, and answering 409 would send the peer
            // hunting for a leaseholder over a bug or a version skew. ⚠️ AND THE
            // MESSAGE IS THE DECODER'S, which names what was wrong with the
            // bytes -- the peer's operator is the one who can act on it.
            response.status(Status.BAD_REQUEST_400).send(String.valueOf(malformed.getMessage()));
            return;
        }
        Sequencer local = sequencer.get();
        if (local == null) {
            response.status(HttpSequencerTransport.NOT_THE_LEASEHOLDER)
                    .send("this node does not hold the lease");
            return;
        }
        try {
            CommitDelta delta = local.commit(HttpSequencerTransport.requestOf(frame));
            response.status(Status.OK_200).send(delta.encode());
        } catch (FencedException fenced) {
            // ⚠️ A FENCED SEQUENCER IS A REFUSAL, AND IT IS THE SAME ANSWER AS
            // "not the leaseholder" BECAUSE IT MEANS THE SAME THING TO THE
            // CALLER: nothing was applied, and the lease has moved. Mapping it
            // to 500 would make a caller treat a clean refusal as ambiguous and
            // stop forwarding, which stalls every write on that node.
            response.status(HttpSequencerTransport.NOT_THE_LEASEHOLDER).send(fenced.getMessage());
        } catch (SequencerTransport.NotTheLeaseholderException moved) {
            // ⚠️ REACHED WHEN THIS NODE ITSELF FORWARDED and its own peer
            // refused -- a chain of two hops, which ADR-0012 permits and which
            // must not be reported as this node's own refusal any differently.
            response.status(HttpSequencerTransport.NOT_THE_LEASEHOLDER).send(moved.getMessage());
        } catch (IOException failed) {
            // ⚠️ 500, AND THE CALLER MUST READ IT AS AMBIGUOUS. The commit may
            // have reached the store and only the answer been lost, so the
            // message says so rather than inviting a retry elsewhere. ⚠️ THE
            // PEER's OWN REASON IS IN THE BODY and the client puts it in the
            // exception it raises, because otherwise the only operator who can
            // see why a fleet stopped committing is the one on the node that
            // failed rather than the one watching writes stall.
            response.status(Status.INTERNAL_SERVER_ERROR_500)
                    .send("commit failed; the outcome is UNKNOWN: " + failed.getMessage());
        }
    }
}
