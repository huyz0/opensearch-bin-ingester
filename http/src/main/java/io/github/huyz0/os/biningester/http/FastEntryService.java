// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.util.Objects;
import java.util.Optional;

/**
 * An exposed fast entry, read by its fast key from the journal of the leader
 * that assigned it, before its upload (ADR-0081 §6, ADR-0082 §2 and §6;
 * M13.30o): the peer route a pod forwards a {@code /seg} read to when it holds
 * no pushed copy (M13.30p).
 *
 * <p>⚠️ NOT A FRAME BUT AN HTTP BODY, as ADR-0082 §2 says: the entry as a
 * one-run segment, with the epoch it was served under in a
 * {@value #EPOCH_HEADER} header the forwarding pod checks against its own
 * fence. {@code 404} for a key this pod's leading term does not hold EXPOSED
 * -- never an unexposed entry, never another term's.
 *
 * <p>⚠️ ON THE PEER LISTENER, its caller named by {@value #POD_PARAM} and bound
 * to its client certificate (ADR-0084), as a frame's sender is.
 */
public final class FastEntryService implements HttpService {

    public static final String PATH = "/ctl/fast-entry";
    public static final String KEY_PARAM = "key";
    public static final String POD_PARAM = "pod";
    public static final String EPOCH_HEADER = "Binstore-Fast-Epoch";

    /** An entry served: its bytes and the epoch they were served under. */
    public record Served(long epoch, byte[] bytes) {
        public Served {
            Objects.requireNonNull(bytes, "bytes");
        }
    }

    /** The leading term's exposed entry by fast key, or empty. */
    @FunctionalInterface
    public interface Lookup {
        Optional<Served> exposed(String fastKey) throws java.io.IOException;
    }

    private final Lookup lookup;
    private final PeerBinding binding;

    public FastEntryService(Lookup lookup, PeerBinding binding) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.binding = Objects.requireNonNull(binding, "binding");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get(PATH, this::serve);
    }

    private void serve(ServerRequest request, ServerResponse response) {
        Optional<String> key = Optional.ofNullable(request.query().first(KEY_PARAM).orElse(null));
        Optional<String> pod = Optional.ofNullable(request.query().first(POD_PARAM).orElse(null));
        if (key.isEmpty() || pod.isEmpty()) {
            response.status(Status.BAD_REQUEST_400).send("a key and the asking pod");
            return;
        }
        Optional<String> refused = binding.refusalForUid(
                request.remotePeer().tlsCertificates(), pod.get());
        if (refused.isPresent()) {
            response.status(Status.FORBIDDEN_403).send(refused.get());
            return;
        }
        Optional<Served> served;
        try {
            served = lookup.exposed(key.get());
        } catch (java.io.IOException | RuntimeException failed) {
            response.status(Status.INTERNAL_SERVER_ERROR_500)
                    .send(String.valueOf(failed.getMessage()));
            return;
        }
        if (served.isEmpty()) {
            response.status(Status.NOT_FOUND_404).send("no exposed entry of that key here");
            return;
        }
        response.header(HeaderNames.create(EPOCH_HEADER), Long.toString(served.get().epoch()))
                .header(HeaderNames.CONTENT_TYPE, "application/octet-stream")
                .send(served.get().bytes());
    }
}
