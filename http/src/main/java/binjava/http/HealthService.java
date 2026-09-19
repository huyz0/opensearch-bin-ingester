// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.util.Objects;

/**
 * The two probes Kubernetes asks (M8.7, research 08 §7 step 1).
 *
 * <p>⚠️ **READINESS FAILS AND LIVENESS KEEPS PASSING.** They answer different
 * questions. Readiness is "send me work", and a draining node must say no to
 * it. Liveness is "am I still running", and a draining node must say yes, or
 * the kubelet kills it mid-flush. That kill is a {@code SIGKILL}, and it
 * skips the lease release this whole sequence exists to reach.
 */
public final class HealthService implements HttpService {

    public static final String READY_PATH = "/ready";
    public static final String LIVE_PATH = "/live";

    private final DrainGate gate;

    public HealthService(DrainGate gate) {
        this.gate = Objects.requireNonNull(gate, "gate");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get(READY_PATH, this::ready).get(LIVE_PATH, this::live);
    }

    private void ready(ServerRequest request, ServerResponse response) {
        boolean ready = gate.ready();
        response.status(ready ? Status.OK_200 : Status.SERVICE_UNAVAILABLE_503)
                .send(ready ? "ready" : "draining");
    }

    private void live(ServerRequest request, ServerResponse response) {
        response.status(Status.OK_200).send("live");
    }
}
