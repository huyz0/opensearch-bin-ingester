// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import java.util.Objects;
import java.util.Optional;

/**
 * Where this node watches the ingester Service's {@code EndpointSlice}s, for
 * the early lease challenge (M8.13, NFR-9).
 *
 * <p>⚠️ **OPTIONAL, AND ITS ABSENCE IS THE BEHAVIOUR BEFORE M8.13**: no watch,
 * no evidence, and failover bounded by the lease TTL alone. A node outside
 * Kubernetes, or one an operator has not given API access, still runs.
 *
 * @param apiBase the API server, e.g. {@code https://kubernetes.default.svc}
 * @param namespace the ingester Service's namespace
 * @param service the ingester Service's name; its slices carry the label
 *     {@code kubernetes.io/service-name=<service>}
 * @param tokenFile the Kubernetes service-account token, read afresh for every watch
 *     connection because a projected token rotates; empty sends none
 */
public record MembershipConfig(String apiBase, String namespace, String service,
        Optional<String> tokenFile) {

    public MembershipConfig {
        Objects.requireNonNull(apiBase, "apiBase");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(service, "service");
        Objects.requireNonNull(tokenFile, "tokenFile");
        if (apiBase.isBlank() || namespace.isBlank() || service.isBlank()) {
            throw new IllegalArgumentException("the EndpointSlice watch needs an API server, "
                    + "a namespace and a service; got '" + apiBase + "', '" + namespace
                    + "', '" + service + "'");
        }
    }
}
