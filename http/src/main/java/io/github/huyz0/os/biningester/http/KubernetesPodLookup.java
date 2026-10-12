// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.PodGone;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The pod-UID GONE seam over the Kubernetes API (ADR-0081 §5 step 4;
 * M13.33c): one pod GET by the incarnation's name.
 *
 * <p>⚠️ GONE IS DELETED, AND ONLY THAT: a 404 -- no pod by that name -- or a
 * pod by that name with another UID, which a StatefulSet's replacement is.
 * Every other answer, and a pod with no UID, is an {@code IOException}: the
 * caller waits rather than counting a live copy lost.
 */
public final class KubernetesPodLookup implements PodGone {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    /** A DNS-1123 subdomain: what Kubernetes allows a pod's name to be. */
    private static final java.util.regex.Pattern POD_NAME = java.util.regex.Pattern.compile(
            "[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
    /** A DNS-1123 label: what Kubernetes allows a namespace to be. */
    private static final java.util.regex.Pattern NAMESPACE =
            java.util.regex.Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?");

    private final WebClient client;
    private final String namespace;
    private final Supplier<Optional<String>> token;

    /**
     * @param apiBase the Kubernetes API server, e.g. {@code https://kubernetes.default.svc}
     * @param token read afresh for every lookup, as the EndpointSlice watch reads it
     * @param trust the cluster CA; empty keeps the JVM's default trust store
     */
    public KubernetesPodLookup(String apiBase, String namespace,
            Supplier<Optional<String>> token,
            java.util.List<java.security.cert.X509Certificate> trust) {
        Objects.requireNonNull(apiBase, "apiBase");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        if (namespace.length() > 63 || !NAMESPACE.matcher(namespace).matches()) {
            throw new IllegalArgumentException("a namespace is a DNS-1123 label: " + namespace);
        }
        this.token = Objects.requireNonNull(token, "token");
        Objects.requireNonNull(trust, "trust");
        var builder = WebClient.builder().baseUri(apiBase).connectTimeout(TIMEOUT)
                .readTimeout(TIMEOUT).keepAlive(false);
        if (!trust.isEmpty()) {
            builder.tls(io.helidon.common.tls.Tls.builder().trust(trust).build());
        }
        this.client = builder.build();
    }

    @Override
    public boolean gone(Roster.Incarnation incarnation) throws IOException {
        Objects.requireNonNull(incarnation, "incarnation");
        String name = incarnation.podId();
        // ⚠️ A NAME KUBERNETES COULD NOT GIVE A POD IS NO ANSWER (M13.33c
        // review round 1, R1): in the path unchecked, a name like
        // `../../pods/other` would read another object, whose UID differs --
        // a live member declared GONE, its copies voided.
        if (name.length() > 253 || !POD_NAME.matcher(name).matches()) {
            throw new IOException("no pod can be named " + name);
        }
        var request = client.get("/api/v1/namespaces/" + namespace + "/pods/"
                + incarnation.podId());
        Optional<String> bearer = token.get();
        if (bearer.isPresent()) {
            request = request.header(io.helidon.http.HeaderNames.AUTHORIZATION,
                    "Bearer " + bearer.get());
        }
        try (HttpClientResponse response = request.request()) {
            int status = response.status().code();
            if (status == 404) {
                return true;
            }
            if (status != 200) {
                throw new IOException("the pod lookup for " + incarnation.podId()
                        + " answered " + status);
            }
            Object parsed;
            try {
                parsed = Json.parse(response.as(String.class));
            } catch (RuntimeException unreadable) {
                throw new IOException("the pod lookup's answer does not parse", unreadable);
            }
            if (!(parsed instanceof Map<?, ?> pod)
                    || !(pod.get("metadata") instanceof Map<?, ?> metadata)
                    || !(metadata.get("uid") instanceof String uid) || uid.isBlank()) {
                throw new IOException("the pod lookup for " + incarnation.podId()
                        + " carries no UID");
            }
            // ⚠️ AND THE POD ASKED FOR (its review round 1, R1): another
            // object's UID says nothing about this incarnation.
            if (!name.equals(metadata.get("name"))) {
                throw new IOException("the pod lookup for " + name + " answered pod "
                        + metadata.get("name"));
            }
            return !uid.equals(incarnation.podUid());
        } catch (RuntimeException unreachable) {
            throw new IOException("the pod lookup for " + incarnation.podId() + " failed",
                    unreachable);
        }
    }
}
