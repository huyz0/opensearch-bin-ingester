// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Who a peer certificate names (ADR-0084 decision 8; M13.52f): its one URI
 * SAN {@code spiffe://<trust.domain>/<path>/<pod-name>/<pod-uid>}.
 *
 * @param prefix everything before the last two segments -- the FLEET: a peer
 *     binds only when its prefix equals the receiving pod's own
 * @param podName the Kubernetes pod name, which may hold dashes
 * @param podUid the Kubernetes pod UID, this pod's {@code pod.uid}
 */
public record PeerIdentity(String prefix, String podName, String podUid) {

    private static final String SCHEME = "spiffe://";
    private static final int URI_SAN = 6;

    public PeerIdentity {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(podName, "podName");
        Objects.requireNonNull(podUid, "podUid");
        // ⚠️ NO VALUE {@link #of} WOULD REFUSE (M13.52f review, P3): a route
        // reading a malformed one would answer 500, not 403.
        String rest = prefix.startsWith(SCHEME) ? prefix.substring(SCHEME.length()) : "";
        if (rest.indexOf('/') <= 0 || java.util.Arrays.stream(rest.split("/", -1))
                        .anyMatch(String::isEmpty)
                || podName.isEmpty() || podUid.isEmpty()
                || podName.contains("/") || podUid.contains("/")) {
            throw new IllegalArgumentException("not a peer identity: " + prefix + "/" + podName
                    + "/" + podUid);
        }
    }

    /**
     * The identity {@code certificate} names, or an {@link IllegalArgumentException}
     * saying why it names none: no {@code spiffe://} URI SAN, several, one
     * whose path is not {@code .../<pod-name>/<pod-uid>}, or one whose prefix
     * is only the trust domain.
     *
     * <p>⚠️ **A PREFIX THAT IS ONLY THE TRUST DOMAIN IS REFUSED** (M13.52e
     * review, P10), the narrowest cluster-wide template. It does not make a
     * prefix unique to the fleet: {@code spiffe://td/pod/<name>/<uid>} still
     * passes, and a same-named pod in another namespace would share it. A
     * path naming the fleet -- its namespace and StatefulSet -- is the
     * issuer template's, which M13.38's deployment docs require.
     */
    public static PeerIdentity of(X509Certificate certificate) {
        Objects.requireNonNull(certificate, "certificate");
        List<String> uris = new ArrayList<>();
        Collection<List<?>> names;
        try {
            names = certificate.getSubjectAlternativeNames();
        } catch (CertificateParsingException unreadable) {
            throw new IllegalArgumentException("its subject alternative names are unreadable",
                    unreadable);
        }
        if (names != null) {
            for (List<?> name : names) {
                if (name.size() == 2 && name.get(0) instanceof Integer type && type == URI_SAN
                        && name.get(1) instanceof String uri) {
                    uris.add(uri);
                }
            }
        }
        return ofUris(uris);
    }

    /** The same, from a certificate's URI SANs, every scheme among them. */
    static PeerIdentity ofUris(List<String> all) {
        List<String> uris = all.stream().filter(uri -> uri.startsWith(SCHEME)).toList();
        if (uris.size() != 1) {
            throw new IllegalArgumentException("it names " + uris.size()
                    + " spiffe:// URI SANs " + uris + ", not exactly one");
        }
        return parse(uris.get(0));
    }

    static PeerIdentity parse(String uri) {
        String rest = uri.substring(SCHEME.length());
        String[] segments = rest.split("/", -1);
        for (String segment : segments) {
            if (segment.isEmpty()) {
                throw new IllegalArgumentException("its SAN " + uri + " has an empty segment");
            }
        }
        // the trust domain, at least one path segment, the name and the UID
        if (segments.length < 4) {
            throw new IllegalArgumentException("its SAN " + uri + " is not spiffe://<trust "
                    + "domain>/<path>/<pod-name>/<pod-uid>: a prefix that is only the trust "
                    + "domain names no fleet");
        }
        int name = segments.length - 2;
        String prefix = SCHEME + String.join("/", java.util.Arrays.copyOf(segments, name));
        return new PeerIdentity(prefix, segments[name], segments[name + 1]);
    }

    /** The trust domain, the prefix's first segment. */
    public String trustDomain() {
        String rest = prefix.substring(SCHEME.length());
        return rest.substring(0, rest.indexOf('/'));
    }

    /** The {@code pod.id} this names: its pod name with the dashes removed (M8.47). */
    public String podId() {
        return podName.replace("-", "");
    }
}
