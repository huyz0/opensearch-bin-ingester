// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.ingest.IngestConfig;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Turns the settings an operator wrote into a {@link ServerConfig}, or refuses
 * (M8.26, M8's criterion 21).
 *
 * <p>⚠️ **THE FAILURE PATH IS THE POINT, NOT THE HAPPY ONE.** A root that
 * defaults a missing store endpoint starts happily, writes nowhere anyone
 * expects, and passes every in-process test. So every refusal here NAMES THE
 * KEY, and the three shapes M8's criterion 21 calls out — a missing setting,
 * an unparseable duration, a negative size — each refuse rather than fall back.
 *
 * <p>⚠️ **AN UNKNOWN KEY IS REFUSED TOO**, which is the one most likely to be
 * argued about. A tolerated typo is a setting that reads as applied and is not:
 * {@code lease.tll=30s} leaves the TTL at its default, the node runs, and the
 * operator's evidence that they changed it is their own config file. Refusing
 * costs a restart; tolerating costs an incident nobody can reproduce.
 *
 * <p>⚠️ **THIS CLASS TAKES A MAP AND READS NO FILE.** Reading one would name
 * {@code java.nio.file} in the one module that is not exempt from
 * {@code check-io-seam.sh} — and the source of the map (an environment, system
 * properties, a mounted file read by the adapter) is the process's business,
 * which is M8.4's.
 */
public final class ServerProperties {

    /** Required: this node's identity in the lease. */
    public static final String POD_ID = "pod.id";
    /**
     * Required: this pod's availability zone, as a LABEL (M9.2, NFR-5).
     *
     * <p>⚠️ **REQUIRED, AND NOT DERIVED FROM ANYTHING.** Nothing else this
     * process reads carries a zone -- the lease carries an endpoint, the
     * {@code EndpointSlice} watch reads addresses -- so a pod that is not told
     * its zone cannot tell a cross-AZ byte from a same-AZ one, and NFR-5 would
     * be measured as zero on a fleet spread over three of them. In a pod it is
     * {@code topology.kubernetes.io/zone} off the node, passed in the manifest.
     */
    public static final String POD_AZ = "pod.az";
    /** Required: the cluster a producer's principal must match. */
    public static final String TRUST_DOMAIN = "trust.domain";
    /** Required: the key prefix everything this node writes lives under. */
    public static final String PREFIX = "store.prefix";
    /** Required: which backend, by name. */
    public static final String STORE_KIND = "store.kind";
    /** Required for a filesystem-backed store, refused for any other. */
    public static final String STORE_ROOT = "store.root";
    /** Optional for {@code s3}, refused for any other: absent means AWS itself. */
    public static final String STORE_ENDPOINT = "store.endpoint";
    /** Required for {@code s3}, refused for any other — SigV4 signs over it. */
    public static final String STORE_REGION = "store.region";
    /** Required for {@code s3}, refused for any other. */
    public static final String STORE_BUCKET = "store.bucket";
    /** Optional for {@code s3}: true for every non-AWS endpoint met so far. */
    public static final String STORE_PATH_STYLE = "store.path-style";
    /** Required: the port the front door listens on. */
    public static final String HTTP_PORT = "http.port";
    /** Required: who an unauthenticated producer is taken to be (M8.4). */
    public static final String PRODUCER_SUBJECT = "producer.subject";
    /** Required: the comma-separated indices that producer may write to. */
    public static final String PRODUCER_ALLOWED_INDICES = "producer.allowed-indices";
    /** Required: where peers reach this node's sequencer. */
    public static final String ENDPOINT = "endpoint";
    /** Optional: how long a lease outlives its holder's last renewal. */
    public static final String LEASE_TTL = "lease.ttl";
    /** Optional: how often the holder renews. */
    public static final String LEASE_RENEW = "lease.renew-interval";
    /** Optional: the adaptive interval's floor. */
    public static final String INTERVAL_FLOOR = "ingest.interval-floor";
    /** Optional: the segment size that forces a flush. */
    public static final String MAX_SEGMENT_BYTES = "ingest.max-segment-bytes";
    /** Optional: whether consumers may fetch with signed URLs. */
    public static final String DIRECT_ENABLED = "ingest.direct-enabled";
    /** Optional: the retention floor -- NFR-13's consumer outage budget. */
    public static final String RETENTION_MIN = "retention.min";
    /** Optional: the retention ceiling, past which data is deleted unread. */
    public static final String RETENTION_MAX = "retention.max";
    /** Optional: how long a silent shard copy is still trusted. */
    public static final String RETENTION_REPORT_TIMEOUT = "retention.report-timeout";
    /** Optional: how long a silent shard copy is remembered at all. */
    public static final String RETENTION_COPY_EXPIRY = "retention.copy-expiry";
    /** Optional: how often the retention loop looks for work. */
    public static final String RETENTION_PASS_INTERVAL = "retention.pass-interval";

    /** Optional: the Kubernetes API server the EndpointSlice watch reads (M8.13). */
    public static final String MEMBERSHIP_API = "membership.kube-api";

    /** Required with {@link #MEMBERSHIP_API}: the ingester Service's namespace. */
    public static final String MEMBERSHIP_NAMESPACE = "membership.namespace";

    /** Required with {@link #MEMBERSHIP_API}: the ingester Service's name. */
    public static final String MEMBERSHIP_SERVICE = "membership.service";

    /** Optional: the Kubernetes service-account token file the watch authenticates with. */
    public static final String MEMBERSHIP_TOKEN_FILE = "membership.token-file";

    /**
     * Optional: the cluster CA the API server's certificate is signed by, as
     * PEM -- in a pod, its service account's {@code ca.crt} (M8.51).
     */
    public static final String MEMBERSHIP_CA_FILE = "membership.ca-file";

    /**
     * ⚠️ 10 s and 3 s are MEASURED (M1, M8.27): a SIGSTOP of 2 to 9 s caused
     * no takeover, and one of 11, 12 or 20 s exactly one, 10.28-10.33 s (two runs) after
     * the stop. The absorbed pause is the TTL whatever the renew phase, because
     * a follower elects only when a forwarded commit times out, and that
     * timeout IS the TTL (M8.12). ⚠️ THESE NUMBERS ARE OF THESE VALUES: a change
     * to either default is a re-run of {@code LeaseTtlMeasurementIT}, and this
     * comment changes with it.
     */
    static final Duration DEFAULT_LEASE_TTL = Duration.ofSeconds(10);

    static final Duration DEFAULT_LEASE_RENEW = Duration.ofSeconds(3);

    private static final Set<String> KNOWN = Set.of(POD_ID, POD_AZ, TRUST_DOMAIN, PREFIX, STORE_KIND,
            STORE_ROOT, STORE_ENDPOINT, STORE_REGION, STORE_BUCKET, STORE_PATH_STYLE,
            ENDPOINT, HTTP_PORT, PRODUCER_SUBJECT, PRODUCER_ALLOWED_INDICES,
            LEASE_TTL, LEASE_RENEW, INTERVAL_FLOOR, MAX_SEGMENT_BYTES, DIRECT_ENABLED,
            RETENTION_MIN, RETENTION_MAX, RETENTION_REPORT_TIMEOUT, RETENTION_COPY_EXPIRY,
            RETENTION_PASS_INTERVAL, MEMBERSHIP_API, MEMBERSHIP_NAMESPACE, MEMBERSHIP_SERVICE,
            MEMBERSHIP_TOKEN_FILE, MEMBERSHIP_CA_FILE);

    private ServerProperties() {
    }

    /**
     * Parses {@code settings}.
     *
     * @throws ConfigurationException if anything is missing, unparseable, out
     *     of range, or unrecognised — ⚠️ **one exception type, so a caller can
     *     tell an operator's mistake from a defect in this code**, which is
     *     what M8.4 needs to exit non-zero with a message rather than a stack
     *     trace.
     */
    public static ServerConfig parse(Map<String, String> settings) {
        Objects.requireNonNull(settings, "settings");
        refuseUnknownKeys(settings);

        // ⚠️ EVERY CONSTRUCTION IS INSIDE THE WRAPPER, and review MEASURED why:
        // with `IngestConfig` built outside it, `ingest.interval-floor=PT10S` --
        // a positive, well-formed duration that passes every check in this
        // class -- escaped as a bare `IllegalArgumentException` reading
        // "intervalCeiling must be at least intervalFloor", which names no key
        // the operator wrote. M8.4 would print a stack trace for a one-line
        // manifest typo, which is the thing `ConfigurationException` exists to
        // prevent. ⚠️ CROSS-FIELD RULES LIVE IN THE RECORDS, NOT HERE: a second
        // copy of "the ceiling must not be below the floor" is a second copy
        // that goes stale.
        try {
            String trustDomain = required(settings, TRUST_DOMAIN);
            IngestConfig ingest = new IngestConfig(
                    duration(settings, INTERVAL_FLOOR, IngestConfig.DEFAULT_INTERVAL_FLOOR),
                    positiveBytes(settings, MAX_SEGMENT_BYTES,
                            IngestConfig.DEFAULT_MAX_SEGMENT_BYTES),
                    trustDomain,
                    IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES,
                    IngestConfig.DEFAULT_INTERVAL_CEILING,
                    IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                    IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD,
                    IngestConfig.DEFAULT_INTERVAL_LENGTHEN_DELAY,
                    IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY,
                    bool(settings, DIRECT_ENABLED, false));

            StoreConfig store = new StoreConfig(required(settings, STORE_KIND),
                    optionalText(settings, STORE_ROOT),
                    optionalText(settings, STORE_ENDPOINT),
                    optionalText(settings, STORE_REGION),
                    optionalText(settings, STORE_BUCKET),
                    bool(settings, STORE_PATH_STYLE, false));

            return new ServerConfig(podId(settings), required(settings, POD_AZ), trustDomain,
                    required(settings, PREFIX), store,
                    duration(settings, LEASE_TTL, DEFAULT_LEASE_TTL),
                    duration(settings, LEASE_RENEW, DEFAULT_LEASE_RENEW),
                    required(settings, ENDPOINT), ingest,
                    port(settings, HTTP_PORT),
                    required(settings, PRODUCER_SUBJECT),
                    indices(settings, PRODUCER_ALLOWED_INDICES),
                    new RetentionConfig(
                            duration(settings, RETENTION_MIN,
                                    RetentionConfig.DEFAULT_MIN_RETENTION),
                            duration(settings, RETENTION_MAX,
                                    RetentionConfig.DEFAULT_MAX_RETENTION),
                            duration(settings, RETENTION_REPORT_TIMEOUT,
                                    RetentionConfig.DEFAULT_REPORT_TIMEOUT),
                            duration(settings, RETENTION_COPY_EXPIRY,
                                    RetentionConfig.DEFAULT_COPY_EXPIRY),
                            duration(settings, RETENTION_PASS_INTERVAL,
                                    io.github.huyz0.os.biningester.ingest.RetentionLoop.DEFAULT_PASS_INTERVAL)),
                    membership(settings));
        } catch (IllegalArgumentException refused) {
            // ⚠️ `ConfigurationException` IS AN `IllegalArgumentException`, so
            // one already carrying a key's name lands here too and is returned
            // unchanged rather than re-wrapped. ⚠️ AN EARLIER VERSION HAD A
            // SEPARATE RETHROW BRANCH whose comment claimed it prevented a
            // doubled message; review measured that deleting it changed no
            // message at all, because the wrapping arm copies `getMessage()`
            // verbatim. One branch, and the `instanceof` says what it does.
            // ⚠️ AND `NullPointerException` IS DELIBERATELY NOT CAUGHT: nothing
            // an operator can write produces one here, so catching it would
            // convert a DEFECT IN THIS CODE into an operator-facing message --
            // with a null text, since that is what an unnamed NPE carries.
            // ⚠️ THE RECORDS' OWN GUARDS ARE REACHED THROUGH HERE, so that a
            // blank prefix and a missing one give an operator the same KIND of
            // message. Without this the two paths differ: one names a key, the
            // other names a field, and only one is distinguishable from a bug.
            throw refused instanceof ConfigurationException named
                    ? named
                    : new ConfigurationException(refused.getMessage(), refused);
        }
    }

    private static void refuseUnknownKeys(Map<String, String> settings) {
        Set<String> unknown = new TreeSet<>();
        for (String key : settings.keySet()) {
            if (key == null) {
                // ⚠️ A YAML `: value` LINE PARSES TO A NULL KEY, and
                // `Set.of(...).contains(null)` THROWS -- so without this the
                // operator gets a bare NullPointerException from inside the
                // parser, which is the one message shape this class exists to
                // avoid handing them.
                throw new ConfigurationException("a setting has no name (an empty key)");
            }
            if (!KNOWN.contains(key)) {
                unknown.add(key);
            }
        }
        if (!unknown.isEmpty()) {
            // ⚠️ THE MESSAGE CARRIES THE KNOWN KEYS, because the commonest
            // cause is a near-miss and a list is what turns "unknown key" into
            // "you meant this one".
            throw new ConfigurationException("unknown setting(s): " + unknown
                    + " (known: " + new TreeSet<>(KNOWN) + ")");
        }
    }

    /**
     * The pod id, refused here if the writer cannot use it (M8.47).
     *
     * <p>⚠️ **{@code -} AND {@code /} ARE THE SEGMENT KEY's SEPARATORS**, so
     * the publisher refuses them -- and it did so from inside the assembly, as
     * an uncaught exception out of {@code main}. A StatefulSet's own names
     * contain a dash, which makes this the likeliest id to be refused.
     */
    private static String podId(Map<String, String> settings) {
        String podId = required(settings, POD_ID);
        if (podId.indexOf('-') >= 0 || podId.indexOf('/') >= 0) {
            throw new ConfigurationException(POD_ID + " may not contain '-' or '/', which "
                    + "separate the fields of a segment key: " + podId);
        }
        return podId;
    }

    private static String required(Map<String, String> settings, String key) {
        String value = settings.get(key);
        if (value == null || value.isBlank()) {
            throw new ConfigurationException(key + " is required and was "
                    + (value == null ? "not set" : "blank"));
        }
        return value.trim();
    }

    /**
     * An optional setting's text, ⚠️ **with the same blank check every required
     * one gets**.
     *
     * <p>Review MEASURED the shape this replaces: {@code store.root} was the
     * one read in this class that skipped it, so
     * {@code store.root=${DATA_DIR}} with {@code DATA_DIR} unset parsed to a
     * root of three spaces — a node that starts, acks writes, and puts them
     * under a directory named "   " relative to the process's working
     * directory. That is the failure this whole class exists to refuse, in the
     * one setting that was allowed to skip the check.
     */
    private static Optional<String> optionalText(Map<String, String> settings, String key) {
        String value = settings.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (value.isBlank()) {
            throw new ConfigurationException(key + " is set but blank — remove it, or give it "
                    + "a value; a blank is almost always an unset variable");
        }
        return Optional.of(value.trim());
    }

    /**
     * The EndpointSlice watch's settings, or empty when no API server is named.
     *
     * <p>⚠️ **A NAMESPACE OR SERVICE WITHOUT AN API SERVER IS REFUSED**, not
     * ignored: an operator who wrote half of it meant to turn the watch on, and
     * a node that silently ran without it would fail over at the TTL while
     * the manifest said otherwise.
     */
    private static java.util.Optional<MembershipConfig> membership(Map<String, String> settings) {
        java.util.Optional<String> api = optionalText(settings, MEMBERSHIP_API);
        if (api.isEmpty()) {
            for (String dependent : java.util.List.of(MEMBERSHIP_NAMESPACE, MEMBERSHIP_SERVICE,
                    MEMBERSHIP_TOKEN_FILE, MEMBERSHIP_CA_FILE)) {
                if (optionalText(settings, dependent).isPresent()) {
                    throw new ConfigurationException(dependent + " is set but "
                            + MEMBERSHIP_API + " is not, so the EndpointSlice watch it "
                            + "configures would never run");
                }
            }
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(new MembershipConfig(api.get(),
                required(settings, MEMBERSHIP_NAMESPACE), required(settings, MEMBERSHIP_SERVICE),
                optionalText(settings, MEMBERSHIP_TOKEN_FILE),
                optionalText(settings, MEMBERSHIP_CA_FILE)));
    }

    private static Duration duration(Map<String, String> settings, String key, Duration fallback) {
        String value = settings.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        Duration parsed;
        try {
            // ⚠️ ISO-8601 (`PT30S`), which is what `Duration.parse` reads and
            // what every other duration in this tree is written as. A bare
            // `30s` is REFUSED rather than guessed at: guessing turns `30`
            // into 30 of whatever the parser felt like, and a lease TTL is not
            // a setting to be generous about.
            parsed = Duration.parse(value.trim());
        } catch (DateTimeParseException notADuration) {
            throw new ConfigurationException(key + " is not an ISO-8601 duration: " + value
                    + " (for example PT30S, PT5M)", notADuration);
        }
        if (parsed.isZero() || parsed.isNegative()) {
            throw new ConfigurationException(key + " must be positive: " + value);
        }
        return parsed;
    }

    private static long positiveBytes(Map<String, String> settings, String key, long fallback) {
        String value = settings.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException notANumber) {
            throw new ConfigurationException(key + " is not a number of bytes: " + value,
                    notANumber);
        }
        if (parsed <= 0) {
            throw new ConfigurationException(key + " must be positive: " + value);
        }
        return parsed;
    }

    /**
     * A TCP port.
     *
     * <p>⚠️ **REQUIRED, AND 0 IS LEGAL.** 0 asks the kernel for a free port,
     * which is what a test wants; it is not a default, because a deployment
     * whose port was chosen by the kernel publishes an {@code endpoint} no peer
     * can reach and every forwarded commit fails with a connection refused.
     * Making an operator write it is one line of config against a failure that
     * only appears once a second pod exists.
     */
    private static int port(Map<String, String> settings, String key) {
        String value = required(settings, key);
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException notANumber) {
            throw new ConfigurationException(key + " is not a port number: " + value, notANumber);
        }
        if (parsed < 0 || parsed > 65535) {
            throw new ConfigurationException(key + " is not in 0..65535: " + value);
        }
        return parsed;
    }

    /**
     * A comma-separated allow-list.
     *
     * <p>⚠️ **AN EMPTY LIST IS REFUSED RATHER THAN PARSED.** {@code Principal}
     * makes an empty allow-list permit NOTHING, so a node configured with one
     * starts, listens, and answers 403 to every write — which reads to an
     * operator as a broken cluster rather than as the config line they left
     * blank. ⚠️ And the opposite default is worse: this parser must never turn
     * "unset" into "all indices".
     */
    private static Set<String> indices(Map<String, String> settings, String key) {
        Set<String> parsed = new LinkedHashSet<>();
        for (String each : required(settings, key).split(",", -1)) {
            String name = each.trim();
            if (name.isEmpty()) {
                throw new ConfigurationException(key + " has an empty entry: "
                        + settings.get(key) + " (a trailing or doubled comma)");
            }
            if (!parsed.add(name)) {
                // ⚠️ A REPEAT IS A TYPO WORTH NAMING. `Set` would swallow it,
                // and the commonest cause is a second line pasted over a first.
                throw new ConfigurationException(key + " names " + name + " twice");
            }
        }
        return parsed;
    }

    private static boolean bool(Map<String, String> settings, String key, boolean fallback) {
        String value = settings.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "true" -> true;
            case "false" -> false;
            // ⚠️ `Boolean.parseBoolean` READS EVERYTHING ELSE AS FALSE, which
            // for a flag whose true value REFUSES A NODE THAT CANNOT SIGN
            // (M5.43) means a typo silently disables the check it was set to
            // turn on.
            default -> throw new ConfigurationException(
                    key + " must be true or false, not: " + value);
        };
    }

    /** Every setting this parser recognises, sorted — for a usage message. */
    public static Set<String> knownKeys() {
        return new LinkedHashSet<>(new TreeSet<>(KNOWN));
    }
}
