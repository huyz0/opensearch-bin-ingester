// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Per-index admission quotas (M11.8, ADR-0078, ADR-0010 mechanism 1): two debt
 * token buckets per index -- bytes/s and records/s, each with a burst of one
 * second's rate -- and a cap on the requests one index may have admitted at
 * once.
 *
 * <p>⚠️ **CHECKED BEFORE THE BODY IS READ, CHARGED AS IT IS APPENDED.** A
 * request is admitted while both of its index's buckets are non-negative, and
 * may take them into debt; it is never cut off part-way, because the prefix it
 * appended is durable-bound. The in-flight cap is what bounds that debt under
 * concurrency (ADR-0078 decision 2a): without it, every request arriving while
 * the bucket is still full passes the check before any of them is charged.
 *
 * <p>⚠️ **A MAP LOOKUP, NEVER A NETWORK CALL, AND PER POD**: a fleet of N pods
 * admits up to N times an index's configured rate (ADR-0078 decision 5).
 *
 * <p>⚠️ **UNLIMITED IS THE DEFAULT.** An index with no rate configured has no
 * bucket and no cap, so a pod configured with nothing behaves as before M11.
 */
public final class IndexQuotas {

    /** A rate of 0 is unlimited. */
    public record Limit(long bytesPerSecond, long recordsPerSecond) {

        /** No limit on either. */
        public static final Limit UNLIMITED = new Limit(0, 0);

        public Limit {
            if (bytesPerSecond < 0 || recordsPerSecond < 0) {
                throw new IllegalArgumentException("a rate is never negative: "
                        + bytesPerSecond + " B/s, " + recordsPerSecond + " records/s");
            }
        }

        boolean unlimited() {
            return bytesPerSecond == 0 && recordsPerSecond == 0;
        }
    }

    /** ADR-0078 decision 2a's default cap on one quota'd index's admitted requests. */
    public static final int DEFAULT_MAX_IN_FLIGHT_PER_INDEX = 8;

    /** How long an index's bucket may sit idle and full before it is dropped (M12.4). */
    public static final Duration DEFAULT_IDLE_EXPIRY = Duration.ofMinutes(5);

    /**
     * The pod's quota configuration: a default, per-index overrides, the
     * in-flight cap, and how long an idle bucket is kept (M12.4).
     */
    public record Config(Limit defaults, Map<String, Limit> perIndex, int maxInFlightPerIndex,
            Duration idleExpiry) {

        /** No quota anywhere: the default. */
        public static Config none() {
            return new Config(Limit.UNLIMITED, Map.of(), DEFAULT_MAX_IN_FLIGHT_PER_INDEX);
        }

        /** The same, keeping an idle bucket for {@link #DEFAULT_IDLE_EXPIRY}. */
        public Config(Limit defaults, Map<String, Limit> perIndex, int maxInFlightPerIndex) {
            this(defaults, perIndex, maxInFlightPerIndex, DEFAULT_IDLE_EXPIRY);
        }

        public Config {
            Objects.requireNonNull(idleExpiry, "idleExpiry");
            if (idleExpiry.isNegative() || idleExpiry.isZero()) {
                throw new IllegalArgumentException("idleExpiry is positive: " + idleExpiry);
            }
            Objects.requireNonNull(defaults, "defaults");
            perIndex = Map.copyOf(Objects.requireNonNull(perIndex, "perIndex"));
            if (maxInFlightPerIndex < 1) {
                throw new IllegalArgumentException("maxInFlightPerIndex admits at least one "
                        + "request: " + maxInFlightPerIndex);
            }
        }
    }

    /**
     * One admitted request's charge and in-flight slot, held until the request
     * ends -- ⚠️ NOT UNTIL ITS RECORDS ARE BUFFERED, which is when the pod's
     * lane permit goes back (M11.7): the cap bounds the debt only while it
     * counts every request not yet finished (M11.8 review P1).
     */
    public interface Ticket {

        /** Charges {@code records} to the request's index, possibly into debt. */
        void charge(List<SegmentRecord> records);

        /** Gives the request's in-flight slot back; releasing twice returns it once. */
        void release();
    }

    /** Why a request was refused, and when to retry. */
    public record Refusal(String reason, long retryAfterSeconds) {
    }

    private static final Ticket FREE = new Ticket() {
        @Override
        public void charge(List<SegmentRecord> records) {
        }

        @Override
        public void release() {
        }
    };

    private final Config config;
    private final Clock clock;
    private final Predicate<String> known;
    private final java.util.function.Function<String, List<String>> aliases;
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();
    /** When idle buckets were last swept; at most one sweep per idle expiry (M12.4). */
    private volatile long sweptAt = Long.MIN_VALUE;

    /** Quotas that refuse nothing: what a front door built without a configuration uses. */
    public static IndexQuotas none() {
        return new IndexQuotas(Config.none(),
                Clock.fixed(java.time.Instant.EPOCH, java.time.ZoneOffset.UTC), name -> false,
                name -> List.of());
    }

    /**
     * Quotas over the indices {@code known} names (M12.4, M11 review F6).
     *
     * <p>⚠️ ONLY A KNOWN INDEX GETS A BUCKET: with a default quota, a bucket per
     * name a producer sends -- and none ever removed -- grew with the names an
     * unauthenticated producer cared to invent (security.md rule 5).
     *
     * <p>⚠️ A NAME UNKNOWN AT ADMISSION IS BOUND ONCE IT IS KNOWN, not waved
     * through (M12.4 review P1): a routed write may wait for its index's
     * registration (ADR-0015) and then stream its whole body. ⚠️ AND ITS FIRST
     * CHUNK IS CHARGED BEFORE THAT WAIT (review round 2): {@code BulkService}
     * charges a chunk, then appends it, and the wait is inside the append. So
     * the ticket tallies what it is charged while unbound; the first charge
     * after registration binds it -- a slot in the index's bucket, past the
     * cap if need be, since the request cannot be refused mid-body -- and
     * charges the tally with it; and a request whose only chunk came before
     * the wait binds at its release and is charged then. A name never
     * registered charges nothing, and is refused downstream.
     *
     * <p>⚠️ AND AN OVERRIDE NAMED BY ONE OF AN INDEX's ALIASES is found too
     * (M12.13, M11.8 P5): the front door charges the concrete index, so an
     * override an operator keyed by the alias they write through matched
     * nothing, silently, and the index ran on the default. ⚠️ THE ONLY
     * CONSTRUCTOR (M13.6a, M12 harvest R5): the three-argument one defaulted
     * the aliases to none, which is that same silent default by another door.
     *
     * @param aliases the aliases naming a concrete index now; an override on
     *     the concrete name wins, then the first alias in sorted order
     */
    public IndexQuotas(Config config, Clock clock, Predicate<String> known,
            java.util.function.Function<String, List<String>> aliases) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.known = Objects.requireNonNull(known, "known");
        this.aliases = Objects.requireNonNull(aliases, "aliases");
    }

    /** How many indices hold a bucket: bounded by the known indices (M12.4). */
    public int bucketCount() {
        return buckets.size();
    }

    /**
     * Admits one request to {@code index}, or says why not.
     *
     * @return the admitted request's ticket, or the refusal
     */
    public Admission admit(String index) {
        Objects.requireNonNull(index, "index");
        Limit limit = limitFor(index);
        if (limit.unlimited()) {
            return new Admission(Optional.of(FREE), Optional.empty());
        }
        if (!known.test(index)) {
            return new Admission(Optional.of(new DeferredTicket(index, limit)), Optional.empty());
        }
        long now = clock.millis();
        sweepIdle(now);
        while (true) {
            Bucket bucket = buckets.computeIfAbsent(index, name -> new Bucket(limit,
                    config.maxInFlightPerIndex(), now));
            Admission admitted = bucket.admit(index, now);
            if (admitted != null) {
                return admitted;
            }
            buckets.remove(index, bucket); // swept while we held it: take a fresh one
        }
    }

    /** The concrete index's own override, else its aliases' in sorted order, else the default. */
    private Limit limitFor(String index) {
        Limit own = config.perIndex().get(index);
        if (own != null) {
            return own;
        }
        List<String> named = new java.util.ArrayList<>(aliases.apply(index));
        named.sort(null);
        for (String alias : named) {
            Limit byAlias = config.perIndex().get(alias);
            if (byAlias != null) {
                return byAlias;
            }
        }
        return config.defaults();
    }

    /** A slot in {@code index}'s bucket without the cap's check, for a request already admitted. */
    private BucketTicket enterAdmitted(String index, Limit limit) {
        long now = clock.millis();
        while (true) {
            Bucket bucket = buckets.computeIfAbsent(index, name -> new Bucket(limit,
                    config.maxInFlightPerIndex(), now));
            if (bucket.enterUnchecked(now)) {
                return new BucketTicket(bucket);
            }
            buckets.remove(index, bucket); // swept while we held it: take a fresh one
        }
    }

    /**
     * Drops the buckets idle for the configured expiry, at most once per expiry.
     *
     * <p>⚠️ ONLY A FULL ONE, with nothing in flight: a fresh bucket starts full,
     * so dropping a full bucket loses nothing, and dropping one in debt would
     * forgive the debt.
     */
    private void sweepIdle(long now) {
        long idle = config.idleExpiry().toMillis();
        long last = sweptAt;
        if (last != Long.MIN_VALUE && now - last < idle) {
            return;
        }
        sweptAt = now;
        buckets.forEach((name, bucket) -> {
            if (bucket.expireIfIdle(now, idle)) {
                buckets.remove(name, bucket);
            }
        });
    }

    /** Exactly one of a ticket and a refusal. */
    public record Admission(Optional<Ticket> ticket, Optional<Refusal> refusal) {
        public Admission {
            if (ticket.isPresent() == refusal.isPresent()) {
                throw new IllegalArgumentException("an admission is a ticket or a refusal");
            }
        }
    }

    private final class Bucket {
        private final Limit limit;
        private final int maxInFlight;
        private double bytes;
        private double records;
        private long refilledAt;
        private int inFlight;
        private long usedAt;
        /** Swept: an admission that finds it so takes a fresh bucket instead. */
        private boolean expired;

        Bucket(Limit limit, int maxInFlight, long now) {
            this.limit = limit;
            this.maxInFlight = maxInFlight;
            // ⚠️ A BURST OF ONE SECOND's RATE, full at the start.
            this.bytes = limit.bytesPerSecond();
            this.records = limit.recordsPerSecond();
            this.refilledAt = now;
            this.usedAt = now;
        }

        synchronized boolean expireIfIdle(long now, long idle) {
            refill(now);
            if (inFlight == 0 && now - usedAt >= idle && bytes >= limit.bytesPerSecond()
                    && records >= limit.recordsPerSecond()) {
                expired = true;
            }
            return expired;
        }

        private void refill(long now) {
            double seconds = Math.max(0, now - refilledAt) / 1000.0;
            refilledAt = Math.max(refilledAt, now);
            bytes = Math.min(limit.bytesPerSecond(), bytes + seconds * limit.bytesPerSecond());
            records = Math.min(limit.recordsPerSecond(),
                    records + seconds * limit.recordsPerSecond());
        }

        /** Takes a slot for a request already admitted; false if this bucket was swept. */
        synchronized boolean enterUnchecked(long now) {
            if (expired) {
                return false;
            }
            usedAt = now;
            refill(now);
            inFlight++;
            return true;
        }

        /** The admission, or {@code null} if this bucket was swept. */
        synchronized Admission admit(String index, long now) {
            if (expired) {
                return null;
            }
            usedAt = now;
            refill(now);
            if (inFlight >= maxInFlight) {
                // ⚠️ ONE SECOND: a slot frees as soon as one of the index's
                // requests ends, which is a flush interval or two away.
                return new Admission(Optional.empty(), Optional.of(new Refusal("index " + index
                        + " has " + inFlight + " requests admitted, its cap; retry", 1)));
            }
            double debtSeconds = Math.max(
                    limit.bytesPerSecond() == 0 ? 0 : -bytes / limit.bytesPerSecond(),
                    limit.recordsPerSecond() == 0 ? 0 : -records / limit.recordsPerSecond());
            if (debtSeconds > 0) {
                return new Admission(Optional.empty(), Optional.of(new Refusal("index " + index
                        + " is over its quota; retry", (long) Math.ceil(debtSeconds))));
            }
            inFlight++;
            return new Admission(Optional.of(new BucketTicket(this)), Optional.empty());
        }

        synchronized void charge(List<SegmentRecord> charged) {
            charge(framedBytes(charged), charged.size());
        }

        synchronized void charge(long size, long count) {
            usedAt = clock.millis();
            refill(usedAt);
            if (limit.bytesPerSecond() > 0) {
                bytes -= size;
            }
            if (limit.recordsPerSecond() > 0) {
                records -= count;
            }
        }

        synchronized void releaseSlot() {
            usedAt = Math.max(usedAt, clock.millis());
            inFlight--;
        }
    }

    private static long framedBytes(List<SegmentRecord> records) {
        long size = 0;
        for (SegmentRecord record : records) {
            size += Accumulator.estimatedFramedBytes(record);
        }
        return size;
    }

    /**
     * A ticket for a name unknown at admission, bound once the name is known;
     * what it is charged before that is tallied and charged on binding (M12.4).
     */
    private final class DeferredTicket implements Ticket {
        private final String index;
        private final Limit limit;
        private BucketTicket bound;
        private boolean released;
        private long owedBytes;
        private long owedRecords;

        DeferredTicket(String index, Limit limit) {
            this.index = index;
            this.limit = limit;
        }

        @Override
        public synchronized void charge(List<SegmentRecord> records) {
            if (released) {
                return;
            }
            owedBytes += framedBytes(records);
            owedRecords += records.size();
            bindIfKnown();
        }

        @Override
        public synchronized void release() {
            if (released) {
                return;
            }
            bindIfKnown(); // ⚠️ A ONE-CHUNK REQUEST's only charge came before its wait
            released = true;
            if (bound != null) {
                bound.release();
            }
        }

        /** Binds once the name is known, and charges everything owed so far; caller holds this. */
        private void bindIfKnown() {
            if (bound == null && known.test(index)) {
                bound = enterAdmitted(index, limit);
            }
            if (bound != null && (owedBytes > 0 || owedRecords > 0)) {
                bound.bucket.charge(owedBytes, owedRecords);
                owedBytes = 0;
                owedRecords = 0;
            }
        }
    }

    private static final class BucketTicket implements Ticket {
        private final Bucket bucket;
        private boolean held = true;

        BucketTicket(Bucket bucket) {
            this.bucket = bucket;
        }

        @Override
        public void charge(List<SegmentRecord> records) {
            bucket.charge(records);
        }

        @Override
        public synchronized void release() {
            if (held) {
                held = false;
                bucket.releaseSlot();
            }
        }

    }
}
