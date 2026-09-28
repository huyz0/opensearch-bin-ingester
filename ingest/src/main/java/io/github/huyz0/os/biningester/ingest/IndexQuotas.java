// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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

    /** The pod's quota configuration: a default, per-index overrides, the in-flight cap. */
    public record Config(Limit defaults, Map<String, Limit> perIndex, int maxInFlightPerIndex) {

        /** No quota anywhere: the default. */
        public static Config none() {
            return new Config(Limit.UNLIMITED, Map.of(), DEFAULT_MAX_IN_FLIGHT_PER_INDEX);
        }

        public Config {
            Objects.requireNonNull(defaults, "defaults");
            perIndex = Map.copyOf(Objects.requireNonNull(perIndex, "perIndex"));
            if (maxInFlightPerIndex < 1) {
                throw new IllegalArgumentException("maxInFlightPerIndex admits at least one "
                        + "request: " + maxInFlightPerIndex);
            }
        }

        Limit limitFor(String index) {
            return perIndex.getOrDefault(index, defaults);
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
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    /** Quotas that refuse nothing: what a front door built without a configuration uses. */
    public static IndexQuotas none() {
        return new IndexQuotas(Config.none(),
                Clock.fixed(java.time.Instant.EPOCH, java.time.ZoneOffset.UTC));
    }

    public IndexQuotas(Config config, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Admits one request to {@code index}, or says why not.
     *
     * @return the admitted request's ticket, or the refusal
     */
    public Admission admit(String index) {
        Objects.requireNonNull(index, "index");
        Limit limit = config.limitFor(index);
        if (limit.unlimited()) {
            return new Admission(Optional.of(FREE), Optional.empty());
        }
        Bucket bucket = buckets.computeIfAbsent(index, name -> new Bucket(limit,
                config.maxInFlightPerIndex(), clock.millis()));
        return bucket.admit(index, clock.millis());
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

        Bucket(Limit limit, int maxInFlight, long now) {
            this.limit = limit;
            this.maxInFlight = maxInFlight;
            // ⚠️ A BURST OF ONE SECOND's RATE, full at the start.
            this.bytes = limit.bytesPerSecond();
            this.records = limit.recordsPerSecond();
            this.refilledAt = now;
        }

        private void refill(long now) {
            double seconds = Math.max(0, now - refilledAt) / 1000.0;
            refilledAt = Math.max(refilledAt, now);
            bytes = Math.min(limit.bytesPerSecond(), bytes + seconds * limit.bytesPerSecond());
            records = Math.min(limit.recordsPerSecond(),
                    records + seconds * limit.recordsPerSecond());
        }

        synchronized Admission admit(String index, long now) {
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
            refill(clock.millis());
            long size = 0;
            for (SegmentRecord record : charged) {
                size += Accumulator.estimatedFramedBytes(record);
            }
            if (limit.bytesPerSecond() > 0) {
                bytes -= size;
            }
            if (limit.recordsPerSecond() > 0) {
                records -= charged.size();
            }
        }

        synchronized void releaseSlot() {
            inFlight--;
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
