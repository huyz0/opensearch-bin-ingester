// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Serves one segment to many consumers with ONE read and no per-consumer copy
 * (FR-6, M5.12) — the {@code proxy} fetch mode.
 *
 * <p>⚠️ ONE READ PER CALL, HOWEVER LARGE THE FAN-OUT. Doc 10 §4 prices proxy at
 * "1 GET + pod bandwidth" against direct's "N GETs", and that ratio is why
 * proxy is the fallback of choice for a busy stream once a batch is too large
 * to inline. ⚠️ NOT "the default" -- an earlier draft said that, and it
 * collides with M5's SPEC ("`inline` is the default and stays it") and with
 * {@code FetchPolicy}'s load-bearing inline-first ordering. The code is
 * right; the sentence was not.
 *
 * <p>⚠️ THAT IS NOT YET cost.md R5, and an earlier draft of this paragraph
 * claimed it was. R5 is "one fetch per object per NODE, shared by every shard
 * on it"; this class holds no cache and coalesces nothing in flight, so a
 * second call for the same segment is a second GET. M5's SPEC names "a late
 * subscriber" as a proxy trigger, and a late subscriber is by construction not
 * in a list that has already been served — so one fan-out of 64 plus three
 * late subscribers is four GETs for one object on one node. ⚠️ The per-AZ
 * prefetch that makes it one-per-node is M5.16's, and the cache SPEC rule R11
 * puts in the ingester pods is not built; until then this class's only byte
 * source is the store. M5.40 owns closing it.
 *
 * <p>⚠️ AND PER-CONSUMER FETCHING IS NOT AN NFR-4 VIOLATION, which an earlier
 * draft of this paragraph also got wrong — the same inversion M5.10's review
 * corrected in {@code BinStore.presign}'s javadoc, written again here. NFR-4
 * forbids scaling with shards, partitions or indices, and a consumer is one per
 * NODE, which NFR-4 explicitly allows. The disproof is in this milestone's own
 * subject: {@code direct} IS one GET per consumer and is a sanctioned mode. The
 * real objection to fetching per consumer is narrower and sufficient: proxy
 * exists to make a large fan-out cheap, so paying K GETs where one serves all K
 * spends the entire advantage the mode has over {@code direct}.
 *
 * <p>⚠️ STREAMED, NEVER BUFFERED (ADR-0004). The loop below reads a chunk and
 * hands that same array to every consumer before reading the next, so the
 * bytes this class materialises are {@code chunkBytes} — independent of the
 * SEGMENT SIZE and of K. Buffering the segment and forwarding it is the
 * implementation M5's SPEC names as this row's falsifier: it makes service
 * memory scale with fan-out, which is NFR-6, and it is what ADR-0004 rejected
 * at $3,732/month.
 *
 * <p>⚠️ THE ORDER OF THE TWO LOOPS IS THE PROPERTY. Reading the whole segment
 * and then looping over consumers is buffer-then-forward however small the
 * chunks are; reading a chunk and then looping over consumers is streaming.
 * Nothing but the nesting distinguishes them, which is why
 * {@code SegmentProxyTest} asserts the peak hand-off rather than reading this
 * file.
 *
 * <p>⚠️ NOTHING HERE PUTS A MODE ON THE WIRE. {@code Push} still carries bytes
 * inline and {@code FetchMode} is not serialized; M5.14 owns the subscription's
 * shape, its ADR and its golden files. Shipping half a format change is what
 * `wire-format-change`'s one-commit rule forbids.
 */
public final class SegmentProxy {

    /**
     * 64 KiB — large enough that a syscall per chunk is not the cost, small
     * enough that the bound is uninteresting beside a 256 MB heap.
     *
     * <p>⚠️ IT IS NOT DERIVED FROM THE SEGMENT SIZE, deliberately. A chunk that
     * scaled with the segment would satisfy "flat in K" and still let one
     * 32 MiB bulk body (the ceiling {@code IngestConfig} documents) materialise
     * whole.
     */
    public static final int DEFAULT_CHUNK_BYTES = 64 * 1024;

    private final BinStore store;
    private final int chunkBytes;

    public SegmentProxy(BinStore store) {
        this(store, DEFAULT_CHUNK_BYTES);
    }

    public SegmentProxy(BinStore store, int chunkBytes) {
        this.store = Objects.requireNonNull(store, "store");
        if (chunkBytes <= 0) {
            throw new IllegalArgumentException(
                    "a chunk of " + chunkBytes + " bytes streams nothing");
        }
        this.chunkBytes = chunkBytes;
    }

    /** The largest slice any consumer is handed. */
    public int chunkBytes() {
        return chunkBytes;
    }

    /**
     * Streams {@code segmentKey} to every sink in {@code consumers}.
     *
     * <p>⚠️ A CONSUMER THAT THROWS IS DROPPED, NOT PROPAGATED, and the read
     * continues for the rest. This is {@code SubscriptionHub.publishRun}'s
     * discipline and it is here for the same reason: the commit is already
     * durable, so a dead consumer must not stall or roll back a write that
     * succeeded — it falls behind and recovers from the commit log, which is
     * what the log is for.
     *
     * <p>⚠️ A SINK THAT BLOCKS IS NOT HANDLED, and an earlier draft of this
     * paragraph said "slow or dead" as though it were. Only a sink that THROWS
     * is dropped. A sink that simply does not return — the expected shape once
     * these bytes go to a consumer's socket — holds the serving thread, this
     * buffer and the open store stream for the whole fan-out. The failure is
     * not hypothetical: consumer 7 of 64 stalls on a zero TCP window, the store
     * connection idles past its read timeout and throws, this method
     * propagates, and all 64 sinks are left holding a truncated prefix — the
     * outcome the paragraph below says the design avoids. ⚠️ Closing it needs a
     * per-sink deadline and somewhere to put the slow ones, which is a design
     * this row does not carry: M5.41 owns it, and until then a caller must not
     * hand this method a sink that can block indefinitely.
     *
     * <p>⚠️ THE READ IS NOT ABANDONED WHEN ONE CONSUMER DIES, because the
     * others are mid-segment and a truncated stream is worse than a slow one:
     * a consumer that received half a segment has no way to tell that from a
     * whole one.
     *
     * <p>⚠️ ONE CALL IS ONE READ, SO THE CALLER OWNS THE GRANULARITY. This
     * method makes no attempt to recognise that two calls name the same
     * segment, so a caller looping the per-{@code RunKey} subscriber map would
     * issue one GET per SHARD per flush -- ~1,600 for a single 8 MiB segment.
     * Call it ONCE PER SEGMENT with every interested sink, not once per run.
     * ⚠️ AND ONE ENTRY PER CONSUMER, not one per subscription: a node
     * subscribed to several runs of the same segment is ONE sink, not several.
     * This method takes a {@code List} and writes every chunk to every entry,
     * so the flattened union of a per-{@code RunKey} map sends one node the
     * same 8 MiB once per {@code RunKey} it hosts -- around 178 of a segment's
     * ~1,600 runs. The GET count stays 1 and every test stays green; what is
     * spent is the pod bandwidth doc 10 §4 prices beside it.
     * ⚠️ Nothing enforces that here and no test can see it, because every test
     * in this file measures a single call; M5.40 carries it.
     *
     * <p>⚠️ AN EMPTY LIST STILL READS, and that is a description rather than a
     * recommendation. An earlier draft justified it by claiming the same branch
     * reused for "every consumer has died" would stop store failures being
     * reported; round-3 review showed that argument is simply wrong -- an entry
     * guard on {@code consumers.isEmpty()} is not that branch and cannot become
     * one, because the loop below never re-checks emptiness and the paragraph
     * above forbids abandoning the read on consumer death.
     *
     * <p>⚠️ SO THE HONEST STATEMENT IS A WARNING, NOT A DEFENCE: calling this
     * with nothing to serve buys a GET for no one. M5's SPEC criterion 8 names
     * "a serving path that fetches once per empty poll" as the falsifier its
     * zero-idle-requests proof must red against, so a caller that reaches here
     * on an empty poll is building exactly what M5.19 has to disprove. The
     * guard belongs at the call site, where whether the poll was empty is
     * known.
     *
     * @return how many sinks received the whole segment
     * @throws IOException if the STORE fails — that is not one consumer's
     *     problem and every consumer's stream is then incomplete
     */
    public int streamTo(String segmentKey, List<? extends SegmentSink> consumers)
            throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(consumers, "consumers");
        // ⚠️ REJECTED HERE RATHER THAN ABSORBED BELOW. The catch around each
        // hand-off treats any throw as "this consumer is gone", so a null in
        // the caller's list would be silently dropped as a dead consumer --
        // a caller's bug reported as a consumer's death, with no log, metric
        // or diagnostic anywhere. Round-1 review named it.
        for (SegmentSink sink : consumers) {
            Objects.requireNonNull(sink, "a null sink is a caller error, not a dead consumer");
        }

        // ⚠️ ONE array for the whole fan-out, allocated once before the read.
        // Allocating inside the loop would be correct and would still be flat
        // in K; it is out here so that the ONLY allocation proportional to
        // anything is this one, and it is proportional to the chunk.
        byte[] buffer = new byte[chunkBytes];
        List<SegmentSink> live = new ArrayList<>(consumers);

        try (InputStream in = store.get(segmentKey)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (read == 0) {
                    continue;
                }
                // ⚠️ CHUNK OUTER, CONSUMER INNER. Swapping these two loops is
                // buffer-then-forward, and is the mutation this class exists
                // to make fail.
                for (int i = live.size() - 1; i >= 0; i--) {
                    try {
                        live.get(i).write(buffer, 0, read);
                    } catch (IOException | RuntimeException slowOrDeadConsumer) {
                        // ⚠️ Iterating BACKWARDS is what makes removal safe
                        // here without a copy per chunk.
                        live.remove(i);
                    }
                }
            }
        }
        return live.size();
    }
}
