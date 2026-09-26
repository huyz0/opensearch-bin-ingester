// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub.Push;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub.Subscriber;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The serving half of {@link SubscriptionHub}: choosing how one segment's bytes
 * travel, opening a sink per consumer, handing the bytes over once, and
 * completing only the sinks that took them all.
 *
 * <p>⚠️ IT IS A SPLIT, NOT A NEW SEAM (M5.65). {@link SubscriptionHub} sat at
 * exactly 700 of 700 lines and code-structure.md rule 1 is the cap, which
 * non-negotiable 2 forbids raising. Nothing here changed behaviour: every
 * method arrived whole, and the one edit is that the consumer grouping now
 * reads the registry through {@link SubscriptionHub#subscribersFor} instead of
 * touching the map directly, because the {@code Subscription} field it needs is
 * private to that class.
 *
 * <p>⚠️ THE SPLIT LINE IS REGISTRY AGAINST SERVING, and {@link Push} and
 * {@link Subscriber} stayed on the other side of it on purpose: they are the
 * vocabulary of both halves, and every transport in the tree names them
 * {@code SubscriptionHub.Push}. Moving them would have renamed a public type in
 * six call sites to answer a line count.
 *
 * <p>⚠️ PACKAGE-PRIVATE, AND ONE INSTANCE PER HUB. It holds the hub rather than
 * the map so that the registry keeps sole ownership of its own structure --
 * {@code subscribers} is still read and written in exactly one class.
 */
final class SegmentServingPath {

    private final SubscriptionHub hub;

    SegmentServingPath(SubscriptionHub hub) {
        this.hub = hub;
    }

    /**
     * One CONSUMER's share of one segment: every run of it that consumer holds.
     *
     * <p>⚠️ ONE PER CONSUMER, NOT ONE PER SUBSCRIPTION, which is
     * M5.40a. This record used to pair one subscription with one run, so a
     * consumer holding K runs of a segment appeared K times and was handed the
     * bytes K times.
     */
    private record Target(Subscriber subscriber, List<RunCommit> runs) {
    }

    /**
     * A {@link Subscriber} keyed by IDENTITY, so grouping cannot be spoofed.
     *
     * <p>⚠️ {@code ==}, NOT {@code equals}. A {@code Subscriber} is
     * caller-supplied and may implement {@code equals} however it likes; two
     * subscribers that merely compare equal are still two consumers, and
     * merging them would hand one of them the other's stream. A plain
     * {@code HashMap<Subscriber, ...>} would do exactly that.
     *
     * <p>⚠️ AND A LINEAR SCAN WOULD BE O(RUNS x CONSUMERS). Comparing
     * each of a segment's runs against every target already built costs 1,600 x
     * ~9 at the scale the M5.40a row uses -- not ruinous, and quadratic only if
     * consumers grew with runs. It is avoided because a hash lookup is simpler
     * to read than a nested loop, not because the tree is on fire.
     * ⚠️ {@link java.util.IdentityHashMap} IS THE JDK CONSTRUCT FOR
     * THIS and is deliberately not used: it does not keep insertion order, and
     * delivery order is worth having stable.
     */
    private record ByIdentity(Subscriber subscriber) {
        @Override
        public boolean equals(Object other) {
            return other instanceof ByIdentity o && o.subscriber == this.subscriber;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(subscriber);
        }
    }

    void publishSegment(SegmentCommit committed, byte[] heldBytes,
            SegmentServing serving, long sequencerEpoch, long chainSequence) {
        // ⚠️ THE WRITER KEEPS WHAT IT WROTE, BEFORE ANYTHING ELSE (M10.15).
        // ADR-0004 prices the writing AZ at zero GETs because this node still
        // holds the segment; handing the bytes to its own subscribers and then
        // dropping them made its first `/seg` read of every segment a GET
        // (ADR-0073). Admitted before the no-subscriber return below, because
        // the consumer that fetches it may poll another node's subscription.
        // `SegmentCache.put` keeps its own ceiling: a segment above it is not
        // admitted, and the route streams it from the store as before.
        if (heldBytes != null) {
            serving.proxy().cache().put(committed.segmentKey(), heldBytes);
        }
        // ⚠️ LINKED, so delivery order follows first-subscription order rather
        // than a hash, which makes a failure reproducible run to run.
        //
        // ⚠️ NO TEST DEPENDS ON IT, AND AN EARLIER DRAFT OF THIS COMMENT SAID
        // ONE DID. Review measured `HashMap` here surviving the whole suite:
        // the assertions that look like they care use
        // `containsExactlyInAnyOrder`, and `deliver`'s by-index pairing is
        // between its OWN parallel lists, which cannot disagree with each
        // other. So this is a choice about debuggability, not a pinned
        // property -- said plainly rather than dressed as a constraint.
        Map<ByIdentity, List<RunCommit>> byConsumer = new LinkedHashMap<>();
        for (RunCommit run : committed.runs()) {
            for (Subscriber subscriber : hub.subscribersFor(run.key())) {
                byConsumer.computeIfAbsent(new ByIdentity(subscriber), k -> new ArrayList<>())
                        .add(run);
            }
        }
        List<Target> targets = new ArrayList<>(byConsumer.size());
        byConsumer.forEach((consumer, runs) ->
                targets.add(new Target(consumer.subscriber(), runs)));
        // ⚠️ NO READ FOR NOBODY. A segment nobody subscribes to must cost zero
        // requests, which is what makes criterion 8's zero hold at fan-out
        // rather than merely at rest.
        if (targets.isEmpty()) {
            return;
        }

        // ⚠️ THE COLD CASE ASKS THE POLICY TOO, WHICH IS WHAT ASSEMBLING
        // `direct` MEANT. Until now a segment this pod did NOT write was forced
        // to `proxy` without consulting anything -- so `FetchPolicy`'s cold arm,
        // the one `direct` exists for, was unreachable from the only caller.
        // ⚠️ `bytesInServingAz` IS FALSE HERE AND TRUE BELOW, and that is the
        // whole difference: we are holding the bytes in one case and not in the
        // other. Passing `true` for both is what made the arm dead.
        FetchMode via = heldBytes == null
                // ⚠️ AND AN INLINE ANSWER IS CORRECTED RATHER THAN TRUSTED.
                // `MAX <= MAX` is true, so a config whose two caps are both
                // `Long.MAX_VALUE` answers INLINE for a cold segment -- and
                // `writeHeldBytes` then dereferences a null array inside its own
                // try, completing every subscriber with nothing. Six fixtures in
                // the tree are that config and none publishes cold, which is the
                // only reason the suite is green. A segment this pod does not
                // hold cannot be inlined AT ANY SIZE.
                ? coldMode(serving, targets.size())
                : serving.policy().modeFor(
                        // ⚠️ CONSUMERS, NOT SUBSCRIPTIONS, and this number moved
                        // with M5.40a: `targets` used to hold one entry per
                        // (subscription, run), so a handful of nodes holding many
                        // runs each reported a fan-out several times their count.
                        //
                        // ⚠️ IT CHANGES NO MODE TODAY, AND AN EARLIER DRAFT OF
                        // THIS COMMENT CLAIMED OTHERWISE. Review measured it:
                        // `modeFor` reads `segmentFanOut` only through
                        // `wantsDirect`, which first needs
                        // `servingPodUnderPressure || !bytesInServingAz`, and
                        // this caller passes `true` for the second and `false`
                        // for the first -- so the INLINE/PROXY choice here turns
                        // on `batchBytes` alone. The number matters to the
                        // CONTRACT, which M5.43 and M5.45d will read, not to the
                        // branch taken from this line.
                        new SegmentDelivery(heldBytes.length, true, targets.size(), false),
                        serving.capabilities());

        switch (via) {
            case INLINE -> deliver(committed.segmentKey(), targets, FetchMode.INLINE,
                    sinks -> writeHeldBytes(heldBytes, sinks), null, sequencerEpoch, chainSequence);
            // ⚠️ FROM THE HELD ARRAY WHEN WE HAVE ONE, and only from the store
            // when we do not. `proxy` names how the bytes reach the CONSUMER;
            // it is never a reason to buy a GET for bytes this pod is already
            // holding, which would be one wasted request per flush.
            case PROXY -> deliver(committed.segmentKey(), targets, FetchMode.PROXY,
                    heldBytes == null
                            ? sinks -> streamFromStore(serving, committed.segmentKey(), sinks)
                            : sinks -> writeHeldBytesChunked(heldBytes, serving, sinks),
                    null, sequencerEpoch, chainSequence);
            // ⚠️ NO BYTES AT ALL, WHICH IS WHAT `direct` MEANS. The consumer
            // fetches with the grant, so this pod writes nothing to the sink
            // and issues no store request of its own -- the saving the mode
            // exists for. The sink is still opened and completed, because that
            // is how a subscriber learns what it was told about.
            //
            // ⚠️ AND NO CONSUMER CAN ACT ON IT YET. `Delivery` carries no grant
            // and nothing in `client`, `plugin` or `http` mentions
            // `FetchMode.DIRECT`, so a consumer served this way learns a
            // segment exists and has no way to fetch it -- M5.45g builds the
            // seam (M5.45c was split) and M5.45e proves the three modes agree. This arm used to
            // throw, which said so loudly; it now serves, so the gap is stated
            // here instead. In practice it is gated by `directEnabled`
            // defaulting off and by neither shipping backend presigning.
            case DIRECT -> deliver(committed.segmentKey(), targets, FetchMode.DIRECT,
                    sinks -> { }, serving.issuer(), sequencerEpoch, chainSequence);
        }
    }

    /**
     * The mode for a segment this pod does NOT hold: never {@code inline}.
     *
     * <p>⚠️ THE POLICY IS ASKED, AND ITS INLINE ANSWER IS OVERRULED. Before
     * M5.45d this case forced {@code proxy} without asking anything, which is
     * why {@code FetchPolicy}'s cold arm — the one {@code direct} exists for —
     * was unreachable from the only caller. Asking it is the change; correcting
     * {@code INLINE} is the part that keeps the change safe.
     *
     * <p>⚠️ {@code batchBytes} IS UNKNOWABLE HERE. The real size would cost a
     * {@code stat} per segment, which is the request this path exists to avoid,
     * so {@code Long.MAX_VALUE} says "larger than any cap". It is not quite
     * enough on its own: the comparison is {@code <=}, so a config whose caps
     * are also {@code Long.MAX_VALUE} answers {@code INLINE} — and an
     * {@code inline} delivery of bytes we do not hold completes every
     * subscriber with nothing, silently, because {@code writeHeldBytes}
     * swallows the resulting failure per sink.
     */
    private static FetchMode coldMode(SegmentServing serving, int consumers) {
        FetchMode asked = serving.policy().modeFor(
                new SegmentDelivery(Long.MAX_VALUE, false, consumers, false),
                serving.capabilities());
        return asked == FetchMode.INLINE ? FetchMode.PROXY : asked;
    }

    /**
     * One grant for one consumer's read of one segment.
     *
     * <p>⚠️ THE TTL IS THE ISSUER'S CEILING, not a number chosen here.
     * {@code GrantIssuer.grantFor(String)} applies ADR-0010's 60 s, and asking
     * for it by the one-argument form keeps that in one place rather than at
     * every call site.
     *
     * <p>⚠️ AND THE STORE'S {@code SignedUrl} BECOMES A {@code format.Grant}
     * HERE, because this is the boundary where it stops being the store's. The
     * two types exist for the reason ADR-0043 records: {@code format} depends
     * on nothing, so the value that crosses to a consumer cannot be
     * {@code binstore-spi}'s.
     */
    private static io.github.huyz0.os.biningester.format.Grant mintGrant(GrantIssuer issuer, String segmentKey) {
        if (issuer == null) {
            throw new IllegalStateException("`direct` was chosen with no grant issuer; a "
                    + "deployment enabling it constructs one at startup (M5.43)");
        }
        try {
            io.github.huyz0.os.biningester.binstore.SignedUrl signed = issuer.grantFor(segmentKey);
            return new io.github.huyz0.os.biningester.format.Grant(signed.url(), signed.expiresAt());
        } catch (IOException signingFailed) {
            // ⚠️ THE SAME SHAPE AS A FAILED READ, and for the same reason: the
            // commit is already durable, so a signing failure must not roll it
            // back. `DefaultIngest.pushLoop` counts it and names the segment.
            throw new UncheckedIOException(
                    "segment " + segmentKey + " could not be signed for delivery", signingFailed);
        }
    }

    /** How a mode's bytes reach the sinks that were opened for them. */
    private interface Source {
        void writeTo(List<SegmentSink> sinks) throws IOException;
    }

    /**
     * Opens every subscriber's sink, hands the bytes over ONCE, and completes
     * only the sinks that took them all.
     *
     * <p>⚠️ COMPLETING A SINK THAT THREW MID-STREAM WOULD BE THE WORST OUTCOME
     * AVAILABLE: the subscriber would be handed a TRUNCATED PREFIX it cannot
     * tell from a whole segment, and would decode fewer records under offsets
     * the commit log says are there. {@code SegmentProxy.streamTo} drops a
     * throwing sink and returns only a COUNT, so the hub wraps each sink to
     * learn WHICH. ⚠️ A sink that merely BLOCKS is unhandled ON THIS PATH:
     * M5.41 added a per-chunk deadline to {@code streamTo}'s three-argument
     * overload and this hub calls the two-argument one, because a sink dropped
     * for missing a deadline never throws and {@link Tracking} would not mark
     * it. M5.58b owns closing that before the other
     * overload is wired -- and since M5.58a the three-argument form NAMES the
     * sinks it dropped, so what is left there is this class crossing that list
     * off against {@link Tracking}, plus a per-chunk deadline that nothing yet
     * owns.
     *
     * @param issuer mints one grant per SEGMENT for {@code direct}, and is
     *     null for every other mode. ⚠️ PER SEGMENT, and an earlier draft of
     *     this line said per CONSUMER -- which is what the mint did before it
     *     was hoisted, and what made it per RUN for every real caller
     * @param sequencerEpoch the chain epoch every push of this segment carries
     *     (M5.15d), or {@link SubscriptionHub#EPOCH_UNKNOWN} from a publisher
     *     that models no chain. ⚠️ EVERY ARM PASSES IT SEPARATELY, so pinning
     *     one says nothing about the others -- review measured PROXY and DIRECT
     *     passing the sentinel while the whole suite stayed green
     */
    private void deliver(String segmentKey, List<Target> targets, FetchMode via, Source source,
            GrantIssuer issuer, long sequencerEpoch, long chainSequence) {
        List<Tracking> opened = new ArrayList<>();
        List<List<Push>> pushes = new ArrayList<>();
        List<Target> live = new ArrayList<>();
        // ⚠️ ONE GRANT PER SEGMENT, MINTED ABOVE BOTH LOOPS, and an
        // earlier version minted once per TARGET believing that was once per
        // consumer. It is not: grouping is by `Subscriber` IDENTITY, and every
        // transport in the tree registers a FRESH subscriber per `RunKey` --
        // M5.40a measured it ("K is always 1") and M5.62 exists because
        // production never opts into the merge. So per-target minting is
        // per-RUN for every real caller: ~400 signatures for one 8 MiB object
        // on a catch-up node, shards-per-node, which non-negotiable 6 forbids
        // and which `CountingBinStore` cannot see because signing issues no
        // request.
        //
        // ⚠️ HOISTING IS SOUND BECAUSE THE GRANT IS A PURE FUNCTION of
        // `(segmentKey, ceiling)`: every consumer of one segment wants the
        // same URL, so one mint serves all of them whatever the subscriber
        // granularity turns out to be. That makes the rate flat in consumers
        // AND in runs, rather than flat only if callers group the way one
        // test grouped them.
        io.github.huyz0.os.biningester.format.Grant grant = via == FetchMode.DIRECT
                ? mintGrant(issuer, segmentKey)
                : null;
        for (Target t : targets) {
            List<Push> forThisConsumer = new ArrayList<>(t.runs().size());
            for (RunCommit run : t.runs()) {
                forThisConsumer.add(new Push(run.key(), segmentKey, run.recordCount(),
                        run.firstOffset(), via, EMPTY, grant, sequencerEpoch, chainSequence));
            }
            try {
                SegmentSink sink = t.subscriber().open(List.copyOf(forThisConsumer));
                if (sink == null) {
                    continue;
                }
                opened.add(new Tracking(sink));
                pushes.add(List.copyOf(forThisConsumer));
                live.add(t);
            } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                // ⚠️ Swallowed ON PURPOSE. The commit is already durable; a
                // consumer that throws must not roll back or stall a write that
                // succeeded. It falls behind and recovers from the log.
                continue;
            }
        }
        if (opened.isEmpty()) {
            return;
        }
        try {
            source.writeTo(List.copyOf(opened));
        } catch (IOException storeFailed) {
            // ⚠️ NOBODY IS COMPLETED, so no subscriber mistakes a prefix for a
            // segment. `DefaultIngest.pushLoop` catches this and drops the push.
            //
            // ⚠️ AND THE KEY IS IN THE MESSAGE, because this is the last frame
            // that knows it. `pushLoop` holds only the key THIS POD WROTE, and
            // by construction that is never the one that failed -- a read
            // happens only for a segment the pod does NOT hold. A log line up
            // there naming its own key sends an operator to a healthy object.
            throw new UncheckedIOException(
                    "segment " + segmentKey + " could not be read for delivery", storeFailed);
        }
        for (int i = 0; i < opened.size(); i++) {
            Tracking sink = opened.get(i);
            if (sink.failed) {
                continue;
            }
            try {
                live.get(i).subscriber().complete(pushes.get(i), sink.delegate);
                // ⚠️ AFTER `complete`, AND ONLY FOR A SINK THAT TOOK EVERY
                // BYTE (M5.15b). A session's resume point is where the consumer
                // must see the NEXT record, so recording a push the consumer
                // did not receive whole moves that point past records it never
                // got -- the SKIP half of SPEC criterion 11, and the half a
                // consumer cannot detect. The `sink.failed` guard above is what
                // makes this line reachable only for a whole segment.
                hub.delivered(live.get(i).subscriber(), pushes.get(i));
            } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                continue;
            }
        }
    }

    private void writeHeldBytes(byte[] heldBytes, List<SegmentSink> sinks) {
        for (SegmentSink sink : sinks) {
            try {
                sink.write(heldBytes, 0, heldBytes.length);
            } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                continue;
            }
        }
    }

    /**
     * Hands held bytes over a chunk at a time, chunk OUTER and consumer INNER.
     *
     * <p>⚠️ THE BOUND HERE IS THE HAND-OFF, NOT THE RESIDENCY, and that is
     * a weaker claim than {@link SegmentProxy}'s. These bytes are ALREADY
     * materialised -- this pod wrote them -- so no loop order makes the serving
     * path hold less than a segment. What the chunking buys is that no
     * CONSUMER is handed more than {@code chunkBytes} at once, which is the
     * property that stays flat as K grows and the one criterion 6 measures.
     *
     * <p>⚠️ CHUNK-OUTER IS THEREFORE A MATTER OF INTERLEAVING RATHER THAN
     * OF MEMORY: consumer-outer would give the first consumer the whole
     * segment before the second saw a byte. An earlier version of this
     * paragraph claimed the loop order was the memory bound, which is true of
     * {@link SegmentProxy} -- where the bytes are not yet read -- and not of
     * this method. Review MEASURED consumer-outer surviving every test in the
     * tree, and an earlier draft of this paragraph explained that away by
     * claiming nothing below T4 could observe the order. That is false: sinks
     * that append their own identity to ONE SHARED ORDERED LOG separate the
     * two loops exactly, at this tier, which is what
     * {@code AssembledServingPathFailureTest.everySinkGetsTheFIRSTChunkBeforeAnyGetsTheSECOND}
     * now does. The mutation dies.
     */
    private void writeHeldBytesChunked(byte[] heldBytes, SegmentServing serving,
            List<SegmentSink> sinks) {
        int chunk = serving.chunkBytes();
        List<SegmentSink> live = new ArrayList<>(sinks);
        for (int offset = 0; offset < heldBytes.length && !live.isEmpty(); offset += chunk) {
            int length = Math.min(chunk, heldBytes.length - offset);
            for (int i = live.size() - 1; i >= 0; i--) {
                try {
                    live.get(i).write(heldBytes, offset, length);
                } catch (IOException | RuntimeException slowOrDeadSubscriber) {
                    live.remove(i);
                }
            }
        }
    }

    private void streamFromStore(SegmentServing serving, String segmentKey,
            List<SegmentSink> sinks) throws IOException {
        serving.proxy().streamTo(segmentKey, sinks);
    }

    /**
     * A sink that remembers whether it threw, so {@link #deliver} completes only
     * the subscribers that received the whole segment.
     */
    private static final class Tracking implements SegmentSink {
        private final SegmentSink delegate;
        private boolean failed;

        Tracking(SegmentSink delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            try {
                delegate.write(buffer, offset, length);
            } catch (IOException | RuntimeException e) {
                failed = true;
                throw e;
            }
        }
    }

    private static final byte[] EMPTY = new byte[0];
}
