// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * `direct` served: one grant per CONSUMER, clamped, and never logged (M5.45d).
 *
 * <p>⚠️ THE MODE WAS UNREACHABLE FROM ITS ONLY CALLER until this row.
 * {@code publishSegment} forced `proxy` whenever this pod did not hold the
 * bytes, without consulting the policy at all — so {@code FetchPolicy}'s COLD
 * arm, the one `direct` exists for, could not be reached. Assembling the mode
 * meant asking the policy in that case too, with {@code bytesInServingAz=false}.
 */
class DirectServingTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static SegmentServing directServing(StoreFakes.CanPresign signer,
            CountingBinStore store, int threshold) {
        return new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(1L, 1L, threshold, true)),
                store.capabilities(), new SegmentProxy(store), new GrantIssuer(store));
    }

    private static SegmentServing directServing(CountingBinStore store, int threshold) {
        return new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(1L, 1L, threshold, true)),
                store.capabilities(), new SegmentProxy(store), new GrantIssuer(store));
    }

    /** A cold publish: this pod holds a DIFFERENT segment, so `held` is null. */
    private static void publishCold(SubscriptionHub hub, CommitDelta delta, SegmentServing serving) {
        hub.publish(delta, "seg-some-other-pod-wrote", new byte[] {1}, serving);
    }

    /**
     * K runs on ONE consumer cost ONE grant, not K.
     *
     * <p>⚠️ K = 64 RUNS, NOT 64 CONSUMERS, and the distinction is the whole
     * criterion: 64 consumers holding one run each yields 64 grants under a
     * per-consumer implementation AND under a per-run one, so it cannot tell
     * them apart. M5.40a made a `Target` a CONSUMER rather than a subscription,
     * which is what lets one mint serve all K.
     */
    @Test
    void SIXTYFOURRunsOnSEPARATESubscribersCostONESigning() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        CountingBinStore store = new CountingBinStore(signer);
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen =
                java.util.Collections.synchronizedList(new ArrayList<>());

        List<RunCommit> runs = new ArrayList<>();
        List<AutoCloseable> handles = new ArrayList<>();
        for (int partition = 0; partition < 64; partition++) {
            RunKey key = new RunKey(INDEX, partition);
            runs.add(new RunCommit(key, 2, 100L * partition));
            // ⚠️ A FRESH SUBSCRIBER PER RUN, WHICH IS THE PRODUCTION SHAPE.
            // An earlier version passed ONE shared lambda to all 64
            // `subscribe` calls -- a shape no caller in the tree uses, and the
            // only shape under which per-target minting looks like per-consumer
            // minting. M5.40a already measured that every transport registers a
            // fresh subscriber per `RunKey`, so 64 runs are 64 targets here.
            handles.add(hub.subscribe(key, pushes -> {
                seen.addAll(pushes);
                return (buffer, offset, length) -> { };
            }));
        }

        // ⚠️ THRESHOLD 64, BECAUSE 64 SEPARATE SUBSCRIBERS ARE A FAN-OUT OF
        // 64. With the default 1 the policy correctly answers `proxy` -- which
        // is the mode being cheaper at fan-out, not a defect -- and the case
        // would then assert nothing about signing at all. Review's shape
        // correction exposed this: the production subscriber granularity and a
        // low threshold cannot both hold while `direct` is elected.
        publishCold(hub, new CommitDelta(0, "seg-cold", runs),
                directServing(signer, store, 64));
        for (AutoCloseable h : handles) {
            h.close();
        }

        assertThat(seen).as("still one push per run").hasSize(64);
        assertThat(seen.stream().map(SubscriptionHub.Push::via).distinct())
                .as("every one of them `direct`")
                .containsExactly(FetchMode.DIRECT);
        // ⚠️ COUNTING SIGNINGS, NOT DISTINCT GRANTS. `Grant` is a record with
        // value equality and the signer is deterministic per key, so 64 mints
        // of one segment are 64 EQUAL grants -- review measured
        // `.distinct().count()` reading 1 under the very implementation this
        // case exists to forbid.
        assertThat(signer.signings.get())
                .as("ONE signature for one segment, however many runs or subscribers -- "
                        + "per run would be 64, a rate in shards-per-node")
                .isEqualTo(1);
    }

    /**
     * A config whose caps are both `Long.MAX_VALUE` still cannot inline a cold
     * segment.
     *
     * <p>⚠️ {@code MAX <= MAX} IS TRUE, so the policy answers INLINE for a cold
     * delivery under that config — and an `inline` delivery of bytes this pod
     * does NOT hold dereferences a null array inside {@code writeHeldBytes}'s
     * own try, completing every subscriber with nothing. Six fixtures in the
     * tree are that config; none of them publishes cold, which is the only
     * reason the suite was green. Review measured {@code coldMode}'s correction
     * removed and nothing red.
     */
    @Test
    void aMAXCAPConfigStillCannotINLINEAColdSegment() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        CountingBinStore store = new CountingBinStore(signer);
        store.put("seg-cold", io.github.huyz0.os.biningester.binstore.Body.ofBytes(new byte[] {1, 2, 3}));
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen =
                java.util.Collections.synchronizedList(new ArrayList<>());
        SegmentServing maxCaps = new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                store.capabilities(), new SegmentProxy(store));

        try (var ignored = hub.subscribe(new RunKey(INDEX, 0), pushes -> {
            seen.addAll(pushes);
            return (buffer, offset, length) -> { };
        })) {
            publishCold(hub, new CommitDelta(0, "seg-cold",
                    List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))), maxCaps);
        }

        assertThat(seen).singleElement().satisfies(push ->
                assertThat(push.via())
                        .as("bytes we do not hold cannot be inlined AT ANY SIZE")
                        .isEqualTo(FetchMode.PROXY));
    }

    /**
     * A fan-out ABOVE the threshold gets `proxy`, and signs nothing.
     *
     * <p>⚠️ REVIEW MEASURED {@code consumers} REPLACED BY THE LITERAL {@code 1}
     * SURVIVING: {@code coldMode} is the only path that can elect
     * {@code direct}, and {@code directFanOutThreshold} is the only dial keeping
     * it off a wide replay — so 300 cold consumers would get `direct` where the
     * old code gave `proxy`, with nothing red. Every other case sits at
     * {@code 64 <= 64}; this one is the first above the line.
     */
    @Test
    void aFanOutABOVETheThresholdGetsPROXYAndSignsNOTHING() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        CountingBinStore store = new CountingBinStore(signer);
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen =
                java.util.Collections.synchronizedList(new ArrayList<>());

        // ⚠️ THE OBJECT MUST EXIST, because `proxy` READS it -- that is the
        // difference from `direct` this case is about. Without it the publish
        // fails on a missing key and the assertion never runs.
        store.put("seg-cold", io.github.huyz0.os.biningester.binstore.Body.ofBytes(new byte[] {1, 2, 3}));
        RunKey key = new RunKey(INDEX, 0);
        try (var a = hub.subscribe(key, pushes -> {
                    seen.addAll(pushes);
                    return (buffer, offset, length) -> { };
                });
                var b = hub.subscribe(key, pushes -> {
                    seen.addAll(pushes);
                    return (buffer, offset, length) -> { };
                })) {
            publishCold(hub, new CommitDelta(0, "seg-cold",
                    List.of(new RunCommit(key, 2, 0))),
                    directServing(signer, store, 1));
        }

        assertThat(seen.stream().map(SubscriptionHub.Push::via).distinct())
                .as("two consumers is a fan-out of 2, above a threshold of 1 -- so proxy, "
                        + "which is the mode being cheaper at fan-out")
                .containsExactly(FetchMode.PROXY);
        assertThat(signer.signings.get())
                .as("and nothing is signed for a segment nobody fetches themselves")
                .isZero();
    }

    /**
     * In a BATCHED delta the grant names each segment, not the delta's first.
     *
     * <p>⚠️ CRITERION 1c's NAMED WRONG IMPLEMENTATION IS
     * {@code delta.segments().get(0)}, and review caught that it was
     * unmeasurable: every other case here publishes a ONE-segment delta, where
     * {@code get(0)} IS the segment served. Two segments are what tell them
     * apart — ADR-0032's silent data error, which M5.47 measured as
     * independently mutable from the bytes.
     */
    @Test
    void aBATCHEDDeltaGrantsEachSegmentItsOWNKey() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        CountingBinStore store = new CountingBinStore(signer);
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen =
                java.util.Collections.synchronizedList(new ArrayList<>());
        RunKey first = new RunKey(INDEX, 0);
        RunKey second = new RunKey(INDEX, 1);

        try (var a = hub.subscribe(first, pushes -> {
                    seen.addAll(pushes);
                    return (buffer, offset, length) -> { };
                });
                var b = hub.subscribe(second, pushes -> {
                    seen.addAll(pushes);
                    return (buffer, offset, length) -> { };
                })) {
            hub.publish(new CommitDelta(0, List.of(
                    new io.github.huyz0.os.biningester.format.SegmentCommit("seg-first",
                            List.of(new RunCommit(first, 2, 0))),
                    new io.github.huyz0.os.biningester.format.SegmentCommit("seg-second",
                            List.of(new RunCommit(second, 2, 0))))),
                    "seg-this-pod-wrote-neither", new byte[] {1},
                    directServing(signer, store, 1));
        }

        assertThat(seen).hasSize(2);
        assertThat(seen.stream()
                .map(push -> push.segmentKey() + " -> " + push.grant().url())
                .sorted().toList())
                .as("each segment's grant names ITS OWN key, not the first in the delta")
                .containsExactly(
                        "seg-first -> https://store.example/seg-first",
                        "seg-second -> https://store.example/seg-second");
        assertThat(signer.signings.get())
                .as("two segments, two signatures -- one each, and no more")
                .isEqualTo(2);
    }

    /**
     * The grant names the segment being SERVED, not the delta's first.
     *
     * <p>⚠️ ADR-0032's SILENT DATA ERROR, which M5.47 measured as independently
     * mutable from the bytes and M5.59 owns as an open row for the push LABEL.
     * A grant for the wrong key sends a consumer to another segment's bytes
     * with a valid signature.
     */
    @Test
    void theGrantNamesTheSegmentBeingSERVED() throws Exception {
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new ArrayList<>();

        try (var ignored = hub.subscribe(new RunKey(INDEX, 0), pushes -> {
            seen.addAll(pushes);
            return (buffer, offset, length) -> { };
        })) {
            publishCold(hub, new CommitDelta(0, "seg-the-one-served",
                    List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))),
                    directServing(store, 1));
        }

        assertThat(seen).singleElement().satisfies(push -> {
            assertThat(push.segmentKey()).isEqualTo("seg-the-one-served");
            // ⚠️ EXACT, NOT `contains`. Review measured
            // `mintGrant(issuer, segmentKey + "0")` surviving a substring check:
            // a NEIGHBOURING key that contains the served one satisfies it, and
            // neighbouring keys are the normal case given the `h<headerLen>`
            // suffix the key grammar carries. The fake is deterministic, so the
            // whole URL can be named.
            assertThat(push.grant().url())
                    .as("the URL names THIS segment and only this one")
                    .isEqualTo("https://store.example/seg-the-one-served");
            // ⚠️ AND THE PUSH ITSELF CARRIES NO BYTES. Review measured
            // `EMPTY` -> `new byte[] {9}` in the `Push` construction surviving
            // the whole suite: the empty-`Source` case watches the SINK, and
            // the assembling adapter overwrites `segment()` with its own array,
            // so a raw subscriber is the only place this is visible.
            assertThat(push.segment())
                    .as("`direct` sends no bytes by either route")
                    .isEmpty();
        });
    }

    /**
     * The TTL is clamped to ADR-0010's 60 s.
     *
     * <p>⚠️ ASSERTED RELATIVE TO {@code Instant.EPOCH}, NOT THE WALL CLOCK.
     * {@code StoreFakes.CanPresign} expires at {@code EPOCH.plus(ttl)}, always
     * in the past, so `expiresAt().isBefore(now().plusSeconds(60))` passes under
     * a clamping issuer AND one passing `requested` straight through. Measured
     * at M5.43 round 3, before anyone fell into it.
     */
    @Test
    void theTTLIsCLAMPEDToSixtySecondsMeasuredFromTheEPOCH() throws Exception {
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new ArrayList<>();

        try (var ignored = hub.subscribe(new RunKey(INDEX, 0), pushes -> {
            seen.addAll(pushes);
            return (buffer, offset, length) -> { };
        })) {
            publishCold(hub, new CommitDelta(0, "seg-cold",
                    List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))),
                    directServing(store, 1));
        }

        // ⚠️ SIXTY SECONDS WRITTEN OUT, not `GrantIssuer.MAX_CEILING`. Review
        // measured the constant form surviving a widening of both ceilings to an
        // hour: the assertion moves with the thing it is asserting. ADR-0010's
        // number belongs here as a number.
        assertThat(Duration.between(Instant.EPOCH, seen.get(0).grant().expiresAt()))
                .as("ADR-0010's 60 s, and the fake's expiry is EPOCH-relative so this measures "
                        + "the TTL the issuer asked for rather than the wall clock")
                .isLessThanOrEqualTo(Duration.ofSeconds(60));
    }

    /**
     * A grant on a non-`direct` push is refused, and a `direct` push with none.
     *
     * <p>⚠️ REVIEW MEASURED BOTH GUARDS UNCONSTRAINED: deleting either left the
     * whole suite green, because no case anywhere built a mismatched pair while
     * the row claimed both refusals as a property of this commit. The first
     * matters because a grant on an `inline` push is a signed URL minted for a
     * consumer that will never fetch with it — security.md rule 4 makes an
     * unnecessary secret a cost, not merely waste.
     */
    @Test
    void aMISMATCHEDViaAndGrantIsRefused() {
        io.github.huyz0.os.biningester.format.Grant grant = new io.github.huyz0.os.biningester.format.Grant(
                "https://store.example/x", Instant.ofEpochMilli(1_000L));

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                new SubscriptionHub.Push(new RunKey(INDEX, 0), "seg", 1, 0L,
                        FetchMode.INLINE, new byte[0], grant))
                .as("a grant on an inline push mints a secret nobody fetches with")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a grant is for `direct`")
                // ⚠️ AND THE MESSAGE IS A LOG LINE ONE HOP AWAY. Review
                // measured `+ " " + grant.url()` on this very message
                // surviving the whole suite, while `DefaultIngest.pushLoop`
                // logs `e.getMessage()` at WARNING -- security.md rule 4
                // defeated through the channel the sibling case does not watch.
                .hasMessageNotContaining("store.example");

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                new SubscriptionHub.Push(new RunKey(INDEX, 0), "seg", 1, 0L,
                        FetchMode.DIRECT, new byte[0], null))
                .as("and a direct push without one tells a consumer to fetch and not how")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without a grant");
    }

    /**
     * `assembling` carries the grant through to its consumer.
     *
     * <p>⚠️ A LIVE DEFECT REVIEW MEASURED, not a hypothetical: the adapter
     * rebuilt each push with the six-argument constructor, dropping the grant,
     * so a `direct` push hit {@code Push}'s own "without a grant" guard, the
     * throw landed inside {@code complete()}, and {@code deliver} swallowed it
     * as a slow subscriber. The consumer lost the whole window with no log and
     * no counter — an invariant added in one place and not honoured in another.
     */
    @Test
    void theASSEMBLINGAdapterCarriesTheGrant() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        CountingBinStore store = new CountingBinStore(signer);
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> got = new ArrayList<>();

        try (var ignored = hub.subscribe(new RunKey(INDEX, 0),
                SubscriptionHub.assembling(got::add))) {
            publishCold(hub, new CommitDelta(0, "seg-cold",
                    List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))),
                    directServing(signer, store, 1));
        }

        assertThat(got).singleElement().satisfies(push -> {
            assertThat(push.via()).isEqualTo(FetchMode.DIRECT);
            assertThat(push.grant())
                    .as("the consumer is told HOW to fetch, not merely that it must")
                    .isNotNull();
            assertThat(push.grant().url()).contains("seg-cold");
        });
    }

    /**
     * A signing failure is an {@code UncheckedIOException} naming the segment.
     *
     * <p>⚠️ REVIEW MEASURED BOTH FAILURE BRANCHES DEAD TO THE SUITE — the
     * null-issuer guard and the {@code catch (IOException)} — so the javadoc's
     * claim that {@code DefaultIngest.pushLoop} counts it and names the segment
     * was asserted nowhere. It is the same shape as a failed READ, and for the
     * same reason: the commit is durable, so a signing failure must not roll it
     * back.
     */
    @Test
    void aSIGNINGFailureNamesTheSegmentAndDoesNotRollBack() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        signer.failSigningWith = new java.io.IOException("the signer is unavailable");
        CountingBinStore store = new CountingBinStore(signer);
        SubscriptionHub hub = new SubscriptionHub();

        try (var ignored = hub.subscribe(new RunKey(INDEX, 0), pushes ->
                (buffer, offset, length) -> { })) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                    publishCold(hub, new CommitDelta(0, "seg-unsignable",
                            List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))),
                            directServing(signer, store, 1)))
                    .as("the segment is named, because `pushLoop` holds only the key this pod "
                            + "wrote and that is never the one that failed")
                    .isInstanceOf(java.io.UncheckedIOException.class)
                    .hasMessageContaining("seg-unsignable");

            // ⚠️ AND "DOES NOT ROLL BACK" IS THE SECOND HALF OF THE NAME,
            // which review measured this case not asserting at all. The catch
            // is PER SEGMENT, so a signer outage on segment 1 of a batched
            // delta must not deny segment 2 -- least of all the one whose bytes
            // are in this pod's hand and need no store. Letting the throw
            // escape the loop leaves the assertion above green and this one red.
            java.util.concurrent.atomic.AtomicLong delivered =
                    new java.util.concurrent.atomic.AtomicLong();
            try (var alsoSubscribed = hub.subscribe(new RunKey(INDEX, 1), pushes ->
                    (buffer, offset, length) -> delivered.addAndGet(length))) {
                org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        hub.publish(new CommitDelta(0, List.of(
                                new io.github.huyz0.os.biningester.format.SegmentCommit("seg-unsignable",
                                        List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))),
                                new io.github.huyz0.os.biningester.format.SegmentCommit("seg-held",
                                        List.of(new RunCommit(new RunKey(INDEX, 1), 2, 0))))),
                                "seg-held", new byte[] {1, 2, 3},
                                directServing(signer, store, 1)))
                        .as("the failure still reaches the caller")
                        .isInstanceOf(java.io.UncheckedIOException.class);
                assertThat(delivered.get())
                        .as("and the segment this pod holds was served anyway")
                        .isEqualTo(3L);
            }
        }
    }

    /**
     * `direct` chosen with no issuer is refused rather than served unsigned.
     */
    @Test
    void DIRECTWithNOIssuerIsRefused() throws Exception {
        StoreFakes.CanPresign signer = new StoreFakes.CanPresign();
        CountingBinStore store = new CountingBinStore(signer);
        SubscriptionHub hub = new SubscriptionHub();
        SegmentServing noIssuer = new SegmentServing(
                new FetchPolicy(new FetchPolicyConfig(1L, 1L, 1, true)),
                store.capabilities(), new SegmentProxy(store));

        try (var ignored = hub.subscribe(new RunKey(INDEX, 0), pushes ->
                (buffer, offset, length) -> { })) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                    publishCold(hub, new CommitDelta(0, "seg-cold",
                            List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))), noIssuer))
                    .as("a deployment enabling `direct` builds an issuer at startup; reaching "
                            + "this arm without one is a wiring defect, not a delivery one")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no grant issuer");
        }
    }

    /**
     * The grant reaches no log, trace or error message.
     *
     * <p>⚠️ CAPTURED WITH {@link LogCapture}, NOT WITH A PROBE OF ITS OWN.
     * The first version of this case attached a root handler that read only
     * {@code getMessage()}, and review MEASURED two leaks surviving the whole
     * module green against it: a {@code System.Logger} call passing the URL as
     * a format ARGUMENT, which lands in {@code LogRecord.getParameters()}, and
     * a bare {@code System.out.println}, which no handler sees at all. The
     * identical leak written with {@code +} concatenation WAS caught, so the
     * case looked sound and pinned one of three channels. {@link LogCapture}
     * is the whole instrument and says why each channel is there.
     *
     * <p>⚠️ AND THE SIGNATURE IS WHAT IS ASSERTED, not the word "grant". A
     * line printing the redaction is fine; a line printing the URL is the
     * defect security.md rule 4 names, and only the secret's own text
     * distinguishes them.
     */
    @Test
    void theGrantReachesNOLogLine() throws Exception {
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new ArrayList<>();

        String output = LogCapture.capturing(() -> {
            try (var ignored = hub.subscribe(new RunKey(INDEX, 0), pushes -> {
                seen.addAll(pushes);
                return (buffer, offset, length) -> { };
            })) {
                publishCold(hub, new CommitDelta(0, "seg-cold",
                        List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))),
                        directServing(store, 1));
            }
        });

        String secret = seen.get(0).grant().url();
        assertThat(secret).as("the fixture must really carry a signature to look for")
                .contains("store.example");
        assertThat(output)
                .as("nothing written carries the signed URL%n%s", output)
                .doesNotContain(secret)
                .doesNotContain("store.example");
    }

    /**
     * The issuer is held per POD, and only when the deployment asked.
     *
     * <p>⚠️ PER-PUBLISH CONSTRUCTION IS NOT REPRESENTABLE once the issuer is a
     * component of {@code SegmentServing}, which the pod builds once -- so this
     * case pins the SHAPE that makes it so rather than a behaviour a mutation
     * could flip. What it does kill is the issuer going missing when enabled,
     * or being built when it was not asked for -- which on either shipping
     * backend would refuse the pod at startup.
     */
    @Test
    void theIssuerIsHeldPerPODAndOnlyWhenAsked() throws Exception {
        CountingBinStore capable = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        // ⚠️ FROM THE HELPER, NOT BY HAND. Writing the nine components out
        // meant inventing an interval ceiling below `NEVER`'s floor, which the
        // config refuses -- the fixture failed on its own arithmetic rather
        // than on anything this case is about.
        IngestConfig base = IngestTestSupport.pinnedIntervalConfig(
                IngestTestSupport.NEVER, 8L << 20);
        IngestConfig enabled = new IngestConfig(base.intervalFloor(), base.maxSegmentBytes(),
                base.trustDomain(), base.maxQueuedPushBytes(), base.intervalCeiling(),
                base.fillRatioLowThreshold(), base.fillRatioHighThreshold(),
                base.intervalLengthenDelay(), base.intervalShortenDelay(), true);

        try (DefaultIngest pod = new DefaultIngest(enabled, capable, IngestTestSupport.PREFIX,
                "pod1", IngestTestSupport.sequencer(capable, "pod1"), hub,
                java.time.Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            assertThat(pod.serving().issuer())
                    .as("enabled, so the pod holds one")
                    .isNotNull();

        }

        CountingBinStore plain = new CountingBinStore(new io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore());
        SubscriptionHub hub2 = new SubscriptionHub();
        try (DefaultIngest pod = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(IngestTestSupport.NEVER, 8L << 20),
                plain, IngestTestSupport.PREFIX, "pod1",
                IngestTestSupport.sequencer(plain, "pod1"), hub2,
                java.time.Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            assertThat(pod.serving().issuer())
                    .as("not asked for, so none -- and a backend that cannot sign has none to give")
                    .isNull();
        }
    }

    /**
     * Serving `direct` issues NO store request, which is the mode's whole point.
     */
    @Test
    void servingDIRECTIssuesNOStoreRequest() throws Exception {
        CountingBinStore store = new CountingBinStore(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        long before = store.counts().total();

        java.io.ByteArrayOutputStream sent = new java.io.ByteArrayOutputStream();
        try (var ignored = hub.subscribe(new RunKey(INDEX, 0), pushes ->
                (buffer, offset, length) -> sent.write(buffer, offset, length))) {
            publishCold(hub, new CommitDelta(0, "seg-cold",
                    List.of(new RunCommit(new RunKey(INDEX, 0), 2, 0))),
                    directServing(store, 1));
        }

        assertThat(store.counts().total() - before)
                .as("the CONSUMER reads; this pod issues no store request")
                .isZero();
        // ⚠️ AND SENDS NO BYTES EITHER, which the request count cannot see.
        // Review measured the DIRECT arm's `sinks -> { }` replaced by a write of
        // three bytes surviving: memory handed to a sink costs no store request,
        // so "sends nothing" was asserted nowhere.
        assertThat(sent.toByteArray())
                .as("`direct` means the consumer fetches; this pod hands over no bytes")
                .isEmpty();
    }
}
