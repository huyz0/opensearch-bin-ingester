// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static binjava.ingest.SegmentProxyFixtures.KEY;
import static binjava.ingest.SegmentProxyFixtures.segment;
import static binjava.ingest.SegmentProxyFixtures.sinks;
import static binjava.ingest.SegmentProxyFixtures.storeHolding;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What {@code proxy} does when a consumer or the store FAILS (M5.12).
 *
 * <p>⚠️ SPLIT FROM {@code SegmentProxyTest} at 700 lines (code-structure.md
 * rule 1), and the seam is the one the review rounds kept finding: every
 * defect in this file was invisible because the happy-path fixtures could not
 * express failure. A {@code ByteArrayInputStream} never fails mid-read and has
 * a {@code close()} that does nothing -- and a production defect hid behind
 * each. ⚠️ Its third blind spot, never returning ZERO, is pinned next door in
 * {@code SegmentProxyTest} rather than here, because a zero read is a normal
 * event on a healthy stream rather than a failure.
 *
 * <p>⚠️ THE CONTRACT THESE PIN IS AN ASYMMETRY. One consumer failing loses one
 * stream and the rest are served; the STORE failing means every consumer's
 * stream is incomplete, so it must reach the caller as a throw and never as a
 * count -- a consumer cannot tell a truncated segment from a whole one.
 */
class SegmentProxyFailureTest {

    /**
     * Criterion 5: a consumer that throws stops neither the others nor the
     * read.
     *
     * <p>⚠️ {@code SubscriptionHub.deliver}'S DISCIPLINE, and here for the
     * same reason: the commit is already durable, so a slow or dead consumer
     * must not stall or roll back a write that succeeded. It falls behind and
     * recovers from the commit log.
     *
     * <p>⚠️ THE SURVIVOR MUST GET THE WHOLE SEGMENT, not merely "some bytes".
     * A proxy that abandoned the read on the first failure would leave every
     * other consumer with a truncated stream — and a consumer holding half a
     * segment has no way to tell that from a whole one.
     */
    @Test
    void aConsumerThatTHROWSStopsNeitherTheOthersNorTheRead() throws Exception {
        byte[] expected = segment();
        CountingBinStore store = storeHolding(expected);
        SegmentProxyFixtures.RecordingSink healthy = new SegmentProxyFixtures.RecordingSink();
        SegmentSink poisoned = (buf, off, len) -> {
            throw new IOException("this consumer is gone");
        };
        int served = new SegmentProxy(store).streamTo(KEY, List.of(poisoned, healthy));
        assertThat(served).as("the dead one is dropped, the live one counted").isEqualTo(1);
        assertThat(healthy.received.toByteArray())
                .as("and the survivor gets the WHOLE segment, not a truncated one")
                .isEqualTo(expected);
    }

    /**
     * A consumer that dies PART WAY THROUGH is dropped without disturbing the
     * others' byte stream.
     *
     * <p>⚠️ THE FIRST CHUNK IS THE EASY CASE. Removal mid-iteration is where a
     * forward-indexed loop skips the consumer that shuffles into the vacated
     * slot, so the survivor beside a mid-stream death would silently lose one
     * chunk — a hole in the middle of a segment, which decodes as corruption
     * rather than as a short read.
     */
    @Test
    void aConsumerThatDiesPARTWAYTHROUGHCostsTheOthersNothing() throws Exception {
        byte[] expected = segment();
        CountingBinStore store = storeHolding(expected);
        SegmentProxyFixtures.RecordingSink first = new SegmentProxyFixtures.RecordingSink();
        SegmentProxyFixtures.RecordingSink last = new SegmentProxyFixtures.RecordingSink();
        int[] seen = {0};
        SegmentSink diesLater = (buf, off, len) -> {
            if (++seen[0] == 3) {
                throw new IOException("gone at the third chunk");
            }
        };
        int served = new SegmentProxy(store).streamTo(KEY, List.of(first, diesLater, last));
        assertThat(served).isEqualTo(2);
        assertThat(first.received.toByteArray()).isEqualTo(expected);
        assertThat(last.received.toByteArray())
                .as("the consumer AFTER the one that died must not lose the chunk it died on")
                .isEqualTo(expected);
    }

    /**
     * A sink throwing an UNCHECKED exception is dropped, exactly as a checked
     * one is.
     *
     * <p>⚠️ ROUND-2 REVIEW MEASURED THE UNCHECKED HALF UNTESTED: narrowing
     * {@code catch (IOException | RuntimeException)} to {@code catch
     * (IOException)} passed all twelve tests, because both poison sinks threw
     * {@code IOException} and nothing made a sink throw unchecked. Criterion 5
     * covered half of what it claimed.
     *
     * <p>⚠️ AND UNCHECKED IS THE LIKELIER HALF. A sink fails inside its own
     * encoder or buffer -- {@code NullPointerException},
     * {@code IndexOutOfBoundsException}, {@code BufferOverflowException} --
     * far more often than it declares an {@code IOException}. Under the
     * narrowed catch that propagates, the try-with-resources closes the store
     * stream mid-segment, and every other consumer is left holding a prefix it
     * cannot tell from a whole segment.
     */
    @Test
    void aSinkThrowingAnUNCHECKEDExceptionIsDroppedToo() throws Exception {
        byte[] expected = segment();
        CountingBinStore store = storeHolding(expected);
        SegmentProxyFixtures.RecordingSink healthy = new SegmentProxyFixtures.RecordingSink();
        SegmentSink unchecked = (buf, off, len) -> {
            throw new IllegalStateException("this sink's encoder blew up");
        };
        int served = new SegmentProxy(store).streamTo(KEY, List.of(unchecked, healthy));
        assertThat(served).isEqualTo(1);
        assertThat(healthy.received.toByteArray())
                .as("an unchecked failure must not truncate everyone else's stream")
                .isEqualTo(expected);
    }

    /**
     * The store stream is CLOSED on the success path AND on the failure path.
     *
     * <p>⚠️ ROUND-2 REVIEW MEASURED THE TRY-WITH-RESOURCES DELETABLE: sixteen
     * tests stayed green, because {@code MemoryBinStore} hands back a
     * {@code ByteArrayInputStream} whose {@code close()} is a no-op and
     * {@code CountingBinStore} counts CALLS rather than closes. Against S3 that
     * is one leaked pooled connection per {@code streamTo}, until the pool
     * exhausts and the pod stops fetching with nothing red anywhere.
     *
     * <p>⚠️ AND THE FAILURE PATH IS THE HALF THAT MATTERS, which round-3 review
     * found this test had claimed and not covered: it served four healthy sinks
     * with no failure anywhere while its own summary said "and on the failure
     * path". A trailing {@code in.close()} in place of try-with-resources
     * closes on success only -- and the failure path is precisely when a
     * connection is most likely to be worth reclaiming.
     */
    @Test
    void theStoreStreamIsCLOSEDOnBOTHPaths() throws Exception {
        byte[] expected = segment();
        long[] ignored = {0};

        SegmentProxyFixtures.StubStore healthy = new SegmentProxyFixtures.StubStore(expected, ignored, 0);
        new SegmentProxy(healthy).streamTo(KEY, sinks(4));
        assertThat(healthy.streamClosed).as("closed after a clean serve").isTrue();

        SegmentProxyFixtures.StubStore breaks = new SegmentProxyFixtures.StubStore(expected, ignored, 0, 3);
        assertThatThrownBy(() -> new SegmentProxy(breaks).streamTo(KEY, sinks(4)))
                .isInstanceOf(IOException.class);
        assertThat(breaks.streamClosed)
                .as("a leaked connection per FAILED serve exhausts the pool just as surely")
                .isTrue();
    }

    /**
     * A store failure PART WAY THROUGH is raised, not counted as a serve.
     *
     * <p>⚠️ ROUND-3 REVIEW: no fixture made the stream fail MID-READ, so a
     * proxy that caught the failure and {@code break}s survived all sixteen
     * tests -- and it returns {@code live.size()}, REPORTING 64 CONSUMERS
     * SERVED WHILE ALL 64 HOLD A TRUNCATED PREFIX. The existing failure test
     * fails at {@code get()} time, before a byte moves, so it covered neither
     * this nor the close above.
     *
     * <p>⚠️ THE ASYMMETRY IS THE WHOLE CONTRACT. One consumer failing loses one
     * stream; the STORE failing means every consumer's stream is incomplete,
     * and a consumer cannot tell a truncated segment from a whole one. So this
     * must reach the caller as a throw, never as a count.
     */
    @Test
    void aStoreFailureMIDREADIsRaisedNotCountedAsAServe() throws Exception {
        byte[] expected = segment();
        long[] ignored = {0};
        SegmentProxyFixtures.StubStore breaks = new SegmentProxyFixtures.StubStore(expected, ignored, 0, 3);
        List<SegmentProxyFixtures.RecordingSink> consumers = sinks(8);

        assertThatThrownBy(() -> new SegmentProxy(breaks).streamTo(KEY, consumers))
                .as("silently returning a count here is a report of 8 served and 8 truncated")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("mid-segment");

        assertThat(consumers.get(0).received.size())
                .as("and the fixture must really have delivered a PREFIX, or this proves nothing")
                .isBetween(1, expected.length - 1);
    }

    /**
     * A store failure is not one consumer's problem and is not swallowed.
     *
     * <p>⚠️ THE ASYMMETRY IS DELIBERATE. A consumer failing means one stream
     * is lost; the store failing means EVERY consumer's stream is incomplete,
     * and reporting that as a successful serve would hand the caller a count
     * of consumers that did not in fact receive a segment.
     */
    @Test
    void aSTOREFailureIsRaisedRatherThanCountedAsAServe() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        assertThatThrownBy(() -> new SegmentProxy(store).streamTo("seg/absent", sinks(4)))
                .isInstanceOf(IOException.class);
    }

    /**
     * A null sink is a CALLER error, refused up front rather than absorbed as
     * a consumer death.
     *
     * <p>⚠️ THE GUARD WAS ADDED IN ROUND 1 AND HAD NO TEST, so round-2 review
     * measured deleting it as surviving: the null then reaches {@code write},
     * throws {@code NullPointerException}, and is swallowed by the very catch
     * the guard's own comment says must not absorb it -- a caller's bug
     * reported as a consumer's death, with nothing anywhere to see.
     */
    @Test
    void aNULLSinkIsACallerErrorNotAConsumerDeath() throws Exception {
        CountingBinStore store = storeHolding(segment());
        List<SegmentSink> withNull = new ArrayList<>();
        withNull.add(new SegmentProxyFixtures.RecordingSink());
        withNull.add(null);
        assertThatThrownBy(() -> new SegmentProxy(store).streamTo(KEY, withNull))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("caller error");
    }

    /** A chunk of nothing streams nothing, and says so at construction. */
    @Test
    void aNONPOSITIVEChunkIsREFUSED() {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        assertThatThrownBy(() -> new SegmentProxy(store, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("streams nothing");
        assertThatThrownBy(() -> new SegmentProxy(store, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentProxy(null))
                .isInstanceOf(NullPointerException.class).hasMessage("store");
    }

    /**
     * An empty fan-out still reads once, and serves nobody.
     *
     * <p>⚠️ RECORDED BECAUSE THE CHEAP OPTIMISATION IS WRONG IN ONE DIRECTION
     * ONLY. Returning early on an empty list would save a GET, but a caller
     * with no subscribers should not be calling this at all -- and a proxy
     * that skipped the read for zero consumers, then reused that branch for
     * "every consumer died", would stop reporting store failures.
     */
    @Test
    void anEMPTYFanOutStillREADSAndServesNobody() throws Exception {
        CountingBinStore store = storeHolding(segment());
        long before = store.counts().gets();
        assertThat(new SegmentProxy(store).streamTo(KEY, List.of())).isZero();
        // ⚠️ THE READ IS THE ASSERTION, not the zero. An early return on an
        // empty list also answers zero, so a count of nobody served does not
        // separate the two -- and that branch, reused for "every consumer
        // died", is what would stop store failures being reported.
        assertThat(store.counts().gets() - before)
                .as("the store is still read, so the failure path stays the same one")
                .isEqualTo(1);
    }

    /**
     * When EVERY consumer has died, a later store failure is still raised.
     *
     * <p>⚠️ ROUND-4 REVIEW MEASURED THIS UNCONSTRAINED: inserting
     * {@code if (live.isEmpty()) { break; }} after the hand-off loop passed all
     * eighteen tests, because no test ever let EVERY sink die. The mutant is
     * seductive -- there is nobody left to serve, so why keep reading? -- and
     * it silently converts a broken object into a clean return of zero.
     *
     * <p>⚠️ IT IS ALSO THE BRANCH {@code anEMPTYFanOutStillREADSAndServesNobody}
     * ARGUES ABOUT. That test justifies reading on an empty list by saying the
     * same early return, reused for "every consumer died", would stop store
     * failures being reported -- and until now nothing pinned the reused
     * branch. A justification whose subject has no test is an argument, not a
     * guarantee.
     *
     * <p>⚠️ AND IT IS THE CLASS ASYMMETRY AT ITS EDGE: "the store failing must
     * reach the caller as a throw and never as a count" has to hold when the
     * count would be zero, or a caller cannot tell a broken object from a
     * fan-out that evaporated.
     */
    @Test
    void aStoreFailureIsRaisedEVENWhenEveryConsumerHasAlreadyDied() throws Exception {
        byte[] expected = SegmentProxyFixtures.segment();
        long[] ignored = {0};
        SegmentProxyFixtures.StubStore breaks =
                new SegmentProxyFixtures.StubStore(expected, ignored, 0, 6);
        SegmentSink diesAtOnce = (buf, off, len) -> {
            throw new IOException("gone on the first chunk");
        };

        assertThatThrownBy(() -> new SegmentProxy(breaks)
                        .streamTo(KEY, List.of(diesAtOnce, diesAtOnce)))
                .as("nobody left to serve is not a reason to stop noticing the store is broken")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("mid-segment");
    }
}
