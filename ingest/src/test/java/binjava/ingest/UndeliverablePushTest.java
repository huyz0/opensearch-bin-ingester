// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static binjava.ingest.IngestTestSupport.appendOnce;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import binjava.sequencer.CommitRequest;
import binjava.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A push that could not be delivered is COUNTED, not silently dropped (M5.48).
 *
 * <p>⚠️ THE SILENCE THIS ENDS. {@code DefaultIngest.pushLoop}'s only handler was
 * {@code catch (RuntimeException e) { continue; }} — no log, no metric, no
 * counter — so a store outage and a quiet window were indistinguishable from
 * outside the process. {@code droppedPushes} existed and counted the OTHER drop
 * path, the queue budget, so a reader of that counter was told about
 * back-pressure and told nothing about a failed read.
 *
 * <p>⚠️ TWO COUNTERS, NOT ONE, because the drops have different answers: a
 * subscriber that cannot keep up means slow down or raise the budget; a
 * delivery that threw means look at the store.
 *
 * <p>⚠️ THE FIXTURE TURNS ON A KEY COMPARISON, NOT A SEGMENT COUNT, and two
 * earlier attempts at this row got that wrong and concluded the handler was
 * UNREACHABLE. {@code SubscriptionHub.publish} chooses held bytes by
 * {@code committed.segmentKey().equals(heldSegmentKey)}. A delta naming ONE
 * segment that is not the one this pod just wrote therefore takes
 * {@code held == null} → {@code PROXY} → {@code streamFromStore} → a
 * {@code get} for a key the store never had. A BATCHED delta would be the
 * obvious way to get a foreign key in, and it does not work:
 * {@code flushLocked} calls {@code delta.runs()}, which refuses any delta with
 * more than one segment — measured, with that stack.
 *
 * <p>⚠️ THE RUNS STAY REAL so the appends complete: only the segment key is
 * replaced, so {@code flushLocked}'s {@code firstOffsets} map still carries the
 * stream that was written.
 */
class UndeliverablePushTest {

    @Test
    void aPushThatCouldNotBeDeliveredIsCOUNTEDSeparatelyFromBackPressure() throws Exception {
        CountingBinStore store = anonymisingStore();
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new ArrayList<>();

        try (var ignored = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 3),
                        SubscriptionHub.assembling(seen::add));
                DefaultIngest ingest = ingestNamingAMissingSegment(store, hub)) {
            appendOnce(ingest, "logs", 3, 10);
            appendOnce(ingest, "logs", 3, 10);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (ingest.undeliverablePushes() < 2 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }

            // ⚠️ TWO, NOT ONE, because `isEqualTo(1)` over a single failure is
            // satisfied by `set(1)` as well as by an increment -- the M5.57
            // family, and review named it here too.
            assertThat(ingest.undeliverablePushes())
                    .as("each delivery that threw is counted, not merely the fact that one did")
                    .isEqualTo(2);
            assertThat(ingest.droppedPushes())
                    .as("and NOT as back-pressure -- the queue budget was never the reason, so a "
                            + "reader of that counter is not told a story about slow consumers")
                    .isZero();
            assertThat(seen)
                    .as("nobody is completed, so no subscriber mistakes a prefix for a segment")
                    .isEmpty();
        }
    }

    /**
     * The WARNING names the segment that could not be read, not the one written.
     *
     * ⚠️ WITHOUT THIS ASSERTION THE LINE WAS DELETABLE, and worse: review
     * MEASURED the first version naming {@code next.segmentKey()}, which is the
     * object this pod just PUT and the store therefore HOLDS. A read only ever
     * happens for a segment the pod does NOT hold, so the logged key was wrong
     * by construction -- an operator greps it, finds a healthy object, and
     * concludes nothing is wrong. The counter assertion could not see that.
     */
    @Test
    void theWarningNamesTheSegmentThatCouldNotBeREAD() throws Exception {
        CountingBinStore store = anonymisingStore();
        SubscriptionHub hub = new SubscriptionHub();
        List<java.util.logging.LogRecord> logged =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.logging.Logger root =
                java.util.logging.LogManager.getLogManager().getLogger("");
        java.util.logging.Level realLevel = root.getLevel();
        java.util.logging.Handler probe = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord record) {
                logged.add(record);
            }

            @Override public void flush() {
            }

            @Override public void close() {
            }
        };
        probe.setLevel(java.util.logging.Level.ALL);

        try (var ignored = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 3),
                        SubscriptionHub.assembling(p -> { }));
                DefaultIngest ingest = ingestNamingAMissingSegment(store, hub)) {
            root.addHandler(probe);
            // ⚠️ INFO, NOT ALL, AND THAT IS THE ASSERTION. Raising the root
            // to ALL would let a line demoted to TRACE reach the probe and the
            // test would stay green while an operator on a default configuration
            // saw nothing -- review MEASURED that mutation surviving. INFO is
            // what an unconfigured JVM runs at, so a demotion now reds here.
            root.setLevel(java.util.logging.Level.INFO);
            appendOnce(ingest, "logs", 3, 10);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            // ⚠️ WAIT FOR THE RECORD THE ASSERTION WANTS, not for any
            // record at all. The probe is on the ROOT logger, so every WARNING
            // in the JVM reaches it -- the lease renewer on its own timer
            // thread (`LocalSequencer`), the checkpoint writer, the batching
            // sequencer. A wait that ended at the FIRST record would run the
            // assertion against a list that does not yet hold the push line and
            // RED on a correct tree.
            //
            // ⚠️ NO TEST PINS THIS, AND THE ROW SAYS SO. The ordering
            // cannot be produced on demand: injecting a foreign WARNING ahead of
            // the push left the old wait green 10 times out of 10, because
            // `appendOnce` waits for durability first and that hands the push
            // thread a head start every time. So this is argued, not measured --
            // the deadline is now the only failure timer either way.
            while (!logged.stream().anyMatch(UnreadableSegmentWarning::names)
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        } finally {
            root.removeHandler(probe);
            root.setLevel(realLevel);
        }

        assertThat(logged)
                .as("the line must name the segment the store could not read, at WARNING%n%s",
                        logged.stream()
                                .map(r -> r.getLevel() + " " + r.getMessage())
                                .toList())
                .anyMatch(UnreadableSegmentWarning::names);
    }

    /**
     * The wait's question is not trivially YES, and not blind to the level.
     *
     * <p>⚠️ THIS IS THE RED THE ROW SAID DID NOT EXIST, and review found
     * it after measuring {@code return true;} -- which is exactly the
     * {@code logged.isEmpty()} wait this task replaces -- surviving the whole
     * suite. Asked of {@link UnreadableSegmentWarning} directly it needs no
     * fixture, no thread and no clock, and it reds on the first assertion.
     */
    @Test
    void theWaitsQuestionIsNotAlwaysYESAndNotBlindToTheLEVEL() {
        assertThat(UnreadableSegmentWarning.names(new java.util.logging.LogRecord(
                java.util.logging.Level.WARNING, "an unrelated warning from somewhere else")))
                .as("a foreign record must not end the wait -- `return true` is the old behaviour")
                .isFalse();
        assertThat(UnreadableSegmentWarning.names(new java.util.logging.LogRecord(
                java.util.logging.Level.WARNING,
                "segment " + UnreadableSegmentWarning.KEY + " could not be read for delivery")))
                .as("and the line the assertion wants must end it, or the wait always deadlines")
                .isTrue();
        assertThat(UnreadableSegmentWarning.names(new java.util.logging.LogRecord(
                java.util.logging.Level.INFO,
                "segment " + UnreadableSegmentWarning.KEY + " could not be read for delivery")))
                .as("the LEVEL half is pinned too: the same text below WARNING is not the line")
                .isFalse();
    }

    /**
     * A store whose failed read names no key, so only the code under test can.
     *
     * <p>⚠️ {@link binjava.binstore.backend.MemoryBinStore} alone cannot
     * carry this test. It throws {@code IOException("no such key: " + key)}, so
     * the key reaches the WARNING through the CAUSE whether or not
     * {@code SegmentServingPath.deliver} names it -- review measured the whole
     * {@code deliver} change reverting green against that fake. A real backend
     * answers with a reset connection and names no object.
     */
    private static CountingBinStore anonymisingStore() {
        return new CountingBinStore(
                new StoreFakes.ReadFailsWithoutNamingTheKey(new MemoryBinStore()));
    }

    /** An ingest whose sequencer renames the committed segment to one the store lacks. */
    private static DefaultIngest ingestNamingAMissingSegment(CountingBinStore store,
            SubscriptionHub hub) throws IOException {
        Sequencer real = IngestTestSupport.sequencer(store, "pod1");
        Sequencer renaming = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                CommitDelta delta = real.commitAll(requests);
                return new CommitDelta(delta.sequence(), UnreadableSegmentWarning.KEY,
                        delta.runs());
            }

            @Override
            public void close() throws IOException {
                real.close();
            }
        };
        return new DefaultIngest(IngestTestSupport.pinnedIntervalConfig(
                        IngestTestSupport.NEVER, 8L << 20),
                store, IngestTestSupport.PREFIX, "pod1", renaming, hub,
                Clock.systemUTC(), index -> IngestTestSupport.LOGS);
    }
}
