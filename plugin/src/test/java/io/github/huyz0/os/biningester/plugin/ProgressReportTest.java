// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.ConsumerProgress;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * What one node tells the ingester about where its shard copies have got to
 * (M7.3, FR-9, ADR-0049).
 *
 * <p>⚠️ THE COUNT AND THE COST ARE THE EASY HALF. A reporter that sends one
 * frame of eight entries at zero object-store requests is green on every
 * count-and-cost assertion while sending the stream's TAIL offset, or a
 * constant — and that drives the ingester's {@code min()} to the live edge
 * forever, so GC deletes records no shard has indexed. Every case here that can
 * assert CONTENTS does.
 */
class ProgressReportTest {

    private static final String LOGS = "nVzgup36TLqWp7VBBREj1w";
    private static final String METRICS = "8Gk1lQ2HRs-TvA4pZ0bXyQ";

    /** Records the frames a node pushed; refuses to be a store. */
    private static final class RecordingTransport implements SubscriptionTransport {
        final List<ConsumerProgress> frames = new ArrayList<>();
        final AtomicInteger failuresToInject = new AtomicInteger();

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void report(ConsumerProgress progress) {
            if (failuresToInject.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new IllegalStateException("the ingester is not reachable");
            }
            frames.add(progress);
        }

    }

    private static ProgressReporter.ShardPosition at(String uuid, int partition, String copy,
            long consumedUpTo) {
        return new ProgressReporter.ShardPosition(uuid, partition, copy, consumedUpTo);
    }

    @Test
    void ONEFrameCarriesEVERYPartitionOnTheNode() {
        RecordingTransport transport = new RecordingTransport();
        List<ProgressReporter.ShardPosition> local = List.of(
                at(LOGS, 0, "alloc-0", 10), at(LOGS, 1, "alloc-1", 11),
                at(LOGS, 2, "alloc-2", 12), at(LOGS, 3, "alloc-3", 13),
                at(METRICS, 0, "alloc-4", 14), at(METRICS, 1, "alloc-5", 15),
                at(METRICS, 2, "alloc-6", 16), at(METRICS, 3, "alloc-7", 17));
        new ProgressReporter(transport, () -> local).report();
        assertThat(transport.frames)
                .as("the reporting unit is the NODE: a frame per shard is eight times the "
                        + "channel for the same information, which M6.7 already got wrong "
                        + "once for registration")
                .hasSize(1);
        assertThat(transport.frames.get(0).entries())
                .as("eight DIFFERENT positions, so a reporter that sent one number eight "
                        + "times is not merely counted but read")
                .extracting(ConsumerProgress.Entry::consumedUpTo)
                .containsExactly(10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L);
    }

    @Test
    void EACHEntryCarriesTHATShardsOwnPosition() {
        RecordingTransport transport = new RecordingTransport();
        List<ProgressReporter.ShardPosition> local = List.of(
                at(LOGS, 0, "alloc-0", 41822), at(LOGS, 1, "alloc-1", 7),
                at(METRICS, 4, "alloc-2", 900001));
        new ProgressReporter(transport, () -> local).report();
        assertThat(transport.frames.get(0).entries())
                .as("⚠️ THE MUTATION THIS CASE EXISTS FOR: the stream's TAIL offset, or a "
                        + "constant, in place of the consumed pointer. One frame, three "
                        + "entries, zero requests -- green on every count-and-cost "
                        + "assertion, and the ingester's min() sits at the live edge "
                        + "forever while GC deletes records no shard has indexed")
                .containsExactly(
                        new ConsumerProgress.Entry(LOGS, 0, "alloc-0", 41822),
                        new ConsumerProgress.Entry(LOGS, 1, "alloc-1", 7),
                        new ConsumerProgress.Entry(METRICS, 4, "alloc-2", 900001));
    }

    @Test
    void aNodeHostingNOTHINGSendsNOFrame() {
        RecordingTransport transport = new RecordingTransport();
        ProgressReporter reporter = new ProgressReporter(transport, List::of);
        reporter.report();
        assertThat(transport.frames)
                .as("an EMPTY frame is refused by the format, because silence already "
                        + "means 'freeze this copy' -- a node with no shards says nothing "
                        + "rather than saying nothing loudly")
                .isEmpty();
        assertThat(reporter.pushFailures())
                .as("⚠️ AND IT IS NOT A FAILURE. Without the empty check the format's "
                        + "refusal is caught and counted, so every idle node logs a "
                        + "WARNING and climbs this counter once per interval forever -- "
                        + "and the number that is supposed to mean 'this transport cannot "
                        + "carry progress' becomes background noise")
                .isZero();
    }

    @Test
    void CONSTRUCTINGAReporterPushesNOTHING() {
        RecordingTransport transport = new RecordingTransport();
        new ProgressReporter(transport, () -> List.of(at(LOGS, 0, "alloc-0", 5)));
        assertThat(transport.frames)
                .as("the interval is the caller's, not a timer inside this class -- and a "
                        + "constructor that pushed would push on a node that is still "
                        + "being built")
                .isEmpty();
    }

    @Test
    void EVERYIntervalCarriesTheCURRENTPositions() {
        RecordingTransport transport = new RecordingTransport();
        List<ProgressReporter.ShardPosition> moving = new ArrayList<>();
        moving.add(at(LOGS, 0, "alloc-0", 5));
        ProgressReporter reporter = new ProgressReporter(transport, () -> List.copyOf(moving));
        reporter.report();
        moving.set(0, at(LOGS, 0, "alloc-0", 500));
        reporter.report();
        assertThat(transport.frames.stream()
                .map(f -> f.entries().get(0).consumedUpTo()).toList())
                .as("the feed is LEVEL-triggered, not edge-triggered: each interval reads "
                        + "the positions again rather than sending a remembered one")
                .containsExactly(5L, 500L);
    }

    @Test
    void aPositionThatWentBACKWARDSIsReportedAsIs() {
        RecordingTransport transport = new RecordingTransport();
        List<ProgressReporter.ShardPosition> moving = new ArrayList<>();
        moving.add(at(LOGS, 0, "alloc-0", 500));
        ProgressReporter reporter = new ProgressReporter(transport, () -> List.copyOf(moving));
        reporter.report();
        moving.set(0, at(LOGS, 0, "alloc-0", 120));
        reporter.report();
        assertThat(transport.frames.get(1).entries().get(0).consumedUpTo())
                .as("a restarted shard resumes from its last Lucene commit and reports "
                        + "LOWER. A reporter that held the maximum would tell the ingester "
                        + "the shard is further along than it is, and the records between "
                        + "the two positions would be deleted before it re-read them")
                .isEqualTo(120);
    }

    @Test
    void aFAILEDPushDoesNotPROPAGATEAndIsCOUNTED() {
        RecordingTransport transport = new RecordingTransport();
        transport.failuresToInject.set(1);
        ProgressReporter reporter = new ProgressReporter(transport,
                () -> List.of(at(LOGS, 0, "alloc-0", 5)));
        assertThatCode(reporter::report)
                .as("this runs on a shared pool inside the node; a throw out of it would "
                        + "kill whatever schedules it, and the watermark would stop for "
                        + "every stream on the node rather than for one interval")
                .doesNotThrowAnyException();
        assertThat(reporter.pushFailures()).isEqualTo(1);
        assertThat(transport.frames).isEmpty();
    }

    @Test
    void aFAILEDIntervalIsNotRETRIEDBecauseTheNEXTOneCarriesMore() {
        RecordingTransport transport = new RecordingTransport();
        transport.failuresToInject.set(1);
        List<ProgressReporter.ShardPosition> moving = new ArrayList<>();
        moving.add(at(LOGS, 0, "alloc-0", 5));
        ProgressReporter reporter = new ProgressReporter(transport, () -> List.copyOf(moving));
        reporter.report();
        moving.set(0, at(LOGS, 0, "alloc-0", 900));
        reporter.report();
        assertThat(transport.frames)
                .as("⚠️ WHY THIS DIFFERS FROM REGISTRATION, WHICH RETRIES THREE TIMES: a "
                        + "registration is EDGE-triggered and a lost push is lost until "
                        + "the next cluster-state change, which may be hours. Progress is "
                        + "LEVEL-triggered -- the next interval carries a position that "
                        + "SUPERSEDES the lost one, so a retry would re-send a stale "
                        + "number. A missed interval costs freshness, and unfresh means "
                        + "GC keeps")
                .singleElement()
                .satisfies(f -> assertThat(f.entries().get(0).consumedUpTo()).isEqualTo(900));
        assertThat(reporter.pushes()).isEqualTo(1);
    }

    @Test
    void POSITIONSThatCannotBeFramedAreCOUNTEDNotThrown() {
        RecordingTransport transport = new RecordingTransport();
        ProgressReporter reporter = new ProgressReporter(transport, () -> List.of(
                at(LOGS, 0, "alloc-a", 5), at(LOGS, 0, "alloc-a", 9)));
        assertThatCode(reporter::report)
                .as("one copy reported twice for one stream is a shape the format refuses, "
                        + "and a real node reaches it while a shard relocates. Rethrowing "
                        + "cancels the schedule and stops progress for every stream here")
                .doesNotThrowAnyException();
        assertThat(reporter.pushFailures()).isEqualTo(1);
        assertThat(transport.frames).isEmpty();
    }

    @Test
    void aNULLPositionInTheListIsCOUNTEDNotThrown() {
        RecordingTransport transport = new RecordingTransport();
        List<ProgressReporter.ShardPosition> withNull = new ArrayList<>();
        withNull.add(at(LOGS, 0, "alloc-a", 5));
        withNull.add(null);
        ProgressReporter reporter = new ProgressReporter(transport, () -> withNull);
        assertThatCode(reporter::report)
                .as("a null element arrives as an NPE from the mapping lambda rather than "
                        + "as the record's IAE, so a catch narrowed to IllegalArgumentException "
                        + "lets it out -- and out means the schedule is cancelled")
                .doesNotThrowAnyException();
        assertThat(reporter.pushFailures()).isEqualTo(1);
    }

    @Test
    void aPOSITIONSourceThatTHROWSIsCOUNTEDNotPropagated() {
        RecordingTransport transport = new RecordingTransport();
        ProgressReporter reporter = new ProgressReporter(transport, () -> {
            throw new IllegalStateException("shard is closing");
        });
        assertThatCode(reporter::report)
                .as("⚠️ THE HALF MOST LIKELY TO THROW: the production source is the node's "
                        + "own ingestion state (M7.17), and every handle onto it throws "
                        + "unchecked when a shard is closing, relocating or gone. A throw "
                        + "out of a scheduled task cancels the schedule SILENTLY, so "
                        + "progress would stop for every stream on this node permanently "
                        + "while pushFailures() never moved")
                .doesNotThrowAnyException();
        assertThat(reporter.pushFailures()).isEqualTo(1);
    }

    @Test
    void theReporterHoldsNOClockNOThreadNOTimerAndNOStore() {
        for (var field : ProgressReporter.class.getDeclaredFields()) {
            assertThat(field.getType().getName())
                    .as("field %s: this class holds a transport and a source of positions "
                            + "and nothing else -- a clock or an executor here is a timer, "
                            + "and a timer is what makes an idle node cost something",
                            field.getName())
                    .doesNotContain("java.time")
                    .doesNotContain("java.lang.Thread")
                    .doesNotContain("java.util.Timer")
                    .doesNotContain("concurrent.Executor")
                    .doesNotContain("concurrent.Scheduled")
                    .doesNotContain("io.github.huyz0.os.biningester.binstore");
        }
    }

    @Test
    void aTransportThatCannotCarryProgressREFUSESRatherThanSwallowing() {
        SubscriptionTransport bare = (key, listener) -> () -> { };
        ProgressReporter reporter = new ProgressReporter(bare,
                () -> List.of(at(LOGS, 0, "alloc-0", 5)));
        reporter.report();
        assertThat(reporter.pushFailures())
                .as("the default `report` refuses, so a deployment whose transport cannot "
                        + "carry progress counts failures loudly instead of reporting "
                        + "success -- a silent no-op here pins retention at maxRetention "
                        + "and the first anyone hears of it is the ceiling alarm")
                .isEqualTo(1);
    }
}
