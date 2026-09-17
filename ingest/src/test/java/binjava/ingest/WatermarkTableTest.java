// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.ConsumerProgress;
import binjava.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Where every known copy of a stream has got to (M7.2, FR-9, ADR-0005).
 *
 * <p>⚠️ EVERY CASE HERE IS A DELETE-OR-KEEP DECISION ONE LEVEL DOWN. A table
 * that answers too HIGH deletes data a copy has not read; one that answers too
 * LOW keeps data forever. Research 09 §6.2-6.4 lists three ways the high answer
 * arrives — a {@code min()} over the copies that happen to be reporting, a copy
 * retired because a new allocation id appeared, and a per-copy {@code max()}
 * over a position that legitimately regresses — and each has a case below.
 */
class WatermarkTableTest {

    private static final String LOGS = "nVzgup36TLqWp7VBBREj1w";
    private static final RunKey STREAM = RunKey.ofIndexUuid(LOGS, 4);

    private static final Duration REPORT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration MIN_RETENTION = Duration.ofHours(6);
    private static final Duration COPY_EXPIRY = Duration.ofHours(12);

    /** A clock the test moves by hand; nothing here reads the wall clock. */
    private static final class TestClock extends Clock {
        private Instant now = Instant.parse("2026-09-17T00:00:00Z");

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    private final TestClock clock = new TestClock();

    private WatermarkTable table() {
        return new WatermarkTable(clock, REPORT_TIMEOUT, COPY_EXPIRY, MIN_RETENTION);
    }

    private static ConsumerProgress frame(int partition, String copy, long at) {
        return new ConsumerProgress(
                List.of(new ConsumerProgress.Entry(LOGS, partition, copy, at)));
    }

    @Test
    void theMINIMUMAcrossCopiesIsTheAnswer() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.observe(frame(4, "alloc-b", 90));
        table.observe(frame(4, "alloc-c", 300));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("every copy consumes the partition independently, so the one that has "
                        + "read LEAST is the one whose data must be kept")
                .isEqualTo(90);
    }

    @Test
    void aDEREGISTEREDCopyLeavesTheOthersBehind() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.observe(frame(4, "alloc-b", 90));
        table.deregister(STREAM, "alloc-b");
        assertThat(table.of(STREAM).consumedUpTo())
                .as("an EXPLICIT deregistration is the one thing that removes a copy "
                        + "promptly -- the shard is gone, not quiet")
                .isEqualTo(100);
    }

    @Test
    void aSILENTCopyFREEZESTheMinimumRatherThanLeavingIt() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.observe(frame(4, "alloc-b", 90));
        table.observe(frame(4, "alloc-c", 300));
        clock.advance(Duration.ofSeconds(10));
        table.observe(frame(4, "alloc-a", 500));
        table.observe(frame(4, "alloc-c", 500));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("⚠️ THE MUTATION THIS CASE EXISTS FOR: a min() over the copies that "
                        + "REPORTED answers 500, and the node that was down for ten "
                        + "minutes comes back to deleted data. Silence freezes; it does "
                        + "not drop")
                .isEqualTo(90);
    }

    @Test
    void aCopyThatREPORTSBackwardsIsBELIEVED() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.observe(frame(4, "alloc-b", 300));
        clock.advance(Duration.ofSeconds(1));
        table.observe(frame(4, "alloc-b", 150));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("a restart resumes from the last LUCENE COMMIT, so the position "
                        + "legitimately regresses and the later frame is the true one")
                .isEqualTo(100);
    }

    @Test
    void aREGRESSINGCopyHoldingTheMINIMUMDragsTheAnswerDOWN() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 90));
        table.observe(frame(4, "alloc-b", 300));
        clock.advance(Duration.ofSeconds(1));
        table.observe(frame(4, "alloc-a", 40));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("⚠️ THE CASE THAT SEPARATES A PER-COPY max() FROM LATEST-WINS: with "
                        + "the regressing copy holding the minimum, a max() answers 90 and "
                        + "deletes exactly the records the restarted shard re-reads")
                .isEqualTo(40);
    }

    @Test
    void oneSILENTCopyMakesTheTableUNFRESHWhileTheOthersReport() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.observe(frame(4, "alloc-b", 90));
        table.observe(frame(4, "alloc-c", 300));
        clock.advance(REPORT_TIMEOUT.plusSeconds(1));
        table.observe(frame(4, "alloc-a", 500));
        table.observe(frame(4, "alloc-c", 500));
        assertThat(table.of(STREAM).fresh())
                .as("⚠️ FRESHNESS IS THE OLDEST REPORT, NOT THE NEWEST, and the mix is "
                        + "the fixture: two copies reported in this interval, so a "
                        + "freshness taken from the newest says fresh and GC deletes "
                        + "against a min() nobody has confirmed")
                .isFalse();
    }

    @Test
    void aTableWhoseEveryCopyReportedIsFRESH() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.observe(frame(4, "alloc-b", 90));
        clock.advance(REPORT_TIMEOUT.minusSeconds(1));
        assertThat(table.of(STREAM).fresh())
                .as("the control: inside the timeout the min() is trustworthy, and a "
                        + "table that is never fresh keeps everything forever")
                .isTrue();
    }

    @Test
    void aNEWAllocationIdDoesNOTRetireTheOldOne() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-old", 90));
        clock.advance(Duration.ofSeconds(1));
        table.observe(frame(4, "alloc-new", 500));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("a relocation produces a new allocation id reporting for the same "
                        + "partition while the old goes quiet -- and BOTH legitimately "
                        + "exist mid-relocation, so the new one arriving retires nothing")
                .isEqualTo(90);
    }

    @Test
    void aCopySilentPastTheEXPIRYIsRETIRED() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-gone", 90));
        table.observe(frame(4, "alloc-live", 500));
        clock.advance(COPY_EXPIRY.plusSeconds(1));
        table.observe(frame(4, "alloc-live", 900));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("⚠️ THE EXPIRY MUST ACTUALLY FIRE: a table that only ever refuses to "
                        + "retire pins retention on the first relocation and never "
                        + "releases it. The expiry is longer than minRetention, so by the "
                        + "time it fires the data it was protecting is deletable anyway")
                .isEqualTo(900);
    }

    @Test
    void anUNKNOWNStreamIsUNKNOWNRatherThanZeroOrMAX() {
        WatermarkTable table = table();
        WatermarkTable.Watermark unknown = table.of(RunKey.ofIndexUuid(LOGS, 9));
        assertThat(unknown.known())
                .as("⚠️ NOBODY HAS REPORTED IS NOT THE SAME AS NOBODY HAS READ. Answering "
                        + "0 keeps everything, which is safe but indistinguishable from a "
                        + "real zero; answering Long.MAX_VALUE deletes the whole stream. "
                        + "The caller must be able to tell")
                .isFalse();
        assertThatThrownBy(unknown::consumedUpTo)
                .as("the position of a stream nobody has reported on cannot be read at "
                        + "all -- a default here is the default that deletes")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void anUNKNOWNStreamIsNEVERFresh() {
        assertThat(table().of(RunKey.ofIndexUuid(LOGS, 9)).fresh())
                .as("freshness over no copies is vacuously true, and a vacuous true here "
                        + "is a table that says 'every copy reported' when none did")
                .isFalse();
    }

    @Test
    void theEXCLUSIVEPositionIsCarriedThroughUNCHANGED() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 41822));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("consumedUpTo is the offset the copy would RESUME FROM, and the table "
                        + "neither adds nor subtracts one -- an off-by-one here is an "
                        + "off-by-one in the DELETING direction on every stream")
                .isEqualTo(41822);
    }

    @Test
    void aWatermarkThatIsUNKNOWNAndFRESHCannotBeBuilt() {
        assertThatThrownBy(() -> new WatermarkTable.Watermark(false, true, 0))
                .as("freshness over no copies is vacuously true, and a vacuous true here "
                        + "tells GC that every copy reported when none did")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anUNKNOWNWatermarkHasNoReadablePositionAtAll() {
        assertThat(WatermarkTable.Watermark.class.getMethods())
                .as("⚠️ THE SENTINEL MUST NOT BE READABLE. A record would expose the "
                        + "backing field as position(), and whatever an unknown watermark "
                        + "held would become usable: 0 is indistinguishable from a real "
                        + "zero, and Long.MAX_VALUE deletes the whole stream. consumedUpTo "
                        + "is the only way in, and it refuses")
                .noneMatch(m -> m.getName().equals("position"));
    }

    @Test
    void aCopySilentForEXACTLYTheExpiryIsSTILLKnown() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-gone", 90));
        table.observe(frame(4, "alloc-live", 500));
        clock.advance(COPY_EXPIRY);
        table.observe(frame(4, "alloc-live", 900));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("the boundary itself: retiring on >= drops a copy one tick early, and "
                        + "one tick early is still data deleted for a copy entitled to it")
                .isEqualTo(90);
    }

    @Test
    void aCopySilentForEXACTLYTheReportTimeoutIsSTILLFresh() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        clock.advance(REPORT_TIMEOUT);
        assertThat(table.of(STREAM).fresh())
                .as("the other boundary: a timeout that fires AT the limit rather than "
                        + "PAST it stalls GC on a perfectly healthy cluster, which is the "
                        + "failure the ceiling exists to make loud rather than the one it "
                        + "exists to prevent")
                .isTrue();
    }

    @Test
    void aFrameCarriesEveryStreamOnTheNodeAtOnce() {
        WatermarkTable table = table();
        table.observe(new ConsumerProgress(List.of(
                new ConsumerProgress.Entry(LOGS, 4, "alloc-a", 90),
                new ConsumerProgress.Entry(LOGS, 5, "alloc-a", 7))));
        assertThat(table.of(STREAM).consumedUpTo()).isEqualTo(90);
        assertThat(table.of(RunKey.ofIndexUuid(LOGS, 5)).consumedUpTo())
                .as("one frame, every partition the node hosts -- and the same copy id on "
                        + "two partitions is two independent positions")
                .isEqualTo(7);
    }

    @Test
    void TWOStreamsDoNotShareAWatermark() {
        String metrics = "8Gk1lQ2HRs-TvA4pZ0bXyQ";
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 900));
        table.observe(new ConsumerProgress(
                List.of(new ConsumerProgress.Entry(metrics, 4, "alloc-a", 5))));
        assertThat(table.of(STREAM).consumedUpTo())
                .as("the same partition number in two indices is two streams, and "
                        + "collapsing them deletes the busier one's data")
                .isEqualTo(900);
    }

    @Test
    void anEXPIRYNoLongerThanTheRETENTIONFloorIsREFUSED() {
        assertThatThrownBy(() -> new WatermarkTable(clock, REPORT_TIMEOUT, MIN_RETENTION,
                MIN_RETENTION))
                .as("a copy retired inside the retention window is a copy whose data is "
                        + "deleted while it is still entitled to read it -- the "
                        + "configuration that quietly turns the outage budget off")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minRetention");
    }

    @Test
    void anEXPIRYShorterThanTheREPORTTimeoutIsREFUSED() {
        assertThatThrownBy(() -> new WatermarkTable(clock, Duration.ofHours(24), COPY_EXPIRY,
                MIN_RETENTION))
                .as("a copy that expires before it can be called stale can never make the "
                        + "table unfresh -- the freshness brake would be unreachable")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reportTimeout");
    }

    @Test
    void aNONPOSITIVETimeoutOrExpiryIsREFUSED() {
        assertThatThrownBy(() -> new WatermarkTable(clock, Duration.ZERO, COPY_EXPIRY,
                MIN_RETENTION)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WatermarkTable(clock, REPORT_TIMEOUT, COPY_EXPIRY,
                Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anINDEXUuidThatIsNotAnIndexUuidIsREFUSED() {
        WatermarkTable table = table();
        assertThatThrownBy(() -> table.observe(new ConsumerProgress(
                List.of(new ConsumerProgress.Entry("not-base64url-16", 4, "alloc-a", 5)))))
                .as("the frame carries OpenSearch's BASE64URL uuid and the stream is keyed "
                        + "by the 16 bytes it decodes to -- a frame that cannot be joined "
                        + "to a stream must say so rather than be dropped, because a "
                        + "dropped frame is a copy that goes silent for no reason")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deregisteringACopyNobodyReportedIsNotAnError() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.deregister(STREAM, "alloc-never-seen");
        assertThat(table.of(STREAM).consumedUpTo())
                .as("deregistration arrives from a cluster-state change and may race the "
                        + "first report; throwing would turn an ordinary race into a "
                        + "failed applier")
                .isEqualTo(100);
    }

    @Test
    void deregisteringTheLASTCopyMakesTheStreamUNKNOWNAgain() {
        WatermarkTable table = table();
        table.observe(frame(4, "alloc-a", 100));
        table.deregister(STREAM, "alloc-a");
        assertThat(table.of(STREAM).known())
                .as("no copy is not a watermark of zero, and not one of MAX either: the "
                        + "stream is unknown, and GC keeps")
                .isFalse();
    }
}
