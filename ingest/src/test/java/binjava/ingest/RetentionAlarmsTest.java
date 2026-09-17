// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What an operator sees while retention is working (M7.13, FR-9, research 09
 * §9).
 *
 * <p>⚠️ THE ONE TILE WORTH A DASHBOARD IS THE OUTAGE BUDGET:
 * {@code minRetention − age(oldest unread data)}, which says how long the
 * cluster can stay broken before data is lost. ⚠️ AND IT MUST COUNT DOWN. A
 * gauge returning a constant is exported, carries no label and satisfies every
 * alarm case — while the tile reads a healthy six hours with the budget minutes
 * from zero — and one computing {@code minRetention + age} counts UP as the
 * budget runs out, which passes a difference-only assertion.
 *
 * <p>⚠️ NO PER-STREAM, PER-INDEX OR PER-COPY LABEL. observability.md rule 1
 * closes the allow-list because a {@code stream} label costs ~2,400,000 series;
 * identities go to a bounded top-K event instead (rule 2), which is the shape
 * M4.14 already established.
 */
class RetentionAlarmsTest {

    private static final UUID INDEX = new UUID(0x1111_2222_3333_4444L, 1);
    private static final RunKey STREAM = new RunKey(INDEX, 0);
    private static final RunKey OTHER = new RunKey(INDEX, 1);

    private static final Duration MIN_RETENTION = Duration.ofHours(6);
    private static final Duration MAX_RETENTION = Duration.ofHours(24);
    private static final Duration REPORT_TIMEOUT = Duration.ofSeconds(30);

    private static final class TestClock extends Clock {
        private Instant now = Instant.parse("2026-09-17T12:00:00Z");

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    private final TestClock clock = new TestClock();
    private final List<RetentionObservable.Alarm> raised = new ArrayList<>();

    private RetentionObservable observable() {
        return new RetentionObservable(clock, MIN_RETENTION, MAX_RETENTION, REPORT_TIMEOUT,
                raised::add);
    }

    @Test
    void theOUTAGEBudgetCOUNTSDOWNAsTheOldestUnreadDataAges() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM, clock.instant().minus(Duration.ofHours(1)));
        Duration first = observable.outageBudget();
        clock.advance(Duration.ofHours(2));
        Duration later = observable.outageBudget();
        assertThat(first).isEqualTo(Duration.ofHours(5));
        assertThat(later)
                .as("⚠️ THE LATER READING IS SMALLER BY EXACTLY THE ELAPSED TIME. A "
                        + "constant gauge is exported, unlabelled and green on every alarm "
                        + "case while the tile reads a healthy six hours with the budget "
                        + "minutes from zero; and `minRetention + age` counts UP as it "
                        + "runs out, which a difference-only assertion cannot tell apart")
                .isEqualTo(Duration.ofHours(3));
    }

    @Test
    void theBudgetIsTheWORSTStreamsNotTheBest() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM, clock.instant().minus(Duration.ofMinutes(5)));
        observable.observeOldestUnread(OTHER, clock.instant().minus(Duration.ofHours(5)));
        assertThat(observable.outageBudget())
                .as("one stream an hour from losing data is the cluster's budget, and an "
                        + "average over streams would read comfortable while that one "
                        + "stream is about to lose records")
                .isEqualTo(Duration.ofHours(1));
    }

    @Test
    void theBudgetGoesNEGATIVERatherThanClampingAtZero() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM, clock.instant().minus(Duration.ofHours(7)));
        assertThat(observable.outageBudget())
                .as("a budget clamped at zero says 'out of time' for an hour over and for "
                        + "a day over alike -- and the difference is how much data an "
                        + "operator is about to lose")
                .isEqualTo(Duration.ofHours(-1));
    }

    @Test
    void aStreamNOBODYHasReportedOnDoesNOTSetTheBudget() {
        RetentionObservable observable = observable();
        assertThat(observable.outageBudget())
                .as("nothing is known, so nothing is claimed -- a budget of zero over an "
                        + "empty observable would page an operator about a cluster that "
                        + "has not started")
                .isNull();
    }

    @Test
    void aCopySILENTPastTheTimeoutALARMS() {
        RetentionObservable observable = observable();
        observable.observeReport(STREAM, "alloc-a");
        clock.advance(REPORT_TIMEOUT.plusSeconds(1));
        observable.sweep();
        assertThat(raised)
                .as("a silent copy freezes the watermark, so storage grows and nobody "
                        + "knows why until the ceiling fires hours later")
                .extracting(RetentionObservable.Alarm::kind)
                .containsExactly(RetentionObservable.Kind.COPY_SILENT);
    }

    @Test
    void ONESilentCopyALARMSWhileITSPeersKeepReporting() {
        RetentionObservable observable = observable();
        observable.observeReport(STREAM, "alloc-a");
        observable.observeReport(STREAM, "alloc-b");
        observable.observeReport(STREAM, "alloc-c");
        clock.advance(REPORT_TIMEOUT.plusSeconds(1));
        observable.observeReport(STREAM, "alloc-a");
        observable.observeReport(STREAM, "alloc-c");
        observable.sweep();
        assertThat(raised)
                .as("⚠️ ANY COPY, NOT EVERY COPY -- and review MEASURED the difference: "
                        + "keyed by STREAM, the two healthy copies keep the entry fresh "
                        + "and nothing alarms, while `WatermarkTable` takes min() across "
                        + "ALL known copies and freshness from the OLDEST report, so this "
                        + "stream's watermark IS frozen at alloc-b's position")
                .extracting(RetentionObservable.Alarm::kind)
                .containsExactly(RetentionObservable.Kind.COPY_SILENT);
        assertThat(raised.get(0).shardCopy())
                .as("and it names WHICH copy, or an operator has three shards to look at")
                .isEqualTo("alloc-b");
    }

    @Test
    void aCopyEXACTLYAtTheTimeoutIsNOTSilentYet() {
        RetentionObservable observable = observable();
        observable.observeReport(STREAM, "alloc-a");
        clock.advance(REPORT_TIMEOUT);
        observable.sweep();
        assertThat(raised)
                .as("a copy reporting on an exactly-regular cadence is healthy, and an "
                        + "alarm AT the boundary pages on every one of them")
                .isEmpty();
    }

    @Test
    void aDEREGISTEREDCopyIsFORGOTTENRatherThanAlarmingForever() {
        RetentionObservable observable = observable();
        observable.observeReport(STREAM, "alloc-gone");
        observable.deregisterCopy(STREAM, "alloc-gone");
        clock.advance(REPORT_TIMEOUT.plusSeconds(1));
        observable.sweep();
        assertThat(raised)
                .as("⚠️ NOTHING IS EVER FORGOTTEN OTHERWISE: an entry for a relocated copy "
                        + "or a deleted index alarms for ever, and under the "
                        + "de-duplication rule that condition can then never re-raise for "
                        + "a real incident")
                .isEmpty();
    }

    @Test
    void aStreamWithNOTHINGUnreadStopsSettingTheBudget() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM, clock.instant().minus(Duration.ofHours(5)));
        observable.observeOldestUnread(OTHER, clock.instant().minus(Duration.ofMinutes(5)));
        observable.observeNothingUnread(STREAM);
        assertThat(observable.outageBudget())
                .as("⚠️ THE BUDGET IS THE WORST STREAM'S, so one entry left behind for a "
                        + "consumed or deleted stream counts the whole cluster down past "
                        + "zero while nothing is at risk -- and an operator who sees that "
                        + "every day stops believing the tile")
                .isEqualTo(Duration.ofHours(6).minus(Duration.ofMinutes(5)));
    }

    @Test
    void aCopyREPORTINGDoesNOTAlarm() {
        RetentionObservable observable = observable();
        observable.observeReport(STREAM, "alloc-a");
        clock.advance(REPORT_TIMEOUT.minusSeconds(1));
        observable.sweep();
        assertThat(raised)
                .as("an alarm that fires on a healthy cluster is one nobody reads")
                .isEmpty();
    }

    @Test
    void dataAPPROACHINGTheCeilingALARMSBeforeItIsDeleted() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM,
                clock.instant().minus(MAX_RETENTION.minus(Duration.ofMinutes(30))));
        observable.sweep();
        assertThat(raised)
                .as("⚠️ BEFORE, NOT AFTER. An alarm that fires when the ceiling deletes "
                        + "tells an operator about data that is already gone; this one "
                        + "fires while there is still time to fix the consumer")
                .extracting(RetentionObservable.Alarm::kind)
                .containsExactly(RetentionObservable.Kind.APPROACHING_CEILING);
    }

    @Test
    void dataCOMFORTABLYInsideTheCeilingDoesNOTAlarm() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM, clock.instant().minus(Duration.ofHours(2)));
        observable.sweep();
        assertThat(raised).isEmpty();
    }

    @Test
    void aPAUSEDStreamOlderThanTheFloorALARMS() {
        RetentionObservable observable = observable();
        observable.observePaused(STREAM, clock.instant().minus(Duration.ofHours(7)));
        observable.sweep();
        assertThat(raised)
                .as("a paused shard reports a frozen pointer indefinitely and pins "
                        + "retention forever (research 09 §6.5) -- it is the one condition "
                        + "that guarantees the ceiling will fire")
                .extracting(RetentionObservable.Alarm::kind)
                .containsExactly(RetentionObservable.Kind.PAUSED_PAST_FLOOR);
    }

    @Test
    void aPAUSEDStreamINSIDETheFloorDoesNOTAlarm() {
        RetentionObservable observable = observable();
        observable.observePaused(STREAM, clock.instant().minus(Duration.ofMinutes(30)));
        observable.sweep();
        assertThat(raised)
                .as("pausing a shard for a minute is an ordinary operation, and an alarm "
                        + "on it would fire during every rolling restart")
                .isEmpty();
    }

    @Test
    void EACHAlarmIsRaisedINDEPENDENTLY() {
        RetentionObservable observable = observable();
        observable.observeReport(STREAM, "alloc-a");
        observable.observePaused(OTHER, clock.instant().minus(Duration.ofHours(7)));
        observable.observeOldestUnread(OTHER,
                clock.instant().minus(MAX_RETENTION.minus(Duration.ofMinutes(30))));
        clock.advance(REPORT_TIMEOUT.plusSeconds(1));
        observable.sweep();
        assertThat(raised)
                .as("⚠️ ONE EMITTER NO-OPPED AT A TIME IS THE MUTATION THIS CASE EXISTS "
                        + "FOR: 'the alarms exist' is green against two dead emitters and "
                        + "one live one")
                .extracting(RetentionObservable.Alarm::kind)
                .containsExactlyInAnyOrder(RetentionObservable.Kind.COPY_SILENT,
                        RetentionObservable.Kind.PAUSED_PAST_FLOOR,
                        RetentionObservable.Kind.APPROACHING_CEILING);
    }

    @Test
    void anALARMCarriesNOStreamIdentityInITSLabels() {
        RetentionObservable observable = observable();
        observable.observePaused(STREAM, clock.instant().minus(Duration.ofHours(7)));
        observable.sweep();
        assertThat(raised.get(0).labels())
                .as("⚠️ observability.md rule 1: a `stream` label costs ~2,400,000 series "
                        + "and ~5.7 GiB of TSDB memory, which would make our own telemetry "
                        + "heavier than the traffic it describes. The identity rides the "
                        + "EVENT -- rule 2 -- and the labels carry only the KIND")
                .containsOnlyKeys("kind");
        assertThat(raised.get(0).stream())
                .as("and the identity IS in the event, or an operator cannot act on it")
                .isEqualTo(STREAM);
    }

    @Test
    void theTOPKIsBOUNDEDHoweverManyStreamsThereAre() {
        RetentionObservable observable = observable();
        for (int i = 0; i < 500; i++) {
            observable.observeOldestUnread(new RunKey(INDEX, i),
                    clock.instant().minus(Duration.ofMinutes(i + 1)));
        }
        assertThat(observable.oldestUnreadTopK(5))
                .as("K names however many streams exist, which is what keeps attribution "
                        + "affordable -- and they are the WORST K, because the best five of "
                        + "500 tell an operator nothing")
                .hasSize(5)
                .first()
                .satisfies(entry -> assertThat(entry.getKey())
                        .isEqualTo(new RunKey(INDEX, 499)));
    }

    @Test
    void aNONPOSITIVERetentionIsREFUSED() {
        assertThatThrownBy(() -> new RetentionObservable(clock, Duration.ZERO, MAX_RETENTION,
                REPORT_TIMEOUT, raised::add)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetentionObservable(clock, MIN_RETENTION, Duration.ZERO,
                REPORT_TIMEOUT, raised::add)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetentionObservable(clock, MIN_RETENTION, MAX_RETENTION,
                Duration.ZERO, raised::add)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aCEILINGNoLongerThanTheFLOORIsREFUSED() {
        assertThatThrownBy(() -> new RetentionObservable(clock, MIN_RETENTION, MIN_RETENTION,
                REPORT_TIMEOUT, raised::add))
                .as("with the ceiling at the floor every stream past the floor is also "
                        + "past the ceiling, so the APPROACHING alarm fires for every "
                        + "stream in an ordinary cluster and means nothing")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNONPOSITIVETopKIsREFUSED() {
        assertThatThrownBy(() -> observable().oldestUnreadTopK(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theSAMEConditionDoesNOTAlarmTWICEInARow() {
        RetentionObservable observable = observable();
        observable.observePaused(STREAM, clock.instant().minus(Duration.ofHours(7)));
        observable.sweep();
        observable.sweep();
        assertThat(raised)
                .as("an alarm re-raised every sweep is a page every interval for one "
                        + "condition, and an operator who silences it stops seeing the "
                        + "next one")
                .hasSize(1);
    }

    @Test
    void aConditionThatCLEARSCanALARMAgainLater() {
        RetentionObservable observable = observable();
        observable.observePaused(STREAM, clock.instant().minus(Duration.ofHours(7)));
        observable.sweep();
        observable.observeResumed(STREAM);
        observable.sweep();
        observable.observePaused(STREAM, clock.instant().minus(Duration.ofHours(7)));
        observable.sweep();
        assertThat(raised)
                .as("de-duplication must not become suppression: the second incident is a "
                        + "second incident")
                .hasSize(2);
    }

    @Test
    void aConditionTheSWEEPSeesCLEARCanALARMAgain() {
        RetentionObservable observable = observable();
        observable.observeReport(STREAM, "alloc-a");
        clock.advance(REPORT_TIMEOUT.plusSeconds(1));
        observable.sweep();
        // The copy comes back, and the SWEEP is what notices -- not an
        // explicit clearing call.
        observable.observeReport(STREAM, "alloc-a");
        observable.sweep();
        clock.advance(REPORT_TIMEOUT.plusSeconds(1));
        observable.sweep();
        assertThat(raised)
                .as("⚠️ THE ONLY CLEARING PATH COPY_SILENT AND APPROACHING_CEILING HAVE IS "
                        + "THE SWEEP ITSELF, and review MEASURED it unpinned: the "
                        + "resume-based case clears the slot through its own call, so "
                        + "deleting the `raised.remove` from the not-holding branch left "
                        + "the suite green. A copy that wedges, is restarted and wedges "
                        + "again would never alarm a second time -- de-duplication become "
                        + "suppression")
                .hasSize(2);
    }

    @Test
    void aCEILINGConditionTheSWEEPSeesCLEARCanALARMAgain() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM,
                clock.instant().minus(MAX_RETENTION.minus(Duration.ofMinutes(30))));
        observable.sweep();
        // The consumer catches up a little, then falls behind again.
        observable.observeOldestUnread(STREAM, clock.instant().minus(Duration.ofHours(1)));
        observable.sweep();
        observable.observeOldestUnread(STREAM,
                clock.instant().minus(MAX_RETENTION.minus(Duration.ofMinutes(30))));
        observable.sweep();
        assertThat(raised).hasSize(2);
    }

    @Test
    void aDELETEDStreamThatWasPAUSEDIsFORGOTTENToo() {
        RetentionObservable observable = observable();
        observable.observePaused(STREAM, clock.instant().minus(Duration.ofHours(7)));
        observable.observeNothingUnread(STREAM);
        observable.sweep();
        assertThat(raised)
                .as("⚠️ `observeNothingUnread` MEANS 'consumed, OR THE STREAM IS GONE', and "
                        + "a deleted index that was paused would otherwise keep its entry "
                        + "and its de-duplication slot for ever -- per-index unbounded "
                        + "growth behind a condition that can never clear")
                .isEmpty();
    }

    @Test
    void dataJUSTOutsideTheWarningWindowDoesNOTAlarm() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM,
                clock.instant().minus(MAX_RETENTION.minus(Duration.ofHours(2))));
        observable.sweep();
        assertThat(raised)
                .as("the warning window is an HOUR before the ceiling, and a window "
                        + "widened to ten would alarm here -- on data with two hours of "
                        + "life left, which is an ordinary lagging consumer rather than an "
                        + "incident")
                .isEmpty();
    }

    @Test
    void aPAUSEDStreamEXACTLYAtTheFloorDoesNOTAlarmYet() {
        RetentionObservable observable = observable();
        observable.observePaused(STREAM, clock.instant().minus(MIN_RETENTION));
        observable.sweep();
        assertThat(raised)
                .as("at exactly the floor the outage budget has not yet been spent, and an "
                        + "alarm AT the boundary fires on a cluster that is fine")
                .isEmpty();
    }

    @Test
    void dataEXACTLYAtTheWarningThresholdDoesNOTAlarmYet() {
        RetentionObservable observable = observable();
        observable.observeOldestUnread(STREAM,
                clock.instant().minus(MAX_RETENTION.minus(Duration.ofHours(1))));
        observable.sweep();
        assertThat(raised).isEmpty();
    }

    @Test
    void theOBSERVABLEHoldsNOStoreAndReadsTheClockThroughItsSEAM() {
        for (var field : RetentionObservable.class.getDeclaredFields()) {
            assertThat(field.getType().getName())
                    .as("field %s: telemetry that read the store would make observing the "
                            + "cluster cost what running it costs", field.getName())
                    .doesNotContain("binjava.binstore");
        }
        assertThat(RetentionObservable.class.getDeclaredFields())
                .as("and the clock is a FIELD of type Clock -- a class calling "
                        + "Instant.now() would read the wall clock and make every case "
                        + "here a race rather than an assertion")
                .anySatisfy(field -> assertThat(field.getType()).isEqualTo(Clock.class));
    }
}
