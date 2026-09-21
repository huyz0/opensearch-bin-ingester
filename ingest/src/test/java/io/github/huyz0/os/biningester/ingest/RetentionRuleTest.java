// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * When a segment may be deleted (M7.5, FR-9, NFR-13, research 09 §7).
 *
 * <p>⚠️ HALF THESE CASES ASSERT A KEEP, AND THAT IS THE POINT. A rule that
 * deletes nothing passes every delete-case's sibling and fails the delete
 * cases; a rule that deletes everything does the reverse. Neither direction can
 * be satisfied by doing less.
 */
class RetentionRuleTest {

    private static final Duration MIN_RETENTION = Duration.ofHours(6);
    private static final Duration MAX_RETENTION = Duration.ofHours(24);
    private static final long SAFETY_MARGIN = 100;

    private static final UUID INDEX = new UUID(0x1111_2222_3333_4444L, 1);
    private static final RunKey STREAM = new RunKey(INDEX, 0);
    private static final RunKey OTHER = new RunKey(INDEX, 1);

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
    }

    private final TestClock clock = new TestClock();
    private final List<String> alarms = new ArrayList<>();

    private RetentionRule rule(RetentionRule.Watermarks watermarks) {
        return new RetentionRule(clock, MIN_RETENTION, MAX_RETENTION, SAFETY_MARGIN,
                (key, age) -> alarms.add(key), watermarks);
    }

    /** A segment of one stream, written {@code age} ago, ending at {@code endOffset}. */
    private RetentionRule.SegmentFacts segment(Duration age, long endOffset) {
        return new RetentionRule.SegmentFacts("seg-1", clock.instant().minus(age),
                Map.of(STREAM, endOffset));
    }

    private static RetentionRule.Watermarks at(long position, boolean fresh) {
        return stream -> new WatermarkTable.Watermark(true, fresh, position);
    }

    private static final RetentionRule.Watermarks NOBODY_REPORTED =
            stream -> new WatermarkTable.Watermark(false, false, 0);

    @Test
    void aFULLYConsumedSegmentINSIDETheTimeFloorIsKEPT() {
        RetentionRule.Verdict verdict = rule(at(1_000_000, true))
                .verdictFor(segment(Duration.ofMinutes(20), 500));
        assertThat(verdict.delete())
                .as("⚠️ THE CASE A FASTER, CHEAPER, WRONG RULE FAILS ALONE. The outage "
                        + "budget IS minRetention (NFR-13): a consumer that restarts into "
                        + "a bucket emptied twenty minutes after the write has lost data "
                        + "it never committed, because the position it reported was the "
                        + "IN-MEMORY one and a restart resumes from the last Lucene commit")
                .isFalse();
        assertThat(alarms).isEmpty();
    }

    @Test
    void aSegmentPASTTheFloorAndBELOWTheWatermarkIsDELETED() {
        RetentionRule.Verdict verdict = rule(at(1_000, true))
                .verdictFor(segment(Duration.ofHours(7), 500));
        assertThat(verdict.delete())
                .as("past the floor, every copy fresh, and the slowest of them 500 records "
                        + "beyond this segment's end -- the case a keep-everything rule "
                        + "fails alone")
                .isTrue();
        assertThat(verdict.ceiling()).isFalse();
        assertThat(alarms).isEmpty();
    }

    @Test
    void aSegmentPASTTheFloorButABOVETheWatermarkIsKEPT() {
        assertThat(rule(at(400, true)).verdictFor(segment(Duration.ofHours(7), 500)).delete())
                .as("the slowest copy has not reached this segment's end, so the records "
                        + "in it are unread whatever their age")
                .isFalse();
    }

    @Test
    void theBOUNDARYIsTheEndOffsetPLUSTheMargin() {
        RetentionRule.SegmentFacts seg = segment(Duration.ofHours(7), 500);
        assertThat(rule(at(500 + SAFETY_MARGIN, true)).verdictFor(seg).delete())
                .as("EXACTLY at endOffset + safetyMargin is KEPT: the rule is strictly "
                        + "greater, and `>=` gives away the whole margin, which exists "
                        + "because the reported position can be a full Lucene commit "
                        + "interval ahead of what a restart resumes from")
                .isFalse();
        assertThat(rule(at(500 + SAFETY_MARGIN + 1, true)).verdictFor(seg).delete())
                .as("one past the boundary deletes -- so the boundary is pinned on both "
                        + "sides rather than only from above")
                .isTrue();
    }

    @Test
    void theMARGINItselfIsLOADBearing() {
        assertThat(rule(at(501, true)).verdictFor(segment(Duration.ofHours(7), 500)).delete())
                .as("a rule with no safetyMargin deletes here, and the consumer that "
                        + "reported 501 resumes from an earlier committed position after a "
                        + "crash -- reading records this segment held")
                .isFalse();
    }

    @Test
    void anUNFRESHWatermarkKEEPSWhateverItsValue() {
        assertThat(rule(at(1_000_000, false)).verdictFor(segment(Duration.ofHours(7), 500))
                .delete())
                .as("a copy silent past reportTimeout makes the min() untrustworthy, so "
                        + "the watermark clause is simply unsatisfied. GC pausing is the "
                        + "correct failure direction")
                .isFalse();
    }

    @Test
    void aStreamNOBODYHasReportedOnKEEPS() {
        assertThat(rule(NOBODY_REPORTED).verdictFor(segment(Duration.ofHours(7), 500)).delete())
                .as("nobody has reported is not nobody has read -- and a rule that read "
                        + "the unknown as zero would keep, while one that read it as "
                        + "MAX_VALUE would delete the whole stream")
                .isFalse();
        assertThat(rule(NOBODY_REPORTED).verdictFor(segment(Duration.ofHours(7), 500)).why())
                .as("and the clause that decided is the UNKNOWN one, not the freshness one "
                        + "next to it -- an unknown watermark is never fresh, so a rule "
                        + "that dropped the known() check would keep for the right value "
                        + "and the wrong reason, and an operator would go looking for a "
                        + "silent copy that does not exist")
                .contains("no copy has reported");
    }

    /** Ready first, unready second — so a rule judging only the FIRST deletes. */
    private static RetentionRule.SegmentFacts readyThenUnready(Instant writtenAt) {
        java.util.LinkedHashMap<RunKey, Long> ordered = new java.util.LinkedHashMap<>();
        ordered.put(STREAM, 500L);
        ordered.put(OTHER, 900L);
        return new RetentionRule.SegmentFacts("seg-1", writtenAt, ordered);
    }

    /** The same two streams the other way round. */
    private static RetentionRule.SegmentFacts unreadyThenReady(Instant writtenAt) {
        java.util.LinkedHashMap<RunKey, Long> ordered = new java.util.LinkedHashMap<>();
        ordered.put(OTHER, 900L);
        ordered.put(STREAM, 500L);
        return new RetentionRule.SegmentFacts("seg-1", writtenAt, ordered);
    }

    private static final RetentionRule.Watermarks MIXED = key -> key.equals(STREAM)
            ? new WatermarkTable.Watermark(true, true, 10_000)
            : new WatermarkTable.Watermark(true, true, 200);

    @Test
    void EVERYStreamInTheSegmentMustBeReady() {
        assertThat(rule(MIXED).verdictFor(readyThenUnready(
                clock.instant().minus(Duration.ofHours(7)))).delete())
                .as("⚠️ A SEGMENT IS SHARED BY EVERY STREAM IT BUNDLES, which is what this "
                        + "product IS. Deleting it because ONE stream's consumer is past "
                        + "it deletes the other stream's unread records -- and at 1,600 "
                        + "streams per segment that is the normal case, not an edge one. "
                        + "⚠️ THE READY STREAM IS FIRST, DELIBERATELY: a rule judging only "
                        + "the first deletes here, and review MEASURED that mutation "
                        + "surviving 4 runs in 6 while the fixture used a Map.of whose "
                        + "iteration order the JVM salts")
                .isFalse();
    }

    @Test
    void EVERYStreamInTheSegmentMustBeReadyTheOTHERWayRoundToo() {
        assertThat(rule(MIXED).verdictFor(unreadyThenReady(
                clock.instant().minus(Duration.ofHours(7)))).delete())
                .as("the same two streams in the opposite order: this one is green for a "
                        + "first-only rule as well, and having BOTH is what makes the "
                        + "pair insensitive to any iteration order the production copy "
                        + "chooses")
                .isFalse();
    }

    @Test
    void aSegmentPASTTheCEILINGIsDELETEDAndALARMS() {
        RetentionRule.Verdict verdict = rule(at(0, true))
                .verdictFor(segment(Duration.ofHours(25), 500));
        assertThat(verdict.delete()).isTrue();
        assertThat(verdict.ceiling()).isTrue();
        assertThat(alarms)
                .as("⚠️ THE ASSERTION IS THAT THE ALARM FIRED, NOT THAT THE OBJECT WENT. "
                        + "Crossing maxRetention means a consumer WILL lose data -- a "
                        + "paused shard pins retention forever (§6.5) -- and deleting it "
                        + "quietly is worse than the storage bill it prevents")
                .containsExactly("seg-1");
    }

    @Test
    void theCEILINGFiresEvenWhenNOBODYHasReported() {
        RetentionRule.Verdict verdict = rule(NOBODY_REPORTED)
                .verdictFor(segment(Duration.ofHours(25), 500));
        assertThat(verdict.delete())
                .as("the ceiling is the one clause that must fire when every other says "
                        + "keep -- an unparenthesised rule reads as AND over the whole "
                        + "expression and the ceiling never fires alone, which is storage "
                        + "growing without bound behind a paused shard")
                .isTrue();
        assertThat(alarms).hasSize(1);
    }

    @Test
    void theCEILINGFiresEvenWhenTheTableIsUNFRESH() {
        assertThat(rule(at(0, false)).verdictFor(segment(Duration.ofHours(25), 500)).delete())
                .isTrue();
        assertThat(alarms).hasSize(1);
    }

    @Test
    void aSegmentEXACTLYAtTheFloorIsKEPT() {
        assertThat(rule(at(1_000_000, true)).verdictFor(segment(MIN_RETENTION, 500)).delete())
                .as("the floor is strictly greater too: at exactly six hours the outage "
                        + "budget has not yet been spent")
                .isFalse();
    }

    @Test
    void aSegmentEXACTLYAtTheCeilingDoesNOTAlarmYet() {
        RetentionRule.Verdict verdict = rule(at(0, true)).verdictFor(segment(MAX_RETENTION, 500));
        assertThat(verdict.ceiling())
                .as("an alarm that fires AT the limit rather than past it fires on every "
                        + "healthy cluster with a consumer exactly at the budget, and an "
                        + "alarm that fires routinely is one nobody reads")
                .isFalse();
        assertThat(alarms).isEmpty();
    }

    @Test
    void aVerdictSaysWHYItKept() {
        assertThat(rule(at(400, true)).verdictFor(segment(Duration.ofHours(7), 500)).why())
                .as("an operator asking why storage is not falling needs the clause, not a "
                        + "boolean -- and the three reasons (inside the floor, unfresh, "
                        + "behind the watermark) are three different incidents")
                .contains("watermark");
    }

    @Test
    void anOffsetNearOverflowDoesNotWRAPIntoADelete() {
        RetentionRule.SegmentFacts huge = new RetentionRule.SegmentFacts("seg-1",
                clock.instant().minus(Duration.ofHours(7)), Map.of(STREAM, Long.MAX_VALUE - 1));
        assertThat(rule(at(Long.MAX_VALUE, true)).verdictFor(huge).delete())
                .as("endOffset + safetyMargin overflows to a negative, which every "
                        + "watermark is greater than -- so the arithmetic alone deletes a "
                        + "segment nobody has read")
                .isFalse();
    }

    @Test
    void aSegmentWithNOStreamsIsREFUSED() {
        assertThatThrownBy(() -> new RetentionRule.SegmentFacts("seg-1", clock.instant(),
                Map.of()))
                .as("a segment that indexes no stream cannot be judged by a rule about "
                        + "streams, and treating it as trivially deletable deletes an "
                        + "object whose commit is merely not read yet")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aCEILINGNoLongerThanTheFLOORIsREFUSED() {
        assertThatThrownBy(() -> new RetentionRule(clock, MIN_RETENTION, MIN_RETENTION,
                SAFETY_MARGIN, (k, a) -> { }, at(0, true)))
                .as("a ceiling at or below the floor deletes everything the moment it is "
                        + "past the floor, alarming each time -- the outage budget turned "
                        + "off and an alarm that means nothing")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetention");
    }

    @Test
    void aNEGATIVESafetyMarginIsREFUSED() {
        assertThatThrownBy(() -> new RetentionRule(clock, MIN_RETENTION, MAX_RETENTION, -1,
                (k, a) -> { }, at(0, true)))
                .as("a negative margin is an ACCELERATOR: it deletes segments the slowest "
                        + "copy has not reached, which is the one thing the margin exists "
                        + "to prevent")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
