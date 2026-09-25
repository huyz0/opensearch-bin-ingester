// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The one job LIST keeps (M7.8, FR-9, research 06 §4, cost rules R2 and R15).
 *
 * <p>⚠️ A POD THAT DIED AFTER THE PUT AND BEFORE THE COMMIT LEAVES A SEGMENT
 * THE LOG BY DEFINITION CANNOT KNOW ABOUT, which is why commit-log-driven GC
 * cannot find it and why this sweep exists at all.
 *
 * <p>⚠️ THE CASE THAT MATTERS IS THE ONE THAT KEEPS. Deleting an uncommitted
 * segment that is merely SLOW to commit destroys acknowledged data: the commit
 * may still be in flight, and the degraded {@code ctl/inbox/} path (M8's) is
 * slower than the direct one. The maximum in-flight delay remains a stated
 * bound rather than a measured worst case. The past-grace delete is the easy
 * half.
 */
class OrphanSweepTest {

    private static final Duration GRACE = Duration.ofHours(1);

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
    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);

    private OrphanSweep sweep() {
        return new OrphanSweep(store, clock, GRACE, 1000, 1000);
    }

    private String put(Duration age, long sequence) throws Exception {
        long millis = clock.instant().minus(age).toEpochMilli();
        String key = new SegmentKey("bucket", millis, "poda", sequence, 12).key();
        backing.put(key, Body.ofBytes(new byte[] {1}));
        return key;
    }

    private String hourPrefixFor(Duration age) {
        return SegmentKey.hourPrefix("bucket", clock.instant().minus(age).toEpochMilli());
    }

    @Test
    void anUNCOMMITTEDSegmentPASTTheGraceIsDELETED() throws Exception {
        String key = put(Duration.ofHours(2), 1);
        OrphanSweep.Result result = sweep().sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(result.deleted()).isEqualTo(1);
        assertThat(backing.stat(key)).isEmpty();
    }

    @Test
    void anUNCOMMITTEDSegmentWITHINTheGraceIsKEPT() throws Exception {
        String key = put(Duration.ofMinutes(30), 1);
        OrphanSweep.Result result = sweep().sweep(hourPrefixFor(Duration.ofMinutes(30)),
                Set.of());
        assertThat(result.deleted())
                .as("⚠️ THE CASE THAT MATTERS. Its commit may still be in flight -- the "
                        + "degraded inbox path is slower than the direct one -- and "
                        + "deleting it destroys acknowledged data. The grace must exceed "
                        + "the maximum possible commit delay")
                .isZero();
        assertThat(result.withinGrace()).isEqualTo(1);
        assertThat(backing.stat(key)).isNotEmpty();
    }

    @Test
    void aSegmentEXACTLYAtTheGraceIsKEPT() throws Exception {
        put(GRACE, 1);
        assertThat(sweep().sweep(hourPrefixFor(GRACE), Set.of()).deleted())
                .as("strictly past, so an hour-old object on a one-hour grace is still "
                        + "inside it -- and a sweep that fired AT the boundary would race "
                        + "the commit it is waiting for")
                .isZero();
    }

    @Test
    void aCOMMITTEDSegmentIsNEVERACandidateWhateverItsAge() throws Exception {
        String key = put(Duration.ofHours(9), 1);
        OrphanSweep.Result result = sweep().sweep(hourPrefixFor(Duration.ofHours(9)),
                Set.of(key));
        assertThat(result.deleted())
                .as("⚠️ A COMMITTED SEGMENT IS THE COMMIT LOG'S TO COLLECT, and only the "
                        + "log knows whether its records have been read. A sweep that "
                        + "deleted by age alone would delete live, referenced data the "
                        + "moment retention exceeded the grace")
                .isZero();
        assertThat(result.committed()).isEqualTo(1);
        assertThat(backing.stat(key)).isNotEmpty();
    }

    @Test
    void ONEHourPrefixPerPassAndNOOther() throws Exception {
        String thisHour = put(Duration.ofHours(2), 1);
        String otherHour = put(Duration.ofHours(5), 2);
        sweep().sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(backing.stat(thisHour)).isEmpty();
        assertThat(backing.stat(otherHour))
                .as("one prefix per pass is what bounds the LIST rate under R15's ~1/s "
                        + "ceiling; a sweep that walked the whole data prefix would issue "
                        + "a LIST per 1,000 objects of the ENTIRE retention window in one "
                        + "burst")
                .isNotEmpty();
    }

    @Test
    void theLISTBudgetIsONECallPerThousandKeys() throws Exception {
        for (long i = 0; i < 2500; i++) {
            put(Duration.ofHours(2), i);
        }
        sweep().sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(store.counts().lists())
                .as("2,500 keys is three pages of 1,000 -- and a LIST costs what a PUT "
                        + "costs, so a page size of 1 would make the sweep 2,500 times the "
                        + "bill of the segments it is collecting")
                .isEqualTo(3);
    }

    @Test
    void theDELETEBudgetIsONECallPerThousandKeys() throws Exception {
        for (long i = 0; i < 2500; i++) {
            put(Duration.ofHours(2), i);
        }
        OrphanSweep.Result result = sweep().sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(result.deleted()).isEqualTo(2500);
        assertThat(store.counts().deletes()).isEqualTo(3);
    }

    @Test
    void anEMPTYPrefixCostsEXACTLYOneListAndNoDelete() {
        OrphanSweep.Result result = sweep().sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(result.deleted()).isZero();
        assertThat(store.counts().lists())
                .as("one LIST is the floor -- the sweep cannot know a prefix is empty "
                        + "without asking once")
                .isEqualTo(1);
        assertThat(store.counts().deletes())
                .as("and an empty delete is still a request")
                .isZero();
    }

    @Test
    void aKeyThatIsNOTASegmentKeyIsLEFTAlone() throws Exception {
        backing.put(hourPrefixFor(Duration.ofHours(2)) + "something-else", Body.ofBytes(
                new byte[] {1}));
        OrphanSweep.Result result = sweep().sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(result.deleted())
                .as("a key whose age cannot be read from its name cannot be judged, and "
                        + "this sweep deletes only what it can date")
                .isZero();
        assertThat(result.unreadable()).isEqualTo(1);
    }

    @Test
    void aFAILEDDeleteLeavesTheObjectAndIsNotCounted() throws Exception {
        String key = put(Duration.ofHours(2), 1);
        OrphanSweep refusing = new OrphanSweep(new FailingDeleteStore(backing), clock, GRACE,
                1000, 1000);
        assertThat(refusing.sweep(hourPrefixFor(Duration.ofHours(2)), Set.of()).deleted())
                .isZero();
        assertThat(backing.stat(key)).isNotEmpty();
    }

    @Test
    void theCONFIGUREDPageSizeIsWhatIsAskedFor() throws Exception {
        for (long i = 0; i < 100; i++) {
            put(Duration.ofHours(2), i);
        }
        new OrphanSweep(store, clock, GRACE, 40, 1000)
                .sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(store.counts().lists())
                .as("100 keys at 40 per page is three LIST calls -- and a sweep that asked "
                        + "for the maximum whatever it was configured with leaves the "
                        + "budget unpinned, because every other case here uses 1,000")
                .isEqualTo(3);
    }

    @Test
    void theCONFIGUREDDeleteBatchIsWhatIsUsed() throws Exception {
        for (long i = 0; i < 100; i++) {
            put(Duration.ofHours(2), i);
        }
        new OrphanSweep(store, clock, GRACE, 1000, 40)
                .sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(store.counts().deletes()).isEqualTo(3);
    }

    @Test
    void theDEFAULTSAreNamedInTheTreeRatherThanInEachCallersHead() {
        assertThat(OrphanSweep.DEFAULT_GRACE)
                .as("⚠️ research 06 §4's one hour, and generous on purpose: it exceeds "
                        + "the conservative commit-delay bound INCLUDING the degraded inbox "
                        + "path; its maximum in-flight delay is not measured. A caller picking "
                        + "its own from memory is how this becomes ten minutes on one "
                        + "deployment and acknowledged data is deleted")
                .isEqualTo(Duration.ofHours(1));
        assertThat(OrphanSweep.DEFAULT_PERIOD)
                .as("and the R15 claim is about THIS number: 44 LIST for an hour-prefix at "
                        + "one pass an hour is ~0.012/s, three orders under the ceiling; "
                        + "the period is part of the budget, not a scheduling detail")
                .isEqualTo(Duration.ofHours(1));
    }

    @Test
    void aPrefixWIDERThanOneHourIsREFUSED() {
        assertThatThrownBy(() -> sweep().sweep("bucket/data/", Set.of()))
                .as("`bucket/data/` is a legal prefix to a store and walks the WHOLE "
                        + "retention window in one burst -- thousands of LIST calls where "
                        + "the budget is 44, and this class's R15 claim would be false the "
                        + "first time a caller passed one")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hour prefix");
    }

    @Test
    void aNONPOSITIVEGraceIsREFUSED() {
        assertThatThrownBy(() -> new OrphanSweep(store, clock, Duration.ZERO, 1000, 1000))
                .as("a zero grace deletes a segment whose commit is one millisecond behind "
                        + "its PUT, which is every segment at some point")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPageSizeABOVEAThousandIsREFUSED() {
        assertThatThrownBy(() -> new OrphanSweep(store, clock, GRACE, 1001, 1000))
                .as("1,000 is what a LIST returns; asking for more is a request that "
                        + "silently returns 1,000 anyway, and a budget asserted against the "
                        + "number asked for rather than the number returned is not a budget")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aSweptPrefixDoesNOTReadAnyObject() throws Exception {
        for (long i = 0; i < 10; i++) {
            put(Duration.ofHours(2), i);
        }
        sweep().sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(store.counts().gets())
                .as("a sweep reads NAMES, never bodies -- a GET per candidate would make "
                        + "reconciliation cost more than the storage it reclaims")
                .isZero();
        assertThat(store.counts().stats())
                .as("and no stat either: the age is in the key, and the store's own "
                        + "last-modified time is not the write time once an object has "
                        + "been copied by a lifecycle rule")
                .isZero();
    }

    @Test
    void aFAILEDListENDSThePassWithoutTHROWING() throws Exception {
        put(Duration.ofHours(2), 1);
        OrphanSweep refusing = new OrphanSweep(new FailingListStore(backing), clock, GRACE,
                1000, 1000);
        org.assertj.core.api.Assertions
                .assertThatCode(() -> refusing.sweep(hourPrefixFor(Duration.ofHours(2)),
                        Set.of()))
                .as("this runs on a timer inside a pod; a throw out of it kills whatever "
                        + "schedules it and the sweep stops for every hour-prefix, not for "
                        + "one pass -- and an unreachable store is the ordinary reason a "
                        + "LIST fails")
                .doesNotThrowAnyException();
    }

    @Test
    void aFAILEDListDeletesNOTHINGItNeverSaw() throws Exception {
        String key = put(Duration.ofHours(2), 1);
        new OrphanSweep(new FailingListStore(backing), clock, GRACE, 1000, 1000)
                .sweep(hourPrefixFor(Duration.ofHours(2)), Set.of());
        assertThat(backing.stat(key))
                .as("a pass that could not read the prefix has judged nothing, and the "
                        + "next one finds the same objects")
                .isNotEmpty();
    }

    /** A store whose list always refuses. */
    private static final class FailingListStore extends ForwardingIngestStore {
        FailingListStore(io.github.huyz0.os.biningester.binstore.BinStore delegate) {
            super(delegate);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.ListPage list(String prefix, String startAfter, int maxKeys)
                throws java.io.IOException {
            throw new java.io.IOException("the store is unreachable");
        }
    }

    /** A store whose delete always refuses. */
    private static final class FailingDeleteStore extends ForwardingIngestStore {
        FailingDeleteStore(io.github.huyz0.os.biningester.binstore.BinStore delegate) {
            super(delegate);
        }

        @Override
        public void delete(java.util.List<String> keys) throws java.io.IOException {
            throw new java.io.IOException("the store is unreachable");
        }
    }
}
