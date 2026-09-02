// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.ListPage;
import binjava.format.MembershipFilter;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentKey;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** ⚠️ One flush is one PUT: the request rate this system exists to control. */
class SegmentPublisherTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;

        @Override public long millis() { return millis; }
        void advance(Duration d) { millis += d.toMillis(); }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis); }
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId z) { return this; }
    }

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                "{\"n\":1}".getBytes(StandardCharsets.UTF_8));
    }

    private static Accumulator accumulator(Clock clock) {
        return new Accumulator(new IngestConfig(Duration.ofMillis(250), 1 << 20, "c"), clock);
    }

    @Test
    void oneFlushIsOnePutWhateverItCarries() throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");
        Accumulator acc = accumulator(clock);

        // three streams, two indices, many records -- still one object
        for (int i = 0; i < 50; i++) {
            acc.add(new RunKey(A, 0), record("a" + i));
            acc.add(new RunKey(A, 1), record("b" + i));
            acc.add(new RunKey(B, 0), record("c" + i));
        }
        clock.advance(Duration.ofMillis(250));
        String key = publisher.publish(acc).orElseThrow().key();

        // ⚠️ M2.6: the filter's ordinal lookups pay a ONE-TIME cost for a
        // genuinely NEW index (ADR-0008, "~0/s, only on index creation") --
        // A and B are both new here, so this first flush costs 1 (the segment)
        // + 2 (one CAS registration each). THE number this test exists to pin
        // is the STEADY-STATE one, asserted below: once an index is known,
        // publishing it again costs exactly one request again, whatever else
        // it carries.
        assertThat(store.counts().puts()).as("segment + 2 first-ever index registrations")
                .isEqualTo(3);

        // A second flush of the SAME two (now-known) indices: 150 more
        // records across 3 streams cost exactly one more request; a design
        // that scaled with records, partitions or indices would show 150, 3
        // or 2 here instead of 1.
        for (int i = 0; i < 50; i++) {
            acc.add(new RunKey(A, 0), record("a" + i));
            acc.add(new RunKey(A, 1), record("b" + i));
            acc.add(new RunKey(B, 0), record("c" + i));
        }
        clock.advance(Duration.ofMillis(250));
        long before = store.counts().total();
        publisher.publish(acc);
        assertThat(store.counts().total() - before)
                .as("both indices already known -- exactly one request, same as M1")
                .isEqualTo(1);

        SegmentReader r = SegmentReader.open(store.get(key).readAllBytes());
        assertThat(r.directory()).hasSize(3);
    }

    @Test
    void aPodShortIdContainingDashOrSlashIsRefusedAtConstruction() throws Exception {
        // ⚠️ Same restriction SegmentKey's own constructor enforces (round-1
        // review, M2.6) -- checked in THIS constructor too, so a bad
        // podShortId fails here rather than being silently deferred to this
        // publisher's first flush (SegmentKey's own constructor would still
        // catch it there, but later and less clearly).
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                () -> new SegmentPublisher(store, "bins/c", "pod-0123456789abcdef-h5")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(org.assertj.core.api.Assertions.catchThrowable(
                () -> new SegmentPublisher(store, "bins/c", "pod/7")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nothingBufferedMeansNoRequestAtAll() throws Exception {
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");
        Accumulator acc = accumulator(new TestClock());

        assertThat(publisher.publish(acc)).isEmpty();
        assertThat(publisher.publish(acc)).isEmpty();
        // ⚠️ Criterion 3 in its simplest form: an idle stream that still PUT an
        // empty object would cost one request per interval, forever.
        assertThat(store.counts().total()).isZero();
    }

    @Test
    void theKeyCarriesTheHeaderLengthTheSegmentActuallyHas() throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");
        Accumulator acc = accumulator(clock);
        acc.add(new RunKey(A, 0), record("1"));
        acc.add(new RunKey(B, 3), record("2"));
        clock.advance(Duration.ofMillis(250));

        String key = publisher.publish(acc).orElseThrow().key();
        // ⚠️ Read back OUT of the bytes, not recomputed, so the key cannot
        // disagree with the object. Two runs is two directory entries (M3;
        // ADR-0025: 49 bytes each, the reserved lane byte).
        assertThat(SegmentKey.headerLenOf(key))
                .isEqualTo(2 * binjava.format.SegmentFormat.DIRECTORY_ENTRY_BYTES);

        // and the promised range covers preamble + directory exactly
        long end = SegmentPublisher.headerRangeEndInclusive(key);
        byte[] head = store.getRange(key, 0, end).readAllBytes();
        assertThat(head).hasSize((int) end + 1);
        assertThat(java.nio.ByteBuffer.wrap(head).order(java.nio.ByteOrder.BIG_ENDIAN).getInt(24))
                .as("runCount is readable from that one range").isEqualTo(2);
    }

    @Test
    void theKeyCarriesARealFilterNotTheLiteralN() throws Exception {
        // ⚠️ M2.6: the whole point of this task -- the filter slot M1 always
        // wrote "N" into now carries a real, computed membership filter.
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");
        Accumulator acc = accumulator(clock);
        acc.add(new RunKey(A, 0), record("1"));
        acc.add(new RunKey(B, 3), record("2"));
        clock.advance(Duration.ofMillis(250));

        String key = publisher.publish(acc).orElseThrow().key();
        assertThat(key).as("two brand-new indices, not registered elsewhere -- A is correct")
                .endsWith("-A.bseg");

        // ⚠️ And a THIRD index, absent from this segment: even under A's
        // "every registered index is present" semantics, mightContain
        // answers true for everything -- that is A's own documented
        // contract, not a gap this test is missing.
        String embedded = key.substring(key.lastIndexOf('-') + 1, key.length() - ".bseg".length());
        MembershipFilter filter = MembershipFilter.decode(embedded);
        assertThat(filter).isInstanceOf(MembershipFilter.All.class);
    }

    @Test
    void aSegmentMissingARegisteredIndexGetsAnExactOrBloomFilterNotA() throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");

        // ⚠️ Register a THIRD index first, via a segment that does not touch
        // A or B at all -- so a LATER segment touching only A and B is
        // genuinely missing a registered index and must not claim A.
        UUID c = UUID.fromString("00000000-0000-0000-0000-0000000000cc");
        Accumulator first = accumulator(clock);
        first.add(new RunKey(c, 0), record("0"));
        clock.advance(Duration.ofMillis(250));
        publisher.publish(first);

        Accumulator second = accumulator(clock);
        second.add(new RunKey(A, 0), record("1"));
        second.add(new RunKey(B, 3), record("2"));
        clock.advance(Duration.ofMillis(250));
        String key = publisher.publish(second).orElseThrow().key();

        String embedded = key.substring(key.lastIndexOf('-') + 1, key.length() - ".bseg".length());
        MembershipFilter filter = MembershipFilter.decode(embedded);
        assertThat(filter).as("3 registered, only 2 present -- A would be a lie")
                .isNotInstanceOf(MembershipFilter.All.class);
        assertThat(filter).isNotInstanceOf(MembershipFilter.None.class);

        // ⚠️ test-reviewer (M2.6, round 1): asserting only the filter's TYPE
        // cannot catch a wiring bug in SegmentPublisher.chooseFilter's OWN
        // ordinal resolution -- e.g. resolving the wrong index's ordinal
        // while still correctly choosing "not A, not N" because the set size
        // still differs from the total. Decoding against the REAL ordinals
        // this registry actually assigned (read back independently, not
        // re-derived) is what would catch that: a dropped or swapped
        // ordinal would make mightContain wrongly answer false for an index
        // genuinely present in the segment.
        IndexOrdinalRegistry independent = new IndexOrdinalRegistry(store, "bins/c");
        int ordinalA = independent.ordinalFor(A.toString());
        int ordinalB = independent.ordinalFor(B.toString());
        assertThat(mightContain(filter, ordinalA))
                .as("A is genuinely present in this segment").isTrue();
        assertThat(mightContain(filter, ordinalB))
                .as("B is genuinely present in this segment").isTrue();
    }

    /**
     * ⚠️ {@code None} has no {@code mightContain} at the type level (ADR-0003:
     * unrecognised means "must read", never "no match"), and the sealed
     * interface itself declares only {@code tag()}/{@code encode()} -- every
     * other variant's query is reached by its own concrete type, matched here
     * rather than assumed via a common interface method that does not exist.
     */
    private static boolean mightContain(MembershipFilter filter, int ordinal) {
        return switch (filter) {
            case MembershipFilter.All f -> f.mightContain(ordinal);
            case MembershipFilter.ExactBitmap f -> f.mightContain(ordinal);
            case MembershipFilter.RunLength f -> f.mightContain(ordinal);
            case MembershipFilter.Bloom f -> f.mightContain(ordinal);
            case MembershipFilter.None f -> throw new AssertionError(
                    "test asserted this filter is not None before calling mightContain");
        };
    }

    @Test
    void theKeyIsStampedWithTheSegmentsOwnCreationTime() throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");
        Accumulator acc = accumulator(clock);
        acc.add(new RunKey(A, 0), record("1"));
        long firstAppend = clock.millis();
        clock.advance(Duration.ofMillis(250));

        String key = publisher.publish(acc).orElseThrow().key();
        // ⚠️ The time PATH must match the segment's own createdAt, or a recovery
        // walk bounded to an hour looks in the wrong hour and finds nothing.
        assertThat(key).startsWith(SegmentKey.hourPrefix("bins/c", firstAppend));
    }

    @Test
    void successiveSegmentsGetDistinctKeys() throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");
        Accumulator acc = accumulator(clock);

        acc.add(new RunKey(A, 0), record("1"));
        clock.advance(Duration.ofMillis(250));
        String first = publisher.publish(acc).orElseThrow().key();
        acc.add(new RunKey(A, 0), record("2"));
        clock.advance(Duration.ofMillis(250));
        String second = publisher.publish(acc).orElseThrow().key();

        // ⚠️ A collision would silently overwrite a segment the commit log has
        // already pointed at. The per-pod sequence is what prevents it inside
        // one millisecond, where the timestamp alone cannot.
        assertThat(first).isNotEqualTo(second);
        ListPage page = store.list("bins/c/data/", null, 100);
        assertThat(page.objects()).hasSize(2);
    }

    @Test
    void twoSegmentsInTheSameMillisecondStillGetDistinctKeys() throws Exception {
        TestClock clock = new TestClock();
        CountingBinStore store = new CountingBinStore(
                new binjava.binstore.backend.MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/c", "pod7");

        Accumulator one = accumulator(clock);
        one.add(new RunKey(A, 0), record("1"));
        Accumulator two = accumulator(clock);
        two.add(new RunKey(A, 0), record("2"));
        // ⚠️ The clock does NOT advance: same millisecond, same pod.
        String a = publisher.publish(one).orElseThrow().key();
        String b = publisher.publish(two).orElseThrow().key();
        assertThat(a).isNotEqualTo(b);
        assertThat(store.list("bins/c/data/", null, 100).objects()).hasSize(2);
    }
}
