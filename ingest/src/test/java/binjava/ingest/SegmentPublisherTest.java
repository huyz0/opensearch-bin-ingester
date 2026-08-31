// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.CountingBinStore;
import binjava.binstore.ListPage;
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
        String key = publisher.publish(acc).orElseThrow();

        // ⚠️ THE number. 150 records across 3 streams and 2 indices cost exactly
        // one request; a design that scaled with records, partitions or indices
        // would show 150, 3 or 2 here.
        assertThat(store.counts().puts()).isEqualTo(1);
        assertThat(store.counts().total()).isEqualTo(1);

        SegmentReader r = SegmentReader.open(store.get(key).readAllBytes());
        assertThat(r.directory()).hasSize(3);
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

        String key = publisher.publish(acc).orElseThrow();
        // ⚠️ Read back OUT of the bytes, not recomputed, so the key cannot
        // disagree with the object. Two runs is two 48-byte entries.
        assertThat(SegmentKey.headerLenOf(key)).isEqualTo(96);

        // and the promised range covers preamble + directory exactly
        long end = SegmentPublisher.headerRangeEndInclusive(key);
        byte[] head = store.getRange(key, 0, end).readAllBytes();
        assertThat(head).hasSize((int) end + 1);
        assertThat(java.nio.ByteBuffer.wrap(head).order(java.nio.ByteOrder.BIG_ENDIAN).getInt(24))
                .as("runCount is readable from that one range").isEqualTo(2);
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

        String key = publisher.publish(acc).orElseThrow();
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
        String first = publisher.publish(acc).orElseThrow();
        acc.add(new RunKey(A, 0), record("2"));
        clock.advance(Duration.ofMillis(250));
        String second = publisher.publish(acc).orElseThrow();

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
        String a = publisher.publish(one).orElseThrow();
        String b = publisher.publish(two).orElseThrow();
        assertThat(a).isNotEqualTo(b);
        assertThat(store.list("bins/c/data/", null, 100).objects()).hasSize(2);
    }
}
