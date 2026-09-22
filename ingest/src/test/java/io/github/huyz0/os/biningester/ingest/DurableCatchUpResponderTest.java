// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DurableCatchUpResponderTest {

    private static final UUID REQUEST = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final RunKey KEY = new RunKey(
            UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), 7);
    private static final RunKey OTHER = new RunKey(
            UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef"), 7);

    @Test
    void replaysRunsFromBatchStartAndEndsTheResponse() throws Exception {
        byte[] segment = segment(KEY, "one");
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put("segments/one", Body.ofBytes(segment));
        List<Long> exclusives = new ArrayList<>();
        CommittedDeltaSource source = source(exclusives,
                new CommittedDeltaSource.CommittedRun(KEY, "segments/one", 1, 41));

        var responder = new DurableCatchUpResponder(store, source, () -> 9);
        var request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(KEY, 41)));

        List<byte[]> frames = responder.respond(request);

        var event = new CatchUpEventFrame(CatchUpEventFrame.decode(frames.get(0)).requestId(),
                SubscriptionEvent.decode(CatchUpEventFrame.decode(frames.get(0)).event().encode()));
        assertThat(event.requestId()).isEqualTo(REQUEST);
        assertThat(event.event().key()).isEqualTo(KEY);
        assertThat(event.event().firstOffset()).isEqualTo(41);
        assertThat(event.event().recordCount()).isEqualTo(1);
        assertThat(event.event().sequencerEpoch()).isEqualTo(9);
        assertThat(event.event().via()).isEqualTo(FetchMode.INLINE);
        assertThat(event.event().inline()).isEqualTo(segment);
        assertThat(CatchUpEndFrame.decode(frames.get(1)).requestId()).isEqualTo(REQUEST);
        assertThat(exclusives).first().isEqualTo(40L);
        assertThat(store.counts().gets()).isEqualTo(1);
    }

    @Test
    void readsOneSharedSegmentOnceForSeveralStreams() throws Exception {
        SegmentWriter writer = new SegmentWriter();
        writer.add(KEY, record("one"), 0);
        writer.add(OTHER, record("two"), 0);
        byte[] segment = writer.toByteArray(0);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put("segments/shared", Body.ofBytes(segment));
        CommittedDeltaSource source = (key, offset, limit) -> List.of(
                new CommittedDeltaSource.CommittedRun(key, "segments/shared", 1,
                        key.equals(KEY) ? 41 : 52)).stream()
                .filter(run -> offset < run.firstOffset())
                .toList();

        var responder = new DurableCatchUpResponder(store, source, () -> 9);
        var request = new CatchUpRequestFrame(REQUEST, List.of(
                new CatchUpRequestFrame.Stream(KEY, 0),
                new CatchUpRequestFrame.Stream(OTHER, 0)));

        List<byte[]> frames = responder.respond(request);

        assertThat(frames).hasSize(3);
        assertThat(store.counts().gets()).isEqualTo(1);
        assertThat(CatchUpEventFrame.decode(frames.get(0)).event().key()).isEqualTo(KEY);
        assertThat(CatchUpEventFrame.decode(frames.get(1)).event().key()).isEqualTo(OTHER);
    }

    @Test
    void convertsZeroBatchStartToTheSourceBeforeTheFirstRecord() throws Exception {
        List<Long> exclusives = new ArrayList<>();
        CommittedDeltaSource source = source(exclusives,
                new CommittedDeltaSource.CommittedRun(KEY, "segments/one", 1, 0));
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put("segments/one", Body.ofBytes(segment(KEY, "zero")));

        new DurableCatchUpResponder(store, source, () -> 9).respond(
                new CatchUpRequestFrame(REQUEST,
                        List.of(new CatchUpRequestFrame.Stream(KEY, 0))));

        assertThat(exclusives).first().isEqualTo(-1L);
    }

    @Test
    void emitsEveryRunAndRefusesAnAnswerOverItsAggregateBudget() throws Exception {
        byte[] first = segment(KEY, "one");
        byte[] second = segment(KEY, "two");
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put("segments/one", Body.ofBytes(first));
        store.put("segments/two", Body.ofBytes(second));
        CommittedDeltaSource source = new CommittedDeltaSource() {
            @Override
            public List<CommittedRun> replay(RunKey key, long offset, int limit) {
                if (!key.equals(KEY)) {
                    return List.of();
                }
                if (offset < 0) {
                    return List.of(new CommittedRun(KEY, "segments/one", 1, 0));
                }
                if (offset == 0) {
                    return List.of(new CommittedRun(KEY, "segments/two", 1, 1));
                }
                return List.of();
            }
        };

        List<byte[]> frames = new DurableCatchUpResponder(store, source, () -> 9)
                .respond(new CatchUpRequestFrame(REQUEST,
                        List.of(new CatchUpRequestFrame.Stream(KEY, 0))));

        assertThat(frames).hasSize(3);
        assertThat(CatchUpEventFrame.decode(frames.get(0)).event().firstOffset()).isZero();
        assertThat(CatchUpEventFrame.decode(frames.get(1)).event().firstOffset()).isEqualTo(1);
        assertThat(store.counts().gets()).isEqualTo(2);

        byte[] oneEvent = new CatchUpEventFrame(REQUEST, new SubscriptionEvent(
                REQUEST.toString(), 9, 1, KEY, "segments/one", 0, 1,
                FetchMode.INLINE, first)).encode();
        byte[] end = new CatchUpEndFrame(REQUEST).encode();
        long oneEventBudget = 4L + oneEvent.length + 4L + end.length;
        long cumulativeBudget = oneEventBudget + 4L + oneEvent.length - 1;
        assertThatThrownBy(() -> new DurableCatchUpResponder(store, source, () -> 9,
                cumulativeBudget).respond(new CatchUpRequestFrame(REQUEST,
                        List.of(new CatchUpRequestFrame.Stream(KEY, 0)))))
                .isInstanceOf(DurableCatchUpResponder.ResponseTooLargeException.class);

        try (var oversized = new MemoryBinStore()) {
            oversized.put("too-large", Body.ofBytes(new byte[65]));
            assertThatThrownBy(() -> new DurableCatchUpResponder(oversized,
                    (key, offset, limit) -> List.of(
                            new CommittedDeltaSource.CommittedRun(key, "too-large", 1, 0)),
                    () -> 9, 64).respond(new CatchUpRequestFrame(REQUEST,
                            List.of(new CatchUpRequestFrame.Stream(KEY, 0)))))
                    .isInstanceOf(IOException.class);
        }

        try (var probe = new ProbeStore()) {
            probe.put("too-large", Body.ofBytes(new byte[65]));
            assertThatThrownBy(() -> new DurableCatchUpResponder(probe,
                    (key, offset, limit) -> List.of(
                            new CommittedDeltaSource.CommittedRun(key, "too-large", 1, 0)),
                    () -> 9, 64).respond(new CatchUpRequestFrame(REQUEST,
                            List.of(new CatchUpRequestFrame.Stream(KEY, 0)))))
                    .isInstanceOf(DurableCatchUpResponder.ResponseTooLargeException.class);
        }
    }

    private static byte[] segment(RunKey key, String id) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, record(id), Instant.EPOCH.toEpochMilli());
        return writer.toByteArray(Instant.EPOCH.toEpochMilli());
    }

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1L),
                id.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static CommittedDeltaSource source(List<Long> exclusives,
            CommittedDeltaSource.CommittedRun run) {
        return new CommittedDeltaSource() {
            @Override
            public List<CommittedRun> replay(RunKey key, long offset, int limit) {
                exclusives.add(offset);
                return key.equals(run.key()) && offset < run.lastOffset() ? List.of(run) : List.of();
            }
        };
    }

    private static final class ProbeStore implements BinStore {
        private final MemoryBinStore delegate = new MemoryBinStore();

        @Override
        public java.io.InputStream get(String key) throws IOException {
            byte[] bytes;
            try (var in = delegate.get(key)) {
                bytes = in.readAllBytes();
            }
            return new InputStreamProbe(bytes);
        }

        @Override public java.io.InputStream getRange(String key, long start, long endIncl)
                throws IOException { return delegate.getRange(key, start, endIncl); }
        @Override public java.util.Optional<ObjectStat> stat(String key) throws IOException {
            return delegate.stat(key);
        }
        @Override public Version put(String key, Body body) throws IOException {
            return delegate.put(key, body);
        }
        @Override public java.util.Optional<Version> putIfAbsent(String key, Body body)
                throws IOException { return delegate.putIfAbsent(key, body); }
        @Override public java.util.Optional<Version> putIfMatch(String key, Body body,
                Version expected) throws IOException { return delegate.putIfMatch(key, body, expected); }
        @Override public MultipartWriter multipart(String key) throws IOException {
            return delegate.multipart(key);
        }
        @Override public ListPage list(String prefix, String startAfter, int maxKeys)
                throws IOException { return delegate.list(prefix, startAfter, maxKeys); }
        @Override public void delete(List<String> keys) throws IOException { delegate.delete(keys); }
        @Override public Capabilities capabilities() { return delegate.capabilities(); }
        @Override public void close() throws IOException { delegate.close(); }

        private static final class InputStreamProbe extends ByteArrayInputStream {
            InputStreamProbe(byte[] bytes) { super(bytes); }

            @Override
            public synchronized int read(byte[] bytes, int offset, int length) {
                if (length > 65) {
                    throw new AssertionError("the segment read was not bounded");
                }
                return super.read(bytes, offset, length);
            }
        }
    }
}
