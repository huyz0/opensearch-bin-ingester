// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A delta carries MANY segments, so the commit rate stops scaling with pods
 * (ADR-0032).
 *
 * <p>⚠️ THE COST MODEL IS THE SUBJECT HERE, not the bytes. One delta must carry
 * every pod flush in a {@code commitBatchInterval} window; with one segment per
 * entry the only way to commit K flushes is K entries, so the commit rate scales
 * with pods and the SPEC's $52/month becomes $52 x pods. No scheduler can fix
 * that — a one-segment record cannot carry two segments — which is why the
 * format changes before the batcher exists.
 */
class BatchedCommitDeltaTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static SegmentCommit seg(String key, UUID id, int partition, int count, long first) {
        return new SegmentCommit(key,
                List.of(new RunCommit(new RunKey(id, partition), count, first)));
    }

    /**
     * ⚠️ THREE SEGMENTS, NOT TWO, and the arity is load-bearing. With every
     * fixture in the tree carrying exactly two, hardcoding the written count as
     * {@code 2} instead of {@code segments.size()} survived all three module
     * suites — and a three-segment delta then round-trips to "chain entry has
     * bytes after its last field". Three pods flushing into one window is the
     * first thing M4.7's batcher produces, and ADR-0032 reasons about six.
     */
    private static CommitDelta batched() {
        return new CommitDelta(9, List.of(
                seg("bins/pod-a/0000000000000001.bseg", A, 0, 3, 100),
                seg("bins/pod-b/0000000000000002.bseg", B, 4, 5, 200),
                seg("bins/pod-c/0000000000000003.bseg", A, 9, 7, 300)));
    }

    @Test
    void aBatchedDeltaRoundTripsThroughItsEncoding() throws Exception {
        CommitDelta original = batched();

        CommitDelta decoded = CommitDelta.decode(original.encode());

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.segments()).hasSize(3);
        assertThat(decoded.segments().get(1).segmentKey())
                .as("the second pod's segment survives, and stays paired with its own runs")
                .isEqualTo("bins/pod-b/0000000000000002.bseg");
        assertThat(decoded.segments().get(1).runs().get(0).firstOffset()).isEqualTo(200);
        assertThat(decoded.segments().get(2).segmentKey())
                .as("and so does the THIRD -- a count written as a literal 2 stops here")
                .isEqualTo("bins/pod-c/0000000000000003.bseg");
        assertThat(decoded.segments().get(2).runs().get(0).firstOffset()).isEqualTo(300);
    }

    @Test
    void aBatchedDeltaIsWrittenUnderTheRESERVEDKindRatherThanANewVersion() {
        // ⚠️ Kind 0 was left unassigned by ADR-0028's implementation with a
        // comment saying it was for "a FUTURE delta layout ... at which point v0
        // and it are genuinely different shapes". Burning a fresh VERSION would
        // leave that reservation unused and unexplained, so this asserts the
        // bytes actually take the reserved slot.
        byte[] bytes = batched().encode();

        assertThat(bytes[7]).as("version word is the kinded v1").isEqualTo((byte) 1);
        assertThat(bytes[8]).as("and the kind is the one that was reserved").isEqualTo((byte) 0);
    }

    @Test
    void aSingleSegmentDeltaIsStillWrittenAsV0SoNoBucketChurns() {
        // ⚠️ THE POINT OF A CANONICAL ENCODING. Always emitting the new shape
        // would rewrite every delta in every bucket, and both golden files, to
        // say exactly what v0 already says -- which ADR-0028's implementation
        // refused for that reason and ADR-0032 keeps refusing.
        byte[] bytes = new CommitDelta(5, "bins/one.bseg",
                List.of(new RunCommit(new RunKey(A, 0), 1, 0))).encode();

        assertThat(bytes[7]).as("one segment is still plain v0").isZero();
    }

    @Test
    void askingABatchedDeltaForTHESegmentKeyIsRefusedRatherThanAnswered() {
        // ⚠️ THE REFUSAL IS THE SAFETY PROPERTY. Returning the first segment
        // would deliver a run under another pod's key, so the consumer fetches
        // the wrong object: the push succeeds, the offsets look right, and the
        // records are someone else's. Silent, and invisible to any test whose
        // window happened to batch a single flush.
        CommitDelta delta = batched();

        assertThatThrownBy(delta::segmentKey)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("3 segments")
                .hasMessageContaining("segments()");
    }

    @Test
    void askingABatchedDeltaForTHERunsIsRefusedRatherThanFlattened() {
        // ⚠️ Flattening here would divorce each run from the object holding its
        // records -- the one pairing this format exists to keep.
        assertThatThrownBy(batched()::runs)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("segments()");
    }

    @Test
    void allRunsSpansEverySegmentForTheOneCallerThatDoesNotCare() {
        // ⚠️ `ChainReplay.fold` folds offsets forward, and an offset is a STREAM
        // fact rather than a segment fact. That caller is why this accessor
        // exists; it is not a general-purpose escape from the pairing.
        assertThat(batched().allRuns()).hasSize(3);
        assertThat(batched().allRuns().stream().map(r -> r.key().partitionId()))
                .as("every segment's runs, in segment order -- not just the first's")
                .containsExactly(0, 4, 9);
    }

    @Test
    void aSingleSegmentDeltaStillAnswersBothConvenienceAccessors() {
        CommitDelta one = new CommitDelta(1, "bins/one.bseg",
                List.of(new RunCommit(new RunKey(A, 2), 4, 8)));

        assertThat(one.segmentKey()).isEqualTo("bins/one.bseg");
        assertThat(one.runs()).hasSize(1);
        assertThat(one.segments()).hasSize(1);
    }

    @Test
    void aBatchedEntryCarryingASINGLESegmentStillDECODES() throws Exception {
        // ⚠️ THE FORWARD-TOLERANCE HALF OF ADR-0032, and it was unfalsified:
        // narrowing the guard from `count <= 0` to `count <= 1` left the whole
        // suite green, because every other batched fixture here carries two.
        // The ADR spends four paragraphs buying this asymmetry — such an entry
        // decodes but is never WRITTEN — so the direction that matters is that
        // it decodes.
        ByteArrayOutputStream out = ChainEntry.kinded(ChainEntry.KIND_DELTA);
        SegmentWriter.putUvarint(out, 4);
        SegmentWriter.putUvarint(out, 1);
        byte[] key = "bins/only.bseg".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, key.length);
        out.writeBytes(key);
        SegmentWriter.putUvarint(out, 1);
        java.nio.ByteBuffer id = java.nio.ByteBuffer.allocate(16)
                .order(java.nio.ByteOrder.BIG_ENDIAN);
        id.putLong(A.getMostSignificantBits());
        id.putLong(A.getLeastSignificantBits());
        out.writeBytes(id.array());
        SegmentWriter.putUvarint(out, 0);
        SegmentWriter.putUvarint(out, 6);
        SegmentWriter.putUvarint(out, 12);

        CommitDelta decoded = CommitDelta.decode(out.toByteArray());

        assertThat(decoded.segments()).hasSize(1);
        assertThat(decoded.segmentKey()).isEqualTo("bins/only.bseg");
        assertThat(decoded.runs().get(0).firstOffset()).isEqualTo(12);
        // ⚠️ AND IT DOES NOT ROUND-TRIP TO ITS OWN BYTES, which is the price
        // stated in the ADR rather than an oversight: re-encoding yields the
        // CANONICAL form, which for one segment is v0.
        assertThat(decoded.encode()[7])
                .as("canonical re-encoding of one segment is v0, not the v1 it arrived as")
                .isZero();
    }

    @Test
    void aDeltaWithNoSegmentsIsRefused() {
        // ⚠️ Reachable only through the list-taking constructor — the one M4.7's
        // batcher will call — so nothing exercised it: the older
        // "delta with no runs" test now stops at SegmentCommit's guard instead,
        // and `if (segments.isEmpty())` survived being turned off.
        assertThatThrownBy(() -> new CommitDelta(1, List.<SegmentCommit>of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("commits nothing");
    }

    @Test
    void theSegmentsListIsCopiedAndUNMODIFIABLESoNoCallerCanGrowADelta() {
        // ⚠️ TWO PROPERTIES, because only one of them distinguishes `List.copyOf`
        // from `new ArrayList<>`: both defend against the CALLER'S list changing
        // afterwards, so that half alone leaves the weaker form an equivalent
        // mutant. What `copyOf` adds is that the list handed BACK cannot be
        // modified either — and a delta is a durable record of assigned
        // offsets, so a reader that can append to it can invent a commit.
        var mutable = new java.util.ArrayList<>(List.of(seg("bins/a.bseg", A, 0, 1, 0)));
        CommitDelta delta = new CommitDelta(1, mutable);

        mutable.add(seg("bins/sneaked-in.bseg", B, 0, 1, 99));

        assertThat(delta.segments())
                .as("the caller's later mutation must not reach a delta already built")
                .hasSize(1);
        assertThatThrownBy(() -> delta.segments().add(seg("bins/no.bseg", B, 0, 1, 0)))
                .as("nor may a reader append to one")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aSegmentCountLargerThanTheOBJECTIsRefusedRatherThanAllocated() {
        // ⚠️ `Cursor`'s own header promises "a length field read out of a
        // corrupt object must not become an array size". That was closed for
        // length-PREFIXED fields and left open for this repeat COUNT: measured
        // before the bound, a 15-byte object claiming 0x7FFFFFFF segments raised
        // OutOfMemoryError — past `recover`'s `throws IOException` and every
        // caller catching it — rather than failing as the corrupt object it is.
        // ⚠️ WITH BYTES LEFT AFTER THE COUNT, deliberately. A fixture that stops
        // at the count leaves `remaining()` at exactly 0, so it cannot tell the
        // bound from an emptiness check: `if (c.remaining() == 0)` survived it,
        // and a truncation almost never lands on a field boundary — trailing
        // bytes are the COMMON shape, not the exotic one.
        ByteArrayOutputStream out = ChainEntry.kinded(ChainEntry.KIND_DELTA);
        SegmentWriter.putUvarint(out, 5);
        SegmentWriter.putUvarint(out, 0x7FFFFFFFL);
        out.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("bytes remain");
    }

    @Test
    void aSegmentCountOnlyMODESTLYTooLargeIsAlsoRefused() {
        // ⚠️ A MODEST OVERCLAIM, because the huge one alone cannot distinguish a
        // real bound from `remaining() * 1000` or from `a.length` — both of
        // which survived the 0x7FFFFFFF fixture. Nine segments cannot live in
        // eight bytes; the smallest honest segment is ~22.
        ByteArrayOutputStream out = ChainEntry.kinded(ChainEntry.KIND_DELTA);
        SegmentWriter.putUvarint(out, 5);
        SegmentWriter.putUvarint(out, 9);
        out.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("9 segments");
    }

    @Test
    void aRUNCountLargerThanTheObjectIsRefusedToo() {
        // ⚠️ THE SECOND REPEAT COUNT, and it predates this change: reachable
        // from the v0 path every bucket already holds, and measured raising
        // `OutOfMemoryError` out of `ChainEntry.decode` before this commit
        // bounded it. Bounding one of two while a comment claimed the class was
        // closed is what made this worth fixing here rather than filing.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ChainEntry.header(ChainEntry.VERSION_DELTA));
        SegmentWriter.putUvarint(out, 1);
        byte[] key = "k".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, key.length);
        out.writeBytes(key);
        SegmentWriter.putUvarint(out, 0x7FFFFFFFL);
        out.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("runs");
    }

    @Test
    void aRunCountOnlyMODESTLYTooLargeIsAlsoRefused() {
        // ⚠️ THE SAME ASYMMETRY THIS COMMIT CLOSED ONE FIELD OVER, and both
        // reviewers measured it: with only a 0x7FFFFFFF fixture, a constant
        // sanity cap (`runCount > Integer.MAX_VALUE / 2`) passes the whole
        // suite, and a 20-byte delta claiming 200,000,000 runs then raises
        // `OutOfMemoryError: Java heap space` out of `ChainEntry.decode`. A
        // modest overclaim is the shape a truncated PUT actually produces.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ChainEntry.header(ChainEntry.VERSION_DELTA));
        SegmentWriter.putUvarint(out, 1);
        byte[] key = "k".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, key.length);
        out.writeBytes(key);
        SegmentWriter.putUvarint(out, 9);
        out.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("9 runs");
    }

    @Test
    void aNEGATIVERepeatCountIsRefusedAsAnIoFailureRatherThanEscapingUnchecked() {
        // ⚠️ `Cursor.uvarint` permits a shift up to 63, so a ten-byte varint
        // decodes to a NEGATIVE long -- and neither guard's negative half was
        // pinned. Under a guard that drops it, a negative run count reaches
        // `new ArrayList<>(int)` and `IllegalArgumentException: Illegal
        // Capacity` escapes `ChainEntry.decode` unchecked, past `recover`'s own
        // `throws IOException`. That is the contract escape both guards' own
        // comments cite as their reason for existing.
        ByteArrayOutputStream runs = new ByteArrayOutputStream();
        runs.writeBytes(ChainEntry.header(ChainEntry.VERSION_DELTA));
        SegmentWriter.putUvarint(runs, 1);
        byte[] key = "k".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(runs, key.length);
        runs.writeBytes(key);
        SegmentWriter.putUvarint(runs, -1L);

        assertThatThrownBy(() -> ChainEntry.decode(runs.toByteArray()))
                .as("a negative RUN count")
                .isInstanceOf(java.io.IOException.class);

        ByteArrayOutputStream segs = ChainEntry.kinded(ChainEntry.KIND_DELTA);
        SegmentWriter.putUvarint(segs, 3);
        SegmentWriter.putUvarint(segs, -1L);

        // ⚠️ THE MESSAGE, not just the type, and only for this half. A negative
        // segment count reaches `new ArrayList<>(int)` inside `decodeKinded`'s
        // try, which converts `IllegalArgumentException` to `IOException` — so
        // asserting the type alone cannot tell the guard from its absence, and
        // `count == 0` survived exactly that way. The run count has no such
        // wrapper: `decodeBody` is called outside it, which is why the half
        // above needs no message.
        assertThatThrownBy(() -> ChainEntry.decode(segs.toByteArray()))
                .as("and a negative SEGMENT count is named, not merely refused")
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("-1 segments");
    }

    @Test
    void aBatchedDeltaClaimingNOSegmentsIsRefusedAsCorruptBytes() throws Exception {
        // ⚠️ A zero count is a truncation or a foreign writer, not a delta that
        // commits nothing -- and it must fail as an IOException like every other
        // malformed object, not as an unchecked exception past `recover`'s own
        // `throws IOException` and every caller catching it.
        ByteArrayOutputStream out = ChainEntry.kinded(ChainEntry.KIND_DELTA);
        SegmentWriter.putUvarint(out, 3);
        SegmentWriter.putUvarint(out, 0);

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("0 segments");
    }
}
