// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.Checkpoint.StreamOffsets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The checkpoint record and its codec (M4.8a).
 *
 * <p>⚠️ ROUND-TRIP PLUS GOLDEN FILES IS NOT ENOUGH, which is why this file
 * exists beside {@link GoldenCheckpointTest} rather than instead of it. Two
 * mutations survive that pairing as naively written, and each has its own test
 * here: deleting the encoder's ORDERING, and swapping {@code nextOffset} with
 * {@code oldestRetainedOffset} CONSISTENTLY in encode and decode — which
 * round-trip cannot see by construction.
 */
class CheckpointTest {

    private static UUID idx(int n) {
        return new UUID(0x1111_2222_3333_4444L, n);
    }

    /**
     * Eight streams and three pods, every field a distinct non-default value.
     *
     * <p>⚠️ EIGHT, NOT SIX, AND THE EXTRA TWO ARE LOAD-BEARING: they share
     * an {@code indexId} and differ only by partition, which is the only
     * shape that pins the partitionId tiebreak in the encoder's total
     * order. Everything else here co-varies indexId with partitionId, so a
     * comparator sorting by indexId alone would survive. Do not cut this
     * fixture back to six -- the golden file is a SEPARATE six-stream
     * artifact and does not track this one.
     *
     * <p>⚠️ SIX IS A FLOOR, not a magic number: M4.1 measured a broken sort at
     * THREE keys still producing the asserted order about 1 run in 6, and only
     * six keys drove that to 8 of 8 fresh JVMs failing. ⚠️ AND EVERY FIELD
     * DIFFERS FROM EVERY OTHER, because a golden file catches a consistent
     * field swap only if the two fields it swaps are distinguishable —
     * {@code oldestRetainedOffset} is exactly the field a lazy fixture leaves
     * at 0.
     */
    static Checkpoint fixture() {
        Map<RunKey, StreamOffsets> streams = new LinkedHashMap<>();
        // ⚠️ INSERTED IN DESCENDING KEY ORDER. The encoder must emit ascending,
        // so a deleted sort is caught DETERMINISTICALLY rather than at 1 in n!.
        for (int n = 6; n >= 1; n--) {
            streams.put(new RunKey(idx(n), n), new StreamOffsets(1000L * n + n, 100L * n));
        }
        // ⚠️ TWO PARTITIONS OF ONE INDEX, inserted higher-partition-first,
        // because everything above co-varies indexId with partitionId -- so no
        // two streams shared an indexId and the partitionId TIEBREAK was
        // unpinned. Measured surviving mutation: sorting by `indexId` alone,
        // which is a partial order over exactly the shape the system produces,
        // one index with many partitions, and lets identical facts encode
        // differently by insertion order.
        streams.put(new RunKey(idx(3), 9), new StreamOffsets(3009, 309));
        streams.put(new RunKey(idx(3), 4), new StreamOffsets(3004, 304));
        Map<String, Long> pods = new LinkedHashMap<>();
        pods.put("podb", 77L);
        pods.put("poda", 42L);
        // ⚠️ ONE NON-ASCII podId, because every other one is ASCII and
        // `putUvarint(out, raw.length)` -> `putUvarint(out, pod.length())`
        // survives when bytes and chars agree.
        pods.put("pod-\u00e9", 5L);
        return new Checkpoint(9, streams, pods);
    }

    @Test
    void everyFieldSurvivesARoundTrip() throws Exception {
        Checkpoint before = fixture();

        Checkpoint after = Checkpoint.decode(before.encode());

        assertThat(after.sequence()).as("the sequence").isEqualTo(9);
        assertThat(after.streams()).as("every stream, with both of its offsets")
                .isEqualTo(before.streams());
        assertThat(after.pods()).as("every pod's lastAppliedFlushSeq")
                .isEqualTo(before.pods());
    }

    @Test
    void theENCODERSortsWhateverOrderTheCallerHandedIn() throws Exception {
        // ⚠️ THE MUTATION THIS KILLS is deleting the encoder's sort. The fixture
        // inserts streams in DESCENDING key order and pods in descending podId
        // order, and the record preserves that order deliberately, so an
        // encoder that simply iterates emits descending and fails here every
        // run -- not 719 runs in 720.
        byte[] bytes = fixture().encode();

        assertThat(streamKeysInEncodedOrder(bytes))
                .as("streams are encoded in ascending RunKey order")
                .isSorted()
                .hasSize(8);
        assertThat(podIdsInEncodedOrder(bytes))
                .as("pods are encoded in ascending podId order")
                .containsExactly("pod-\u00e9", "poda", "podb");
    }

    @Test
    void twoCheckpointsDifferingONLYInInsertionOrderEncodeIdentically() throws Exception {
        Map<RunKey, StreamOffsets> ascending = new LinkedHashMap<>();
        for (int n = 1; n <= 6; n++) {
            ascending.put(new RunKey(idx(n), n), new StreamOffsets(1000L * n + n, 100L * n));
            if (n == 3) {
                ascending.put(new RunKey(idx(3), 4), new StreamOffsets(3004, 304));
                ascending.put(new RunKey(idx(3), 9), new StreamOffsets(3009, 309));
            }
        }
        Map<String, Long> pods = new LinkedHashMap<>();
        pods.put("pod-\u00e9", 5L);
        pods.put("poda", 42L);
        pods.put("podb", 77L);

        byte[] fromAscending = new Checkpoint(9, ascending, pods).encode();

        assertThat(fromAscending)
                .as("the bytes are a function of the CONTENT, not of how it was built")
                .isEqualTo(fixture().encode());
    }

    // ⚠️ A TEST ASSERTING THE FIXTURE'S DISCRIMINATING POWER WAS WRITTEN AND
    // DELETED. It checked that `oldestRetainedOffset` is never 0 and never
    // equals `nextOffset`, which is the property that lets a golden file catch
    // a CONSISTENT swap of the two. Two reasons it is gone rather than kept.
    // ⚠️ IT CANNOT BE RED-RECORDED: the only mutation that fails it is a change
    // to the fixture in THIS file, and `tdd-red.sh` binds a red record to the
    // sha256 of the file it was observed in -- so restoring the fixture
    // invalidates the record it was just given. A test that cannot be observed
    // failing on demand cannot honour testing.md's red-record rule.
    // ⚠️ AND IT IS REDUNDANT AS A DETECTOR, though not for the reason an
    // earlier draft gave. Swapping the two fields consistently in encode AND
    // decode is killed FIRST by `StreamOffsets`'s own `oldest <= next`
    // invariant, not by any assertion. Re-measured with that invariant also
    // removed -- which is the only way to test the prescription rather than a
    // lucky guard -- the swap is killed by
    // `GoldenCheckpointTest#aV0CheckpointFromAnEarlierBuildStillParses` alone,
    // while both byte-identity tests go green. Byte identity cannot see a
    // consistent swap; only the hand-verified VALUES can. And that test pins
    // the values as literals against committed bytes without ever calling this
    // fixture, so the property needs no separate pinning here. The property survives as
    // the fixture's own documented contract above.

    @Test
    void aCheckpointAt1600StreamsStaysNearItsSTATEDSize() throws Exception {
        // ⚠️ THE MAGNITUDE IS THE DISCRIMINATOR. "The index stays in the deltas"
        // has no other constructible expression while nothing writes a
        // checkpoint, and a bound of merely "< 1 MiB" is green against an
        // encoder that inlines the offset->segment index. The row states
        // ~38 KiB at 1,600 streams; this brackets that rather than bounding it
        // loosely on one side.
        Map<RunKey, StreamOffsets> streams = new LinkedHashMap<>();
        for (int n = 0; n < 1600; n++) {
            streams.put(new RunKey(idx(n), n), new StreamOffsets(500_000L + n, 1_000L + n));
        }
        Map<String, Long> pods = new LinkedHashMap<>();
        pods.put("poda", 1L);

        int size = new Checkpoint(1, streams, pods).encode().length;

        assertThat(size).as("1,600 streams is tens of KiB, not hundreds").isLessThan(64 * 1024);
        assertThat(size).as("and the fixture really carries 1,600 streams")
                .isGreaterThan(20 * 1024);
    }

    @Test
    void anOldestRetainedOffsetPastNextOffsetIsRefused() {
        assertThatThrownBy(() -> new StreamOffsets(5, 6))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("past nextOffset");
    }


    @Test
    void theRecordREFUSESValuesNoEncoderShouldEverProduce() {
        // ⚠️ THE FULL MESSAGE, not a substring of it: `hasMessageContaining
        // ("nextOffset")` passed with the negative guard DELETED, because the
        // cross-field check then fires and its message says "is past
        // nextOffset". Measured.
        assertThatThrownBy(() -> new StreamOffsets(-1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nextOffset is never negative");
        assertThatThrownBy(() -> new StreamOffsets(5, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oldestRetainedOffset");
        assertThatThrownBy(() -> new Checkpoint(-1, Map.of(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sequence");
        assertThatThrownBy(() -> new Checkpoint(1, Map.of(), Map.of(" ", 1L)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("never blank");
        assertThatThrownBy(() -> new Checkpoint(1, Map.of(), Map.of("poda", -1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lastAppliedFlushSeq");
        // ⚠️ A NEGATIVE ROUND-TRIPS CLEANLY as a 10-byte varint, so nothing
        // downstream would catch what these refuse.
    }

    @Test
    void aNullKeyOrValueInEitherMapIsRefusedAtConstruction() {
        // ⚠️ BOTH MAPS. `pods` was validated entry by entry and `streams` was
        // not, so a `LinkedHashMap` copy accepted what `Map.copyOf` refuses and
        // the NPE surfaced later, inside `encode()`, on the write path.
        Map<RunKey, StreamOffsets> nullValue = new LinkedHashMap<>();
        nullValue.put(new RunKey(idx(1), 1), null);
        assertThatThrownBy(() -> new Checkpoint(1, nullValue, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("stream offsets");

        Map<RunKey, StreamOffsets> nullKey = new LinkedHashMap<>();
        nullKey.put(null, new StreamOffsets(5, 1));
        assertThatThrownBy(() -> new Checkpoint(1, nullKey, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("stream key");
    }

    @Test
    void theMapsAreCOPIEDSoALaterMutationCannotReachTheRecord() {
        Map<RunKey, StreamOffsets> streams = new LinkedHashMap<>();
        streams.put(new RunKey(idx(1), 1), new StreamOffsets(5, 1));
        Map<String, Long> pods = new LinkedHashMap<>();
        pods.put("poda", 1L);
        Checkpoint c = new Checkpoint(1, streams, pods);

        streams.put(new RunKey(idx(2), 2), new StreamOffsets(9, 2));
        pods.put("podz", 9L);

        assertThat(c.streams()).as("the record kept its own copy").hasSize(1);
        assertThat(c.pods()).hasSize(1);
        assertThatThrownBy(() -> c.streams().put(new RunKey(idx(3), 3), new StreamOffsets(1, 1)))
                .isInstanceOf(UnsupportedOperationException.class);
        // ⚠️ BOTH MAPS, because the name says "the maps" and an earlier version
        // pinned only `streams()`: dropping `unmodifiableMap` from `pods` left
        // the suite green and `pods().put(...)` succeeding.
        assertThatThrownBy(() -> c.pods().put("podz", 9L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static List<RunKey> streamKeysInEncodedOrder(byte[] bytes) throws IOException {
        Checkpoint decoded = Checkpoint.decode(bytes);
        // ⚠️ Decode preserves the ENCODED order, because the record keeps the
        // order it is handed and the decoder reads front to back.
        return new ArrayList<>(decoded.streams().keySet());
    }

    private static List<String> podIdsInEncodedOrder(byte[] bytes) throws IOException {
        return new ArrayList<>(Checkpoint.decode(bytes).pods().keySet());
    }
}
