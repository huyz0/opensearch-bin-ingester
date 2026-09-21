// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What the plugin tells the ingester about an index (M6.2, FR-16).
 *
 * <p>⚠️ EVERY REFUSAL HERE PREVENTS A SILENT WRONG ANSWER RATHER THAN A CRASH.
 * A registration decides where records are placed, and a placement error is a
 * query that misses: no exception, no failed gate, and a document that exists
 * in the cluster and cannot be found. So a frame that is torn, from a future
 * build, or self-contradictory about the split factor is refused rather than
 * read as best it can be.
 */
class IndexRegistrationTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    private static IndexRegistration split() {
        return new IndexRegistration(UUID, "logs-000002", List.of("logs", "logs-write"),
                8, 32, 4, 1);
    }

    @Test
    void aREGISTRATIONRoundTrips() throws Exception {
        IndexRegistration decoded = IndexRegistration.decode(split().encode());

        assertThat(decoded)
                .as("the shape that decides placement survives the wire unchanged")
                .isEqualTo(split());
    }

    @Test
    void anUNSPLITIndexHasAFactorOfONE() {
        IndexRegistration r = IndexRegistration.unsplit(UUID, "logs", 3);

        assertThat(r.routingNumShards())
                .as("an unsplit index keeps routingNumShards at numShards, which is what "
                        + "makes the placement expression reduce to floorMod(hash, numShards)")
                .isEqualTo(3);
        assertThat(r.routingFactor()).isEqualTo(1);
    }

    @Test
    void aROUTINGFactorOfZEROIsREFUSED() {
        assertThatThrownBy(() -> new IndexRegistration(UUID, "logs", List.of(), 4, 4, 0, 1))
                .as("routingFactor DIVIDES -- a zero is a division by zero on the write path "
                        + "of every routed record for this index, discovered there rather than "
                        + "here")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routingFactor");
    }

    @Test
    void aFACTORDisagreeingWithTheSHARDCountsIsREFUSED() {
        assertThatThrownBy(() -> new IndexRegistration(UUID, "logs", List.of(), 8, 32, 2, 1))
                .as("OpenSearch DERIVES the factor as routingNumShards/numShards, so a "
                        + "registration carrying a different one describes an index that "
                        + "cannot exist -- and using it places every record on the wrong shard "
                        + "with nothing to say so")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disagrees");
    }

    @Test
    void aROUTINGShardCountThatIsNOTAMultipleIsREFUSED() {
        assertThatThrownBy(() -> new IndexRegistration(UUID, "logs", List.of(), 3, 8, 2, 1))
                .as("OpenSearch derives routingFactor as routingNumShards/numberOfShards and "
                        + "refuses a split that is not a whole factor -- the power-of-two form "
                        + "is how the DEFAULT is computed, not an invariant, and asserting the "
                        + "stronger rule would refuse a legitimate hand-configured index")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple");
    }

    @Test
    void aBLANKUuidNameOrALIASIsREFUSED() {
        assertThatThrownBy(() -> new IndexRegistration("  ", "logs", List.of(), 1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IndexRegistration(UUID, "", List.of(), 1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IndexRegistration(UUID, "logs", Arrays.asList("ok", " "),
                1, 1, 1, 1))
                .as("a blank alias would resolve every unnamed write to this index")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNONPOSITIVEShardCountIsREFUSED() {
        assertThatThrownBy(() -> new IndexRegistration(UUID, "logs", List.of(), 0, 1, 1, 1))
                .as("an index with no shards has nowhere to place a record, and floorMod by "
                        + "zero throws on the write path")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numShards");
    }

    @Test
    void aTRUNCATEDFrameIsREFUSED() {
        byte[] whole = split().encode();

        for (int cut = 1; cut < whole.length; cut++) {
            byte[] torn = Arrays.copyOf(whole, cut);
            assertThatThrownBy(() -> IndexRegistration.decode(torn))
                    .as("every prefix is refused -- a frame cut at %d of %d bytes that decoded "
                            + "to a SHORTER alias list or a smaller shard count would place "
                            + "records correctly for a shape nobody registered", cut,
                            whole.length)
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void aFORWARDVersionIsREFUSEDAndTheMessageNamesIt() {
        byte[] future = split().encode();
        ByteBuffer.wrap(future).order(ByteOrder.BIG_ENDIAN).putInt(4, 7);

        assertThatThrownBy(() -> IndexRegistration.decode(future))
                .as("a reader that SKIPPED an unknown version would place records against a "
                        + "shape it did not understand, and the result is a query that misses "
                        + "rather than an exception")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("7");
    }

    @Test
    void aWRONGMagicIsREFUSED() {
        byte[] alien = split().encode();
        ByteBuffer.wrap(alien).order(ByteOrder.BIG_ENDIAN).putInt(0, 0x42535542);

        assertThatThrownBy(() -> IndexRegistration.decode(alien))
                .as("a subscription event decoded as a registration would read its session id "
                        + "as an index uuid")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void anALIASCountBiggerThanTheFrameIsREFUSEDBeforeItAllocates() throws Exception {
        byte[] small = IndexRegistration.unsplit(UUID, "logs", 1).encode();
        int at = 8 + 1 + UUID.length() + 1 + "logs".length();
        // ⚠️ A FIVE-BYTE UVARINT CLAIMING ~34 MILLION ALIASES, in a frame with
        // a handful of bytes left. `new ArrayList<>(34_000_000)` is what the
        // unbounded form allocates before reading one of them, which is how
        // every format in this project learned to bound a repeat count --
        // recovery died with an OutOfMemoryError past its own
        // `throws IOException`.
        byte[] lying = new byte[at + 5 + 4];
        System.arraycopy(small, 0, lying, 0, at);
        lying[at] = (byte) 0x80;
        lying[at + 1] = (byte) 0x80;
        lying[at + 2] = (byte) 0x80;
        lying[at + 3] = (byte) 0x80;
        lying[at + 4] = 0x10;

        assertThatThrownBy(() -> IndexRegistration.decode(lying))
                .as("a count is bounded by the bytes that could justify it, and the refusal "
                        + "names BOTH numbers -- an operator holding a torn frame needs to see "
                        + "that 34 million aliases were claimed in a frame of a few bytes, not "
                        + "a generic short read from four fields further on")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("aliases with only");
    }

    @Test
    void aDECODEDInvalidFrameThrowsIOExceptionRatherThanIAE() {
        byte[] bad = new IndexRegistration(UUID, "logs", List.of(), 4, 4, 1, 1).encode();
        // ⚠️ numShards is the third uvarint after the (empty) alias list; zero
        // it to build a frame that decodes structurally and is invalid.
        int at = 8 + 1 + UUID.length() + 1 + "logs".length() + 1;
        bad[at] = 0;

        assertThatThrownBy(() -> IndexRegistration.decode(bad))
                .as("an unchecked throw out of a decode is the shape that kills a poll loop; a "
                        + "caller reading a frame handles a BAD FRAME")
                .isInstanceOf(IOException.class);
    }

    /**
     * A uvarint too large for the field it fills is REFUSED (M6.2, round 1's
     * test major).
     *
     * <p>⚠️ THE CAST IS THE DEFECT, NOT THE SIZE. Without the range check,
     * {@code 0x1_0000_0004} read into {@code numShards} narrows to 4: a
     * structurally valid, semantically wrong registration, accepted with no
     * exception anywhere, placing every record of that index against a shard
     * count nobody configured. That is the silent misplacement this whole
     * format exists to refuse, arriving through arithmetic rather than through
     * a torn frame.
     */
    @Test
    void aNUMERICFieldTooLargeForAnINTIsREFUSED() throws Exception {
        byte[] whole = IndexRegistration.unsplit(UUID, "logs", 4).encode();
        int at = 8 + 1 + UUID.length() + 1 + "logs".length() + 1;
        byte[] huge = new byte[at + 5 + 3];
        System.arraycopy(whole, 0, huge, 0, at);
        // ⚠️ 0x1_0000_0004 as a uvarint: five bytes, and it narrows to 4.
        huge[at] = (byte) 0x84;
        huge[at + 1] = (byte) 0x80;
        huge[at + 2] = (byte) 0x80;
        huge[at + 3] = (byte) 0x80;
        huge[at + 4] = 0x10;
        huge[at + 5] = 4;
        huge[at + 6] = 1;
        huge[at + 7] = 1;

        assertThatThrownBy(() -> IndexRegistration.decode(huge))
                .as("a value that does not fit is refused by NAME, not narrowed -- "
                        + "0x100000004 narrowing to 4 is a registration nobody sent, accepted "
                        + "without a word")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("numShards");
    }

    @Test
    void aSTRINGLengthTooLargeForTheFrameIsREFUSED() throws Exception {
        byte[] whole = IndexRegistration.unsplit(UUID, "logs", 4).encode();
        byte[] lying = new byte[8 + 5 + 8];
        System.arraycopy(whole, 0, lying, 0, 8);
        // ⚠️ THE FIRST STRING'S LENGTH, as a five-byte uvarint above 2^31: the
        // unbounded form casts it to an int, which WRAPS, and the frame is
        // then reinterpreted from a position nothing chose.
        lying[8] = (byte) 0x84;
        lying[9] = (byte) 0x80;
        lying[10] = (byte) 0x80;
        lying[11] = (byte) 0x80;
        lying[12] = 0x10;

        assertThatThrownBy(() -> IndexRegistration.decode(lying))
                .as("a length is bounded by the bytes that could justify it BEFORE it becomes "
                        + "an index into the array -- and the message is the registration's "
                        + "own, not the cursor's: without the bound the wrapped cast reaches "
                        + "`Cursor.bytes`, which also refuses but says nothing about WHICH "
                        + "field lied, so the case would pass while measuring the cursor")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("string with only");
    }

    @Test
    void aFrameWithTRAILINGBytesIsREFUSED() throws Exception {
        byte[] whole = IndexRegistration.unsplit(UUID, "logs", 4).encode();
        byte[] extra = Arrays.copyOf(whole, whole.length + 3);

        assertThatThrownBy(() -> IndexRegistration.decode(extra))
                .as("trailing bytes are a different shape, two frames run together, or a torn "
                        + "stream -- the one thing they are not is this registration, and "
                        + "accepting the prefix places records against a shape nobody sent")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailing");
    }

    @Test
    void aROUTINGPartitionSizeNotSMALLERThanTheShardCountIsREFUSED() {
        assertThatThrownBy(() -> new IndexRegistration(UUID, "logs", List.of(), 4, 4, 1, 4))
                .as("OpenSearch requires routing_partition_size < number_of_shards, since it "
                        + "names how many shards one routing value may spread across -- a "
                        + "larger one describes an index that cannot exist")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routingPartitionSize");
        assertThatThrownBy(() -> new IndexRegistration(UUID, "logs", List.of(), 4, 4, 1, 0))
                .as("and zero is refused at the other end")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theALIASListIsCOPIEDNotAliased() {
        List<String> mutable = new java.util.ArrayList<>(List.of("logs"));
        IndexRegistration r = new IndexRegistration(UUID, "logs-000001", mutable, 1, 1, 1, 1);

        mutable.add("logs-write");

        assertThat(r.aliases())
                .as("a caller mutating the list it handed over must not change which names "
                        + "resolve to this index -- the catalog reads these to place records")
                .containsExactly("logs");
    }
}
