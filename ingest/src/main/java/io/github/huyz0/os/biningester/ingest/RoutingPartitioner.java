// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import java.util.Objects;

/**
 * Where a routing value places a record (M6.4, FR-19, ADR-0015 § 1).
 *
 * <p>⚠️ THIS IS THE CLASS M6's HEADLINE RISK IS ABOUT. OpenSearch never sets
 * {@code _routing} on an ingested document — {@code MessageProcessorRunnable}
 * has no such key and {@code IngestPipelineExecutor} forbids mutating it — so a
 * routed search finds a document ONLY because this computation reproduces
 * OpenSearch's own. A divergence is not an exception and not a failed gate: the
 * query simply misses, for that routing value, forever.
 *
 * <p>⚠️ SO IT IS PINNED AGAINST {@code OperationRouting} ITSELF, in
 * {@code plugin}'s test tier — the one module whose tests see both this class
 * and {@code org.opensearch.cluster.routing}. ADR-0015 says so in as many
 * words: "must be tested against {@code OperationRouting}, not reimplemented
 * from memory". A test that recomputed the expected value from this formula
 * would agree with itself and be wrong.
 *
 * <p>⚠️ {@code floorMod}, NEVER {@code %}. A murmur3 hash is a signed
 * {@code int} and about half of all routing values hash negative; {@code %}
 * then yields a negative shard id, which lands every one of them on shard 0
 * (or throws, depending on the caller). That is the single most likely defect
 * in this file and the mutation the parity case exists to kill.
 *
 * <p>⚠️ {@code routingFactor} IS NOT DECORATION. A split index keeps
 * {@code routingNumShards} at its ORIGINAL value and scales the hash down by
 * the factor, so a shard count of 8 grown from 2 hashes over 8 and divides by
 * 1 — while one SPLIT from 2 to 8 hashes over its original 32 and divides by 4.
 * Dropping the division misplaces every record in every split index.
 *
 * <p>⚠️ IT HOLDS NO CLOCK, NO SOCKET AND NO STORE. Hashing a short string is
 * ~50 ns and is not document parsing, which stays forbidden — the routing value
 * arrives out of band in the frame, never extracted from the body
 * (research doc 01 § 2).
 */
public final class RoutingPartitioner {

    private RoutingPartitioner() {
    }

    /**
     * The partition a routing value places a record in.
     *
     * <p>⚠️ THE EXPRESSION IS OPENSEARCH'S, TRANSCRIBED:
     * {@code floorMod(murmur3_32(routing), routingNumShards) / routingFactor},
     * which is {@code OperationRouting.calculateScaledShardId} with a
     * {@code partitionOffset} of zero — and the offset is zero because
     * {@link IndexCatalog#resolveForRouting} refuses an index whose
     * {@code routing_partition_size} exceeds 1, where the offset would depend
     * on a document {@code _id} the ingester does not have.
     *
     * <p>⚠️ AND IT IS THE RIGHT EXPRESSION ONLY FOR THE INDICES ADR-0006 NAMES.
     * {@code OperationRouting.generateShardId} takes a DIFFERENT path for an
     * index with {@code index.number_of_virtual_shards} set, and another for
     * one mid-split: it resolves through a virtual-shard mapping or through
     * {@code getSplitShardsMetadata().getShardIdOfHash}, neither of which this
     * expression reproduces. ⚠️ **THIS CLASS CANNOT DETECT EITHER**, because
     * {@code IndexRegistration} carries no field that says so — so a routed
     * write to such an index is placed by the simple form and every routed
     * query for it returns nothing, with no exception and no metric. ADR-0006
     * states the obligation ("refuse the mode where the simple form does not
     * hold"); **M6.14** is the row that carries it, and until it lands the
     * safe deployment is not to register such an index at all, which makes its
     * writes pend and then be refused rather than silently misplaced.
     */
    public static int partitionFor(IndexRegistration index, String routing) {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(routing, "routing");
        if (index.routingPartitionSize() > 1) {
            throw new PlacementRefusedException("index " + index.indexName()
                    + " sets routing_partition_size=" + index.routingPartitionSize()
                    + ", whose shard depends on the document _id as well as the routing value "
                    + "-- the ingester cannot compute it and must not guess");
        }
        int hash = murmur3(routing);
        return Math.floorMod(hash, index.routingNumShards()) / index.routingFactor();
    }

    /**
     * Lucene's {@code StringHelper.murmurhash3_x86_32} with seed 0, which is
     * what {@code Murmur3HashFunction} calls and therefore what OpenSearch's
     * routing is.
     *
     * <p>⚠️ TRANSCRIBED, NOT INVENTED, and every constant here is load-bearing:
     * the two multipliers, the two rotations, the {@code 5 * h + 0xe6546b64}
     * step, the tail's fall-through, and the final avalanche. A value that is
     * "a hash" but not THIS hash places records consistently on the wrong
     * shards, which is the failure mode that produces no error anywhere.
     *
     * <p>⚠️ SEED 0. A seed of 1 is a different function whose output looks
     * equally random.
     *
     * <p>⚠️ PUBLIC SO THE PARITY CASE CAN COMPARE THE HASH ALONE, in
     * {@code plugin}'s test tier where {@code Murmur3HashFunction} is real.
     * Without that, a broken transcription and a broken {@code floorMod}
     * present identically -- both are 'the partition disagrees' -- and whoever
     * reads the failure starts by bisecting an expression with two suspects.
     */
    public static int murmur3(String routing) {
        // ⚠️ UTF-16 CHARS AS TWO BYTES EACH, LOW BYTE FIRST -- **NOT** UTF-8,
        // and this is the transcription error the parity case caught on its
        // first run. `Murmur3HashFunction.hash(String)` builds
        // `new byte[routing.length() * 2]` and writes `(byte) c` then
        // `(byte) (c >>> 8)` for every char; hashing the UTF-8 bytes instead
        // gives a different number for EVERY value, including pure ASCII,
        // because the zero high bytes change both the length and the tail.
        // ⚠️ AND NOTHING ELSE WOULD HAVE FOUND IT. The formula was right, the
        // constants were right, the arithmetic around it was right, and every
        // record would have landed on a shard no routed query looks at --
        // which is why ADR-0015 requires the expectation to come from
        // `OperationRouting` rather than from a re-derivation.
        byte[] data = new byte[routing.length() * 2];
        for (int i = 0; i < routing.length(); i++) {
            char c = routing.charAt(i);
            data[i * 2] = (byte) c;
            data[i * 2 + 1] = (byte) (c >>> 8);
        }
        return murmur3(data);
    }

    /**
     * ⚠️ PUBLIC FOR THE PARITY CASE OVER ARBITRARY BYTES, which is the only
     * way to reach a 1- or 3-byte tail: the String overload builds an array of
     * {@code length * 2}, always even, so through it {@code data.length & 3}
     * is never 1 or 3 and a transcription error in either branch is invisible.
     */
    public static int murmur3(byte[] data) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int h1 = 0;
        int roundedEnd = data.length & ~3;
        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        int k1 = 0;
        // ⚠️ WRITTEN WITHOUT FALL-THROUGH, and the shape is the compiler's
        // choice rather than the algorithm's: `-Werror` refuses
        // `[fallthrough]`, and a `@SuppressWarnings` on the hash that decides
        // where every routed record lands is a worse trade than three
        // explicit `if`s. The BEHAVIOUR is murmur3's tail exactly -- a
        // three-byte tail contributes all three bytes, a two-byte tail two --
        // and the parity case against `OperationRouting` is what proves the
        // transcription rather than this comment.
        int tail = data.length & 3;
        if (tail == 3) {
            k1 = (data[roundedEnd + 2] & 0xff) << 16;
        }
        if (tail >= 2) {
            k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        }
        if (tail >= 1) {
            k1 |= data[roundedEnd] & 0xff;
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        }
        h1 ^= data.length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
