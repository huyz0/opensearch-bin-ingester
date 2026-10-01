// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * What the plugin tells the ingester about one index (M6.2, FR-16, ADR-0015 §2).
 *
 * <p>⚠️ V2 ADDS THE THREE FAST-MODE SETTINGS (M13.23, ADR-0082 §1):
 * {@code flushTimerMillis}, {@code wal} and {@code walQuorum}. An index at all
 * three defaults still ENCODES AS V1, so an older ingester -- which refuses an
 * unknown version -- reads every index that sets none of them, and a rolling
 * upgrade works whichever side moves first; only an index that sets one needs
 * the newer ingester. With {@code wal} off the quorum is not on the wire, so
 * it is normalised to its default here, and equality means the same bytes:
 * the registrar diffs by equality, and a quorum the wire drops would read as a
 * change it re-pushes forever.
 *
 * <p>⚠️ IT CARRIES WHAT PLACEMENT NEEDS AND THE THREE FAST-MODE SETTINGS, AND
 * NOTHING ELSE. ADR-0015 §2's payload also lists {@code replicationMode},
 * {@code allActive} and {@code lanes[]}; a field with no reader is a field
 * nothing can be wrong about. Adding one is another wire-format change.
 *
 * <p>⚠️ THE PRODUCER NEVER SEES ANY OF THIS. ADR-0015 rejected an
 * unauthenticated endpoint on the plugin and rejected putting OpenSearch
 * credentials in every producer; the plugin runs INSIDE the node, already holds
 * an authenticated subscription, and pushes over it. So the shard count reaches
 * the ingester without a new endpoint, a new credential, or a producer that has
 * to know the cluster's topology.
 *
 * <p>⚠️ {@code routingFactor} DIVIDES, so it is refused at zero. A split index
 * keeps {@code routingNumShards} at its ORIGINAL value and OpenSearch scales
 * the hash down by this factor — {@code OperationRouting.calculateScaledShardId}
 * — so for an unsplit index it is 1 and the expression reduces to
 * {@code floorMod(murmur3_32(routing), numShards)}. A zero here is not a
 * degenerate case to tolerate; it is a division by zero on the write path of
 * every routed record for that index.
 *
 * <p>⚠️ {@code routingPartitionSize} IS CARRIED SO IT CAN BE REFUSED. With
 * OpenSearch's built-in tenant partitioning the shard depends on the routing
 * value AND the document {@code _id} ({@code OperationRouting:587}), which the
 * ingester does not have — so a ROUTED write to such an index cannot be placed
 * correctly and must be refused rather than hashed anyway. ⚠️ The INDEX is not
 * refused: an explicit-partition write needs no hash and is unaffected
 * (SPI § 4b). Where that refusal happens is M6.3's; this format is what makes
 * it possible at all.
 *
 * <p>⚠️ THE INDEX UUID IS A STRING HERE, not a {@link java.util.UUID}, and that
 * is deliberate. An OpenSearch index UUID is BASE64URL — {@code UUID.fromString}
 * throws on every real one, which
 * {@code BinStoreConsumerFactory.indexUuidOf} records finding the hard way, on
 * a booting node, because every in-process test built a {@code java.util.UUID}
 * directly. What travels here is what the cluster state actually says; turning
 * it into stream identity is the reader's job and is already written down.
 */
public record IndexRegistration(String indexUuid, String indexName, List<String> aliases,
        int numShards, int routingNumShards, int routingFactor, int routingPartitionSize,
        long flushTimerMillis, boolean wal, int walQuorum) {

    /** {@code "BIRG"} — big-endian, so an operator sees it in a hex dump. */
    public static final int MAGIC = 0x42495247;

    /** The placement-only shape. */
    public static final int VERSION_1 = 1;

    /** V1 plus the three fast-mode settings (ADR-0082 §1). */
    public static final int VERSION_2 = 2;

    /** {@code index.ingestion_source.param.flush_timer}'s default, 5 s. */
    public static final long DEFAULT_FLUSH_TIMER_MILLIS = 5_000;

    /** {@code index.ingestion_source.param.wal_quorum}'s default. */
    public static final int DEFAULT_WAL_QUORUM = 2;

    /** A registration at the three settings' defaults (a v1 sender's indices). */
    public IndexRegistration(String indexUuid, String indexName, List<String> aliases,
            int numShards, int routingNumShards, int routingFactor, int routingPartitionSize) {
        this(indexUuid, indexName, aliases, numShards, routingNumShards, routingFactor,
                routingPartitionSize, DEFAULT_FLUSH_TIMER_MILLIS, false, DEFAULT_WAL_QUORUM);
    }

    /** This registration with the three fast-mode settings replaced. */
    public IndexRegistration withFastSettings(long flushTimerMillis, boolean wal, int walQuorum) {
        return new IndexRegistration(indexUuid, indexName, aliases, numShards, routingNumShards,
                routingFactor, routingPartitionSize, flushTimerMillis, wal, walQuorum);
    }

    public IndexRegistration {
        requireText(indexUuid, "indexUuid");
        requireText(indexName, "indexName");
        Objects.requireNonNull(aliases, "aliases");
        aliases = List.copyOf(aliases);
        for (String alias : aliases) {
            requireText(alias, "alias");
        }
        requirePositive(numShards, "numShards");
        requirePositive(routingNumShards, "routingNumShards");
        // ⚠️ IT DIVIDES. See the class javadoc: a zero is a division by zero on
        // the write path of every routed record for this index, and the write
        // path is where it would be discovered.
        requirePositive(routingFactor, "routingFactor");
        requirePositive(routingPartitionSize, "routingPartitionSize");
        if (flushTimerMillis <= 0) {
            // ⚠️ A ZERO DEADLINE IS AN UPLOAD PER RECORD, not a setting, and a
            // negative one is a timer that has always already fired.
            throw new IllegalArgumentException("flushTimerMillis is never " + flushTimerMillis
                    + " for " + indexName);
        }
        if (!wal) {
            // ⚠️ NOT ON THE WIRE WITHOUT WAL (ADR-0082 §1), so not in the value
            // either -- see the class javadoc on equality.
            walQuorum = DEFAULT_WAL_QUORUM;
        } else if (walQuorum < 1 || walQuorum > 3) {
            throw new IllegalArgumentException("walQuorum " + walQuorum + " for " + indexName
                    + " is not 1, 2 or 3 -- the fleet spans at most three AZs");
        }
        if (routingPartitionSize > 1 && routingPartitionSize >= numShards) {
            // ⚠️ OPENSEARCH REFUSES IT TOO: `routing_partition_size` must be
            // strictly less than the shard count, because it names how many
            // shards one routing value may spread across. A frame carrying a
            // larger one describes an index that cannot exist, and this record
            // exists so that shape is refused where it arrives rather than
            // where it would be divided by.
            throw new IllegalArgumentException("routingPartitionSize " + routingPartitionSize
                    + " is not less than numShards " + numShards + " for " + indexName);
        }
        if (routingNumShards % numShards != 0) {
            // ⚠️ A MULTIPLE, WHICH IS WHAT OPENSEARCH ENFORCES. `IndexMetadata`
            // derives `routingFactor = routingNumShards / numberOfShards` and
            // refuses a split that is not a whole factor; the DEFAULT
            // `routingNumShards` is a power-of-two multiple, but that is how
            // the default is computed rather than an invariant -- an earlier
            // version of this comment asserted the stronger rule, which would
            // have refused a legitimate index configured by hand.
            throw new IllegalArgumentException("routingNumShards " + routingNumShards
                    + " is not a multiple of numShards " + numShards + " for " + indexName);
        }
        if (routingNumShards / numShards != routingFactor) {
            throw new IllegalArgumentException("routingFactor " + routingFactor + " for "
                    + indexName + " disagrees with routingNumShards/numShards ("
                    + routingNumShards + "/" + numShards + " = " + routingNumShards / numShards
                    + ") -- OpenSearch derives the factor, so a disagreement here is a "
                    + "placement that silently misses");
        }
    }

    /** An unsplit index with no aliases: {@code routingNumShards == numShards}. */
    public static IndexRegistration unsplit(String indexUuid, String indexName, int numShards) {
        return new IndexRegistration(indexUuid, indexName, List.of(), numShards, numShards, 1, 1);
    }

    private static void requireText(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " is never blank");
        }
    }

    private static void requirePositive(int value, String what) {
        if (value <= 0) {
            throw new IllegalArgumentException(what + " is never " + value);
        }
    }

    /** Whether all three fast-mode settings hold their defaults. */
    private boolean atFastDefaults() {
        return flushTimerMillis == DEFAULT_FLUSH_TIMER_MILLIS && !wal;
    }

    /**
     * These bytes: {@link #VERSION_1} for an index at the three defaults,
     * {@link #VERSION_2} otherwise (see the class javadoc).
     */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        boolean v1 = atFastDefaults();
        head.putInt(MAGIC);
        head.putInt(v1 ? VERSION_1 : VERSION_2);
        out.writeBytes(head.array());
        putString(out, indexUuid);
        putString(out, indexName);
        SegmentWriter.putUvarint(out, aliases.size());
        for (String alias : aliases) {
            putString(out, alias);
        }
        SegmentWriter.putUvarint(out, numShards);
        SegmentWriter.putUvarint(out, routingNumShards);
        SegmentWriter.putUvarint(out, routingFactor);
        SegmentWriter.putUvarint(out, routingPartitionSize);
        if (!v1) {
            out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
                    .putLong(flushTimerMillis).array());
            out.write(wal ? 1 : 0);
            if (wal) {
                out.write(walQuorum);
            }
        }
        return out.toByteArray();
    }

    /**
     * Reads one registration.
     *
     * <p>⚠️ AN UNKNOWN VERSION STOPS; IT DOES NOT SKIP. A reader that skipped
     * would place records against a shape it did not understand, and a
     * placement error is a query that misses rather than an exception — the
     * failure mode M6 exists to prevent.
     *
     * <p>⚠️ AND THE ALIAS COUNT IS BOUNDED BY THE BYTES REMAINING before it
     * becomes an allocation, which is {@code Cursor.remaining}'s whole reason:
     * a torn frame claiming {@code 0x7FFFFFFF} aliases sized a list before
     * reading one, and recovery died with an {@code OutOfMemoryError} past its
     * own {@code throws IOException}.
     */
    public static IndexRegistration decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor c = new Cursor(bytes, 0, "index registration");
        int magic = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (magic != MAGIC) {
            throw new IOException("not an index registration: magic 0x"
                    + Integer.toHexString(magic));
        }
        int version = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (version != VERSION_1 && version != VERSION_2) {
            throw new IOException("index registration version " + version
                    + " is not readable by this build, which knows " + VERSION_1 + " and "
                    + VERSION_2
                    + " -- refusing rather than guessing at a shape that decides placement");
        }
        String indexUuid = getString(c);
        String indexName = getString(c);
        long aliasCount = c.uvarint();
        if (aliasCount < 0 || aliasCount > c.remaining()) {
            throw new IOException("index registration claims " + aliasCount
                    + " aliases with only " + c.remaining() + " bytes left");
        }
        List<String> aliases = new java.util.ArrayList<>((int) aliasCount);
        for (long i = 0; i < aliasCount; i++) {
            aliases.add(getString(c));
        }
        int numShards = toInt(c.uvarint(), "numShards");
        int routingNumShards = toInt(c.uvarint(), "routingNumShards");
        int routingFactor = toInt(c.uvarint(), "routingFactor");
        int routingPartitionSize = toInt(c.uvarint(), "routingPartitionSize");
        long flushTimerMillis = DEFAULT_FLUSH_TIMER_MILLIS;
        boolean wal = false;
        int walQuorum = DEFAULT_WAL_QUORUM;
        if (version == VERSION_2) {
            flushTimerMillis = ByteBuffer.wrap(c.bytes(8)).order(ByteOrder.BIG_ENDIAN).getLong();
            int walByte = c.bytes(1)[0];
            if (walByte != 0 && walByte != 1) {
                // ⚠️ STOP, DO NOT GUESS: a wal byte of 2 is a shape this build
                // does not know, and reading it as either mode picks a
                // durability nobody chose.
                throw new IOException("index registration wal byte " + walByte
                        + " is neither 0 nor 1");
            }
            wal = walByte == 1;
            if (wal) {
                walQuorum = c.bytes(1)[0];
            }
        }
        if (!c.atEnd()) {
            // ⚠️ TRAILING BYTES ARE A REFUSAL, NOT SLACK. A frame with more
            // after the last field is either a different shape this build does
            // not know, two frames run together, or a torn stream -- and the
            // one thing it is not is this registration. Accepting the prefix
            // would place records against a shape nobody sent.
            throw new IOException("index registration has " + c.remaining()
                    + " trailing bytes after its last field");
        }
        try {
            return new IndexRegistration(indexUuid, indexName, aliases, numShards,
                    routingNumShards, routingFactor, routingPartitionSize, flushTimerMillis, wal,
                    walQuorum);
        } catch (IllegalArgumentException refused) {
            // ⚠️ AN IOException, NOT AN IAE, because this arrived over a wire:
            // a caller reading a frame handles a bad frame, and an unchecked
            // throw out of a decode is the shape that kills a poll loop.
            throw new IOException("index registration is not valid: " + refused.getMessage(),
                    refused);
        }
    }

    private static int toInt(long value, String what) throws IOException {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IOException(what + " " + value + " does not fit an int");
        }
        return (int) value;
    }

    private static void putString(ByteArrayOutputStream out, String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, utf8.length);
        out.writeBytes(utf8);
    }

    private static String getString(Cursor c) throws IOException {
        long length = c.uvarint();
        if (length < 0 || length > c.remaining()) {
            throw new IOException("index registration claims a " + length
                    + "-byte string with only " + c.remaining() + " bytes left");
        }
        return new String(c.bytes((int) length), StandardCharsets.UTF_8);
    }
}
