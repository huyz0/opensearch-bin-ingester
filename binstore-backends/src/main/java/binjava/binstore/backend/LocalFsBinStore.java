// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.CostTable;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link BinStore} over a local directory. The M1 durable backend
 * (ADR-0018's walking skeleton) and the fixture every higher tier runs against.
 *
 * <p>⚠️ ONE OBJECT IS TWO FILES IN A FLAT DIRECTORY, named by the SHA-256 of
 * the key. Keys are NOT mapped onto paths: a key is a byte range, not a tree, so
 * {@code p/a} and {@code p/a/x} are both legal and no filesystem can hold both —
 * the first would have to be a file and a directory at once. The conformance
 * suite asserts those two keys together for exactly that reason.
 *
 * <p>⚠️ Percent-escaping into one filename was tried first and FAILS on
 * NAME_MAX: a 1024-byte key escapes to at least 1024 characters and ext4 refuses
 * anything over 255 bytes per component. A fixed-width hash sidesteps the limit
 * for any key the SPI admits, which is why {@code maxKeyBytes} can be a number
 * this backend chooses rather than one the filesystem imposes.
 *
 * <p>⚠️ The sidecar carries the KEY and the VERSION, so listing reads sidecars
 * and sorts by the real key — a hash is not order-preserving, and recovery walks
 * the listing in key order. On a local filesystem nothing is billed, so the scan
 * costs nothing the cost model counts (R2 confines LIST to recovery and GC).
 */
public final class LocalFsBinStore implements BinStore {

    private static final long MAX_KEY_BYTES = 1024;
    private static final String VERSION_SUFFIX = ".v";

    private final Path root;
    private final AtomicLong versions = new AtomicLong();

    public LocalFsBinStore(Path root) throws IOException {
        this.root = root;
        Files.createDirectories(root);
    }

    /** ⚠️ Fixed width, so no key length can breach NAME_MAX. */
    private static String encode(String key) {
        try {
            byte[] d = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(key.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(64);
            for (byte b : d) {
                out.append(String.format("%02x", b));
            }
            return out.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JDK", e);
        }
    }

    private Path pathFor(String key) {
        return root.resolve(encode(key));
    }

    private void checkKey(String key) throws IOException {
        // ⚠️ BYTES. NAME_MAX is a byte limit, and this is the backend that
        // answers to it; counting characters passes an ASCII probe and then
        // refuses nothing for a key of multi-byte characters.
        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IOException("key longer than " + MAX_KEY_BYTES + " bytes");
        }
    }

    /**
     * ⚠️ The sidecar holds the KEY and the VERSION. The key, because the data
     * file is named by a hash and listing must report real keys in key order.
     * The version, because a filesystem has no ETag. // SKELETON: S3 and GCS
     * carry one natively; deriving it from mtime would be UNSTABLE across calls,
     * which the conformance suite refuses outright.
     */
    private Version writeMeta(Path data, String key) throws IOException {
        Version v = new Version(Long.toString(versions.incrementAndGet()));
        Files.writeString(Path.of(data + VERSION_SUFFIX), v.token() + "\n" + key,
                StandardCharsets.UTF_8);
        return v;
    }

    private record Meta(Version version, String key) {}

    private Meta readMeta(Path data) throws IOException {
        try {
            String[] parts = Files.readString(Path.of(data + VERSION_SUFFIX)).split("\n", 2);
            return new Meta(new Version(parts[0].strip()), parts.length > 1 ? parts[1] : "");
        } catch (NoSuchFileException e) {
            // A crash between the data write and the sidecar. Size is a poor
            // version but a stable one, and M1 never uses it for CAS.
            return new Meta(new Version("size-" + Files.size(data)), "");
        }
    }

    @Override
    public InputStream get(String key) throws IOException {
        return Files.newInputStream(pathFor(key));
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        Path p = pathFor(key);
        long size = Files.size(p);
        if (start < 0 || endIncl < start) {
            throw new IOException("range [" + start + "," + endIncl + "] is not a range");
        }
        if (start >= size) {
            // ⚠️ NOT a short read. RandomAccessFile.seek past EOF then read
            // yields zero bytes, which a segment reader parses as a valid empty
            // run instead of reporting the mistake.
            throw new IOException("range starts at " + start + " but " + key + " is " + size);
        }
        long last = Math.min(endIncl, size - 1);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        ch.position(start);
        return new java.io.FilterInputStream(java.nio.channels.Channels.newInputStream(ch)) {
            private long left = last - start + 1;

            @Override
            public int read() throws IOException {
                if (left <= 0) {
                    return -1;
                }
                int b = super.read();
                if (b >= 0) {
                    left--;
                }
                return b;
            }

            @Override
            public int read(byte[] buf, int off, int len) throws IOException {
                if (left <= 0) {
                    return -1;
                }
                int n = super.read(buf, off, (int) Math.min(len, left));
                if (n > 0) {
                    left -= n;
                }
                return n;
            }
        };
    }

    @Override
    public Optional<ObjectStat> stat(String key) throws IOException {
        Path p = pathFor(key);
        if (!Files.exists(p)) {
            return Optional.empty();
        }
        return Optional.of(new ObjectStat(key, Files.size(p), readMeta(p).version()));
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        checkKey(key);
        Path target = pathFor(key);
        // ⚠️ TEMP-THEN-RENAME. Streaming into the final path destroys the
        // previous object before the length is checked, so a REFUSED write would
        // lose data the caller was told had not been written. The rename is
        // atomic, so O_TRUNC semantics come for free: no stale tail survives a
        // shorter overwrite.
        Path tmp = Files.createTempFile(root, "put", ".tmp");
        try {
            try (InputStream in = body.checkedStream()) {
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
            }
            moveAtomically(tmp, target);
            return writeMeta(target, key);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        checkKey(key);
        Path target = pathFor(key);
        // ⚠️ CREATE_NEW is O_CREAT|O_EXCL: the kernel decides the race, not us.
        // Files.exists() then write is the shape the conformance suite's
        // concurrency case exists to refuse -- two writers both observe absence
        // and the loser silently overwrites the winner's record (invariant I1).
        try (var ch = FileChannel.open(target, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            try (InputStream in = body.checkedStream()) {
                in.transferTo(java.nio.channels.Channels.newOutputStream(ch));
            }
        } catch (java.nio.file.FileAlreadyExistsException lostTheRace) {
            return Optional.empty();
        } catch (IOException badBody) {
            // The body was refused after the exclusive create won. Nothing was
            // here before -- putIfAbsent only writes a free key -- so removing
            // the partial file restores the state the caller expects.
            Files.deleteIfExists(target);
            throw badBody;
        }
        return Optional.of(writeMeta(target, key));
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException {
        checkKey(key);
        java.util.Objects.requireNonNull(expected, "expected");
        Path target = pathFor(key);
        // ⚠️ SERIALIZED per key. A filesystem has no native compare-and-swap the
        // way S3's If-Match or GCS's generation-match do, so within this one JVM
        // -- which is all this dev/test backend ever runs as (ADR-0008's
        // addendum: this is a fixture, not a production backend) -- a monitor
        // per key gives the read-check-write the same atomicity putIfAbsent gets
        // for free from O_CREAT|O_EXCL.
        synchronized (lockFor(key)) {
            if (!Files.exists(target)) {
                // ⚠️ NOT empty: see MemoryBinStore's identical case. A lease
                // renewal or registry update against a key that was never
                // written is a different failure than one that lost a race.
                throw new IOException("no such key: " + key + " (nothing to match version against)");
            }
            Meta current = readMeta(target);
            if (!current.version().equals(expected)) {
                // ⚠️ The body is not read at all on this path -- the write was
                // already known stale before a byte of it mattered, matching
                // putIfAbsent's own "empty means already occupied" cost shape.
                return Optional.empty();
            }
            Path tmp = Files.createTempFile(root, "put", ".tmp");
            try {
                try (InputStream in = body.checkedStream()) {
                    Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                }
                moveAtomically(tmp, target);
                return Optional.of(writeMeta(target, key));
            } finally {
                Files.deleteIfExists(tmp);
            }
        }
    }

    private final java.util.concurrent.ConcurrentHashMap<String, Object> keyLocks =
            new java.util.concurrent.ConcurrentHashMap<>();

    private Object lockFor(String key) {
        return keyLocks.computeIfAbsent(key, k -> new Object());
    }

    private static void moveAtomically(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
        // ⚠️ Sorted by DECODED key: the escape is not order-preserving, and
        // recovery walks this in key order.
        TreeMap<String, Path> byKey = new TreeMap<>();
        List<Path> dataFiles = new ArrayList<>();
        try (var entries = Files.list(root)) {
            entries.forEach(p -> {
                String name = p.getFileName().toString();
                if (!name.endsWith(VERSION_SUFFIX) && !name.endsWith(".tmp")) {
                    dataFiles.add(p);
                }
            });
        }
        for (Path p : dataFiles) {
            byKey.put(readMeta(p).key(), p);
        }
        String from = startAfter == null || startAfter.compareTo(prefix) < 0 ? prefix : startAfter;
        var tail = startAfter == null || startAfter.compareTo(prefix) < 0
                ? byKey.tailMap(from, true)
                : byKey.tailMap(from, false);
        List<ObjectStat> page = new ArrayList<>();
        String next = null;
        for (var e : tail.entrySet()) {
            if (!e.getKey().startsWith(prefix)) {
                break;
            }
            if (page.size() == maxKeys) {
                next = page.get(page.size() - 1).key();
                break;
            }
            page.add(new ObjectStat(e.getKey(), Files.size(e.getValue()),
                    readMeta(e.getValue()).version()));
        }
        return new ListPage(page, Optional.ofNullable(next));
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        // ⚠️ Per key, not around the loop: an absent key is not an error, and
        // catching outside the loop would abandon every key after the first
        // missing one -- which is exactly what a GC re-run after a crash hits.
        for (String key : keys) {
            Path p = pathFor(key);
            Files.deleteIfExists(p);
            Files.deleteIfExists(Path.of(p + VERSION_SUFFIX));
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(true, false, MAX_KEY_BYTES, 5L * 1024 * 1024, CostTable.free());
    }

    @Override
    public void close() {}
}
