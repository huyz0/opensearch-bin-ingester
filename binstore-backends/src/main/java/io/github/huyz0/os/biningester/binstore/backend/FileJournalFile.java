// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * {@link JournalFile} on a local file in the pod's {@code emptyDir} (M13.24,
 * ADR-0083).
 *
 * <p>⚠️ A REPLACE IS A NEW FILE RENAMED OVER THE OLD ONE, NEVER A REWRITE IN
 * PLACE: the new contents are written to a sibling, fsynced, moved over the
 * journal atomically, and the directory is fsynced so the rename itself
 * survives a crash. A crash at any point leaves the old file or the new one.
 *
 * <p>⚠️ THE DIRECTORY FSYNC IS BEST EFFORT WHERE THE PLATFORM HAS NONE. Linux
 * -- where the pods run -- opens a directory for reading and forces it;
 * Windows refuses to open one as a channel, and there the rename is as
 * durable as the platform makes it. The tests run on both; the property this
 * class promises is the Linux one.
 */
public final class FileJournalFile implements JournalFile {

    private final Path path;
    private FileChannel channel;

    private FileJournalFile(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
    }

    /**
     * Opens or creates {@code name} in {@code directory}, creating the directory
     * if it is missing (M13.27i): the server names its {@code emptyDir} from
     * configuration as text and may not reach the file system itself
     * (non-negotiable 7).
     */
    public static FileJournalFile in(String directory, String name) {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(name, "name");
        Path dir = Path.of(directory).toAbsolutePath();
        try {
            Path missing = dir;
            while (missing.getParent() != null && !Files.exists(missing.getParent())) {
                missing = missing.getParent();
            }
            boolean created = !Files.exists(dir);
            Files.createDirectories(dir);
            if (created) {
                // ⚠️ A NEW DIRECTORY'S NAME IS DURABLE ONLY ONCE ITS PARENT IS
                // (M13.27i review round 1, P2), as a new file's is once its
                // directory is: forced from the deepest level up to the
                // parent of the first one created.
                Path top = missing.getParent();
                for (Path level = dir; !level.equals(top); level = level.getParent()) {
                    forceDirectory(level.getParent());
                }
            }
        } catch (IOException failed) {
            throw new UncheckedIOException("cannot create the fast journal's directory "
                    + directory, failed);
        }
        return open(dir.resolve(name));
    }

    /** Opens or creates the journal at {@code path}. */
    public static FileJournalFile open(Path path) {
        Objects.requireNonNull(path, "path");
        try {
            boolean created = !Files.exists(path);
            FileJournalFile file = new FileJournalFile(path, openChannel(path));
            if (created) {
                // ⚠️ A NEW FILE'S NAME IS DURABLE ONLY ONCE ITS DIRECTORY IS
                // (M13.24 review round 2, P2): without this, a crash before the
                // first compaction may leave forced bytes in no file at all.
                try {
                    file.forceDirectory();
                } catch (IOException failed) {
                    file.channel.close();
                    throw failed;
                }
            }
            return file;
        } catch (IOException failed) {
            throw new UncheckedIOException("cannot open the fast journal at " + path, failed);
        }
    }

    private static FileChannel openChannel(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.position(channel.size());
        return channel;
    }

    @Override
    public synchronized byte[] readAll() throws IOException {
        long size = channel.size();
        if (size > Integer.MAX_VALUE) {
            throw new IOException("journal of " + size + " bytes is larger than one read");
        }
        ByteBuffer all = ByteBuffer.allocate((int) size);
        long at = 0;
        while (all.hasRemaining()) {
            int read = channel.read(all, at);
            if (read < 0) {
                throw new IOException("journal ended at " + at + " of " + size + " bytes");
            }
            at += read;
        }
        return all.array();
    }

    @Override
    public synchronized void append(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public synchronized void force() throws IOException {
        channel.force(false);
    }

    @Override
    public synchronized void truncate(long length) throws IOException {
        if (length < 0 || length > channel.size()) {
            // ⚠️ AS THE FAKE DOES: a truncate past the end would position the
            // next append beyond it and leave a zero-filled hole, which the next
            // recovery would read as a tear and cut, with everything after it.
            throw new IllegalArgumentException("cannot truncate " + channel.size()
                    + " bytes to " + length);
        }
        channel.truncate(length);
        channel.position(length);
        channel.force(true);
    }

    @Override
    public synchronized void replace(byte[] contents) throws IOException {
        Path next = path.resolveSibling(path.getFileName() + ".next");
        try (FileChannel out = FileChannel.open(next, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(contents);
            while (buffer.hasRemaining()) {
                out.write(buffer);
            }
            out.force(true);
        }
        channel.close();
        IOException moveFailed = null;
        try {
            Files.move(next, path, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException notAtomic) {
            moveFailed = new IOException("the journal's directory cannot rename atomically; "
                    + "refusing a non-atomic replace", notAtomic);
        } catch (IOException failed) {
            moveFailed = failed;
        }
        try {
            channel = openChannel(path);
        } catch (IOException reopenFailed) {
            // ⚠️ THE MOVE'S FAILURE IS THE ONE THAT EXPLAINS THIS; keep it first.
            if (moveFailed != null) {
                moveFailed.addSuppressed(reopenFailed);
                throw moveFailed;
            }
            throw reopenFailed;
        }
        if (moveFailed != null) {
            throw moveFailed;
        }
        forceDirectory();
    }

    private void forceDirectory() throws IOException {
        forceDirectory(path.toAbsolutePath().getParent());
    }

    private static void forceDirectory(Path dir) throws IOException {
        try (FileChannel directory = FileChannel.open(dir, StandardOpenOption.READ)) {
            directory.force(true);
        } catch (IOException | UnsupportedOperationException noDirectoryFsync) {
            // ⚠️ SEE THE CLASS JAVADOC: Windows cannot open a directory as a
            // channel; Linux, where the pods run, can.
            if (!System.getProperty("os.name", "").startsWith("Windows")) {
                throw noDirectoryFsync instanceof IOException io ? io
                        : new IOException(noDirectoryFsync);
            }
        }
    }

    @Override
    public synchronized long size() {
        try {
            return channel.size();
        } catch (IOException failed) {
            throw new UncheckedIOException(failed);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
