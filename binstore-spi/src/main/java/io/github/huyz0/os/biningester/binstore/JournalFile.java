// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.io.IOException;

/**
 * One pod-local, append-only file: the fast journal's only reach to a disk
 * (M13.24, ADR-0083).
 *
 * <p>⚠️ APPENDED IS NOT DURABLE. Bytes from {@link #append} may be lost by a
 * crash until {@link #force} returns, and a crash may keep ANY PREFIX of them
 * -- a torn tail. The journal answers nothing about an entry before the force
 * that covers it, and truncates a torn tail before it appends again
 * (ADR-0082 §4).
 *
 * <p>⚠️ {@link #truncate} AND {@link #replace} ARE DURABLE WHEN THEY RETURN, and
 * {@link #replace} is atomic: after a crash the file holds the old contents or
 * the new, never a mix -- a temporary file, fsynced, renamed over the old one,
 * and the directory fsynced. Never rewritten in place.
 */
public interface JournalFile extends AutoCloseable {

    /** Every byte the file holds, durable or not yet, from its start. */
    byte[] readAll() throws IOException;

    /** Appends {@code bytes}; durable only once a later {@link #force} returns. */
    void append(byte[] bytes) throws IOException;

    /** Makes every byte appended so far durable. */
    void force() throws IOException;

    /**
     * Cuts the file to {@code length} bytes, durably.
     *
     * @throws IllegalArgumentException for a length past the end: never a hole
     */
    void truncate(long length) throws IOException;

    /** Replaces the whole file with {@code contents}, atomically and durably. */
    void replace(byte[] contents) throws IOException;

    /** The file's length in bytes. */
    long size();

    /** Releases the file; bytes not yet forced may be lost. */
    @Override
    void close() throws IOException;
}
