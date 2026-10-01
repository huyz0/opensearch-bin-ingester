// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * {@link JournalFile} in memory, with crashes a test can inject (M13.24).
 *
 * <p>⚠️ IT LOSES EXACTLY WHAT ADR-0082 §4 ASSUMES A DISK LOSES: every byte
 * appended since the last {@link #force} on {@link #crash()}, or all but a
 * prefix of them on {@link #crashTearing(int)}. A fake that never lost
 * anything would make every journal test vacuous, since the journal's whole
 * contract is about what a crash takes away. A truncate or a replace is
 * durable when it returns, and anything appended after it is unforced again.
 */
public final class MemoryJournalFile implements JournalFile {

    private byte[] durable = new byte[0];
    private final ByteArrayOutputStream pending = new ByteArrayOutputStream();

    @Override
    public synchronized byte[] readAll() {
        byte[] tail = pending.toByteArray();
        byte[] all = Arrays.copyOf(durable, durable.length + tail.length);
        System.arraycopy(tail, 0, all, durable.length, tail.length);
        return all;
    }

    @Override
    public synchronized void append(byte[] bytes) {
        pending.writeBytes(bytes);
    }

    @Override
    public synchronized void force() {
        durable = readAll();
        pending.reset();
    }

    @Override
    public synchronized void truncate(long length) {
        byte[] all = readAll();
        if (length < 0 || length > all.length) {
            throw new IllegalArgumentException("cannot truncate " + all.length + " bytes to "
                    + length);
        }
        durable = Arrays.copyOf(all, (int) length);
        pending.reset();
    }

    @Override
    public synchronized void replace(byte[] contents) {
        durable = contents.clone();
        pending.reset();
    }

    @Override
    public synchronized long size() {
        return (long) durable.length + pending.size();
    }

    @Override
    public void close() {
    }

    /** A crash that loses every byte appended since the last force. */
    public synchronized void crash() {
        pending.reset();
    }

    /** A crash that keeps the first {@code keep} unforced bytes: a torn tail. */
    public synchronized void crashTearing(int keep) {
        byte[] tail = pending.toByteArray();
        int kept = Math.max(0, Math.min(keep, tail.length));
        durable = Arrays.copyOf(durable, durable.length + kept);
        System.arraycopy(tail, 0, durable, durable.length - kept, kept);
        pending.reset();
    }
}
