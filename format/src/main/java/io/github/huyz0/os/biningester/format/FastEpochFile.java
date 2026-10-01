// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * The pod's {@code epoch} file beside its fast journal (ADR-0081 §3, ADR-0082
 * §4; M13.26c): the highest fast epoch the pod has seen, so a container
 * restart still refuses a deposed leader's frame of a lower epoch.
 *
 * <p>Seventeen bytes: magic {@code 0x42464550} ("BFEP") u32, version 1 u8,
 * the epoch i64, and the CRC32C of the thirteen bytes before it, u32 --
 * big-endian, as every frame here.
 *
 * <p>⚠️ NO TORN FORM EXISTS: the file is only ever replaced whole (written
 * beside, fsynced, renamed, the directory fsynced), so a file that does not
 * decode is damage or another build's, and is REFUSED -- the pod stays
 * unready -- never read as some lower epoch, which would reopen the window
 * this file closes.
 */
public final class FastEpochFile {

    public static final int MAGIC = 0x42464550;
    public static final int VERSION = 1;
    public static final int LENGTH = 17;

    private FastEpochFile() {
    }

    public static byte[] encode(long epoch) {
        if (epoch < 0) {
            throw new IllegalArgumentException("an epoch is never negative: " + epoch);
        }
        ByteBuffer out = ByteBuffer.allocate(LENGTH);
        out.putInt(MAGIC).put((byte) VERSION).putLong(epoch);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, LENGTH - 4);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    public static long decode(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length != LENGTH) {
            throw new IOException("an epoch file is " + LENGTH + " bytes, not "
                    + (bytes == null ? "absent" : bytes.length));
        }
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt() != MAGIC) {
            throw new IOException("not an epoch file: bad magic");
        }
        int version = in.get() & 0xFF;
        if (version != VERSION) {
            throw new IOException("an epoch file of version " + version + ", which this build "
                    + "cannot read");
        }
        long epoch = in.getLong();
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, LENGTH - 4);
        if (in.getInt() != (int) crc.getValue()) {
            throw new IOException("an epoch file whose checksum fails");
        }
        if (epoch < 0) {
            throw new IOException("an epoch file holding a negative epoch: " + epoch);
        }
        return epoch;
    }
}
