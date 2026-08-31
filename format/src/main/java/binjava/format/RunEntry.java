// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * One directory entry: where a run lives and what is in it.
 *
 * <p>⚠️ Read WITHOUT touching the run's bytes, which is the point of a
 * fixed-width directory: a consumer resolves its byte range, and
 * {@code pointerFromTimestampMillis} is answerable from {@code minTimestampMillis}
 * alone, with no data read at all.
 */
public record RunEntry(
        RunKey key, int recordCount, long byteStart, int byteLen, long minTimestampMillis,
        int codecAndFlags) {}
