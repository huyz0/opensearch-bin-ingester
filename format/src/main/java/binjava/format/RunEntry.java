// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * One directory entry: where a run lives and what is in it.
 *
 * <p>⚠️ Read WITHOUT touching the run's bytes, which is the point of a
 * fixed-width directory: a consumer resolves its byte range, and
 * {@code pointerFromTimestampMillis} is answerable from {@code minTimestampMillis}
 * alone, with no data read at all.
 *
 * <p>⚠️ {@code lane} (M3; ADR-0025) is a {@link SegmentFormat#VERSION}-only
 * field, reserved and always {@code 0} until M10 starts writing a real
 * value — a {@link SegmentFormat#VERSION_0} entry has no lane byte at all,
 * and {@link SegmentReader} synthesises {@code 0} for one rather than
 * leaving this field's meaning depend on which version was actually read.
 */
public record RunEntry(
        RunKey key, int recordCount, long byteStart, int byteLen, long minTimestampMillis,
        int codecAndFlags, byte lane) {}
