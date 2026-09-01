// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.io.Closeable;
import java.io.IOException;

/**
 * One in-progress multipart upload, for an object that exceeds {@link
 * Capabilities#minPartSize()} (M2's own segments are not expected to exercise
 * this by default — research doc 01 §7's 8 MiB flush size stays under the
 * 16 MiB multipart threshold — but the capability must exist and be
 * conformance-tested).
 *
 * <p>⚠️ Parts are numbered from 1 and may arrive OUT OF ORDER — S3, GCS and
 * Azure all allow uploading part 3 before part 2 — but {@link #complete()}
 * always assembles them in PART-NUMBER order, never arrival order.
 *
 * <p>⚠️ Every part except the LAST is refused below {@link
 * Capabilities#minPartSize()}, mirroring S3's own multipart constraint: the
 * condition is evaluated at {@code complete()}, not per part, because which
 * part is last is not known until then.
 *
 * <p>⚠️ Streamed in, same as {@link Body}: nothing here may materialise a
 * whole part, let alone the whole assembled object, in heap (constraint C8).
 */
public interface MultipartWriter extends Closeable {

    /**
     * Uploads one part. Re-uploading the same {@code partNumber} replaces
     * whatever was previously staged for it — the same "last write wins"
     * shape a real multipart PUT of a part has.
     *
     * @param partNumber 1 or greater
     */
    void uploadPart(int partNumber, Body body) throws IOException;

    /**
     * Assembles every uploaded part, in part-number order, into the final
     * object and returns its version.
     *
     * <p>⚠️ Refuses if any part other than the highest-numbered one is
     * smaller than {@link Capabilities#minPartSize()}, and refuses an upload
     * with no parts at all — there is nothing to assemble.
     */
    Version complete() throws IOException;

    /**
     * Discards every uploaded part. The final object is never created, and a
     * key that already held an object before this upload started is left
     * untouched.
     */
    void abort() throws IOException;

    /**
     * ⚠️ NOT an implicit complete. A writer closed without calling {@link
     * #complete()} behaves exactly like {@link #abort()} — closing on an
     * exception path must never silently publish a partial object.
     */
    @Override
    void close() throws IOException;
}
