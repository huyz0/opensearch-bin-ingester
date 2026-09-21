// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore.backend;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.Version;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * One multipart upload, for a segment too large to hand to {@code put} as one
 * body (M8.2, M2.2).
 *
 * <p>⚠️ **THE KEY IS NOT OCCUPIED UNTIL {@link #complete()}**, which the SPI
 * promises and the writer path relies on: a reader that could see a half-staged
 * object would read a truncated segment, and a truncated segment PARSES — the
 * record boundary is inside it.
 *
 * <p>⚠️ **AN ABANDONED UPLOAD IS BILLED STORAGE NOBODY CAN SEE.** Staged parts
 * survive the process that staged them and are invisible to every LIST this
 * project makes, so {@link #close()} aborts one that was never completed rather
 * than leaving it to a lifecycle rule nobody configured.
 */
final class S3MultipartWriter implements MultipartWriter {

    private final S3Client client;
    private final String bucket;
    private final String key;
    private final String uploadId;
    private final long minPartSize;
    /**
     * ⚠️ **BY PART NUMBER, NOT BY ARRIVAL, AND THAT IS THE SPI's CONTRACT IN AS
     * MANY WORDS**: parts may arrive out of order and {@code complete()} always
     * assembles them in PART-NUMBER order. ⚠️ **AND A MAP RATHER THAN A LIST**,
     * because re-uploading a part number REPLACES what was staged for it — an
     * appending list sends the number twice, S3 concatenates it twice, and the
     * object is silently double the size it should be and still parses.
     * MEASURED by review on the running endpoint: 5 MiB + 5 MiB + 3 bytes for
     * parts 1, 1, 2 completed successfully at 10,485,763 bytes.
     * {@code LocalFsBinStore} keeps a {@code TreeMap} for the same two reasons.
     */
    // ⚠️ `NavigableMap`, NOT `Map`: `complete()` asks for `lastKey()`, and with
    // the field typed `Map` that was a CAST -- so swapping in an unordered map
    // compiled and failed at run time, in the writer path, as a
    // `ClassCastException`.
    private final NavigableMap<Integer, CompletedPart> parts = new TreeMap<>();
    private final NavigableMap<Integer, Long> sizes = new TreeMap<>();
    private boolean finished;

    static S3MultipartWriter begin(S3Client client, String bucket, String key, long minPartSize)
            throws IOException {
        try {
            String uploadId = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                    .bucket(bucket).key(key).build()).uploadId();
            return new S3MultipartWriter(client, bucket, key, uploadId, minPartSize);
        } catch (SdkException failed) {
            throw new IOException("multipart " + key + " failed: " + failed.getMessage(), failed);
        }
    }

    private S3MultipartWriter(S3Client client, String bucket, String key, String uploadId,
            long minPartSize) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.uploadId = uploadId;
        this.minPartSize = minPartSize;
    }

    @Override
    public void uploadPart(int partNumber, Body body) throws IOException {
        Objects.requireNonNull(body, "body");
        if (partNumber <= 0) {
            // ⚠️ AN `IOException`, MATCHING `LocalFsBinStore`. M8.22 runs ONE
            // conformance suite against every backend, and a suite cannot
            // assert two different exception types for one contract.
            throw new IOException("part numbers start at 1: " + partNumber);
        }
        if (finished) {
            throw new IOException("multipart " + key + " is finished; no further part");
        }
        try {
            UploadPartResponse written = client.uploadPart(UploadPartRequest.builder()
                            .bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber)
                            .build(),
                    RequestBody.fromContentProvider(body::checkedStream, body.length(),
                            "application/octet-stream"));
            parts.put(partNumber, CompletedPart.builder().partNumber(partNumber)
                    .eTag(written.eTag()).build());
            sizes.put(partNumber, body.length());
        } catch (SdkException failed) {
            throw new IOException("uploadPart " + partNumber + " of " + key + " failed: "
                    + failed.getMessage(), failed);
        }
    }

    @Override
    public Version complete() throws IOException {
        if (finished) {
            throw new IOException("multipart " + key + " is already finished");
        }
        if (parts.isEmpty()) {
            throw new IOException("multipart " + key + " has no parts to complete");
        }
        // ⚠️ REFUSED HERE RATHER THAN BY THE ENDPOINT, and the two are not the
        // same refusal: S3 rejects a short non-final part when the upload
        // COMPLETES, which is after every byte has been sent and paid for. The
        // same check in front of it turns a wasted upload into an argument
        // error -- and `LocalFsBinStore` makes it too, so the conformance suite
        // asserts one contract rather than two.
        // ⚠️ THE HIGHEST PART NUMBER IS THE FINAL PART, NEVER THE LAST ONE THAT
        // ARRIVED. An earlier draft walked arrival order and refused a perfectly
        // legal upload whose small final part happened to be sent first --
        // measured by review, which is also how the ordering defect above was
        // found.
        int lastPartNumber = sizes.lastKey();
        for (Map.Entry<Integer, Long> part : sizes.entrySet()) {
            if (part.getKey() != lastPartNumber && part.getValue() < minPartSize) {
                throw new IOException("part " + part.getKey() + " of " + key + " is "
                        + part.getValue() + " bytes, below the minimum non-final part size "
                        + minPartSize + " bytes");
            }
        }
        try {
            CompleteMultipartUploadResponse done = client.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucket).key(key).uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder()
                                    .parts(new ArrayList<>(parts.values())).build())
                            .build());
            finished = true;
            return new Version(done.eTag());
        } catch (SdkException failed) {
            throw new IOException("complete " + key + " failed: " + failed.getMessage(), failed);
        }
    }

    @Override
    public void abort() throws IOException {
        if (finished) {
            return;
        }
        finished = true;
        try {
            client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket).key(key).uploadId(uploadId).build());
        } catch (SdkException failed) {
            throw new IOException("abort " + key + " failed: " + failed.getMessage(), failed);
        }
    }

    /**
     * ⚠️ **CLOSING AN INCOMPLETE UPLOAD ABORTS IT**, because the alternative is
     * parts nobody can see and everybody pays for. A completed one closes to
     * nothing.
     */
    @Override
    public void close() throws IOException {
        abort();
    }
}
