// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * How a consumer gets a segment's bytes -- the subscription's {@code via}
 * (FR-6, M5.11).
 *
 * <p>⚠️ THE INGESTER CHOOSES, NEVER THE CONSUMER. FR-6 says so and ADR-0004
 * priced the alternative: a consumer that could demand {@link #DIRECT} at
 * fan-out 300 reproduces the $3,732/month design that record rejected. Nothing
 * in {@code FetchPolicy}'s input names a mode, so there is no way to ask --
 * the rule is unrepresentable rather than written down.
 *
 * <p>⚠️ THESE THREE AND NO OTHERS. The names come from the contract document
 * rather than from this file: {@code 04-discovery-and-tailing.md}'s "Do the
 * bytes cost a fetch?" table defines them, and
 * {@code 10-client-library-and-fetch-modes.md} bounds when each applies. This
 * enum IMPLEMENTS that contract; it does not change it, which is why M5.11
 * owes no banner and no version bump. The subscription's own shape changes at
 * M5.14, which owns the ADR and the golden files.
 *
 * <p>⚠️ NOT SERIALIZED YET, deliberately. No encoder or decoder names this
 * type until M5.14 puts it on the wire. Adding one here would ship half a
 * format change -- a tree that can write bytes nothing reads, which is exactly
 * what `wire-format-change`'s one-commit rule exists to prevent.
 */
public enum FetchMode {

    /**
     * The bytes travel with the push, so the consumer issues NO object-store
     * request at all.
     *
     * <p>⚠️ THE DEFAULT, AND IT STAYS THE DEFAULT. It is the only mode under
     * which an idle or a busy consumer costs nothing to serve, which is what
     * makes M5's zero-idle-requests criterion hold under load rather than only
     * at rest. Every other mode is an escape from it.
     */
    INLINE,

    /**
     * The ingester streams the segment through to the consumer, which still
     * issues no object-store request.
     *
     * <p>⚠️ STREAMED, NEVER BUFFERED (ADR-0004). Buffering makes service
     * memory scale with fan-out, which NFR-6 forbids; M5.12 owns the
     * implementation and the memory bound that pins it.
     */
    PROXY,

    /**
     * The consumer reads the store itself over a short-lived signed URL, and
     * this is the one mode that adds a consumer-side GET.
     *
     * <p>⚠️ THE ESCAPE HATCH, not a default: catch-up replay at fan-out 1, or
     * a pod shedding load. It needs {@code Capabilities.presignedUrls}
     * (ADR-0041), which only the S3 backend has (M8.18) -- so a policy that
     * chose this against a backend that cannot sign would hand out a mode
     * nothing can serve. M5.13 owns the grant and the startup refusal.
     */
    DIRECT
}
