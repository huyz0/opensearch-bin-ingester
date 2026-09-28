// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.helidon.webserver.http.ServerRequest;

/**
 * Reads a bulk request's placement and lane from its query string. Moved out
 * of {@link BulkService} unchanged by M11.24c.
 */
final class PlacementParser {

    private PlacementParser() {
    }

    /**
     * ⚠️ EXACTLY ONE OF THE TWO, AND NEITHER HAS A DEFAULT. Defaulting the
     * partition to 0 funnels every producer that forgot the parameter into one
     * shard while returning 202; accepting both and preferring one silently
     * makes a producer's stated intent depend on which the implementation
     * happens to read first.
     */
    static BulkService.Placement placementOf(ServerRequest request) {
        BulkService.Placement placed = placeOf(request);
        return new BulkService.Placement(placed.partition(), placed.routing(), laneOf(request));
    }

    /**
     * The request's priority lane (FR-18, ADR-0074): absent is 0.
     *
     * <p>⚠️ ONLY ITS SYNTAX IS CHECKED HERE -- an integer that fits {@code i8}.
     * Whether it is ACTIVE is the ingester's decision, made before anything is
     * buffered and answered as a placement refusal; this adapter owns no
     * decision (ADR-0019).
     */
    private static byte laneOf(ServerRequest request) {
        var raw = request.query().first("lane");
        if (raw.isEmpty()) {
            return 0;
        }
        int lane;
        try {
            lane = Integer.parseInt(raw.get());
        } catch (NumberFormatException e) {
            throw new BulkParseException("'lane' is an integer");
        }
        if (lane < Byte.MIN_VALUE || lane > Byte.MAX_VALUE) {
            throw new BulkParseException("'lane' is a signed byte, -128 to 127");
        }
        return (byte) lane;
    }

    private static BulkService.Placement placeOf(ServerRequest request) {
        var rawPartition = request.query().first("partition");
        var routing = request.query().first("routing");
        if (rawPartition.isPresent() && routing.isPresent()) {
            throw new BulkParseException("'partition' and 'routing' are alternatives: the "
                    + "first says where these records go, the second asks the ingester to "
                    + "work it out. Send one");
        }
        if (routing.isPresent()) {
            if (routing.get().isEmpty()) {
                // ⚠️ AN EMPTY ROUTING VALUE IS REFUSED HERE, not placed. It
                // hashes to a partition like any other string, so accepting it
                // would send every producer that built its query string wrong
                // to one shard -- indistinguishable from a working deployment
                // until the shard is hot.
                throw new BulkParseException("'routing' is not empty");
            }
            return new BulkService.Placement(null, routing.get());
        }
        String raw = rawPartition.orElseThrow(() -> new BulkParseException(
                "one of the 'partition' or 'routing' query parameters is required: there is "
                        + "no default, because defaulting to partition 0 funnels every "
                        + "producer that forgot it into one shard while returning 202"));
        int partition;
        try {
            partition = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new BulkParseException("'partition' is an integer");
        }
        if (partition < 0) {
            throw new BulkParseException("'partition' is not negative");
        }
        return new BulkService.Placement(partition, null);
    }
}
