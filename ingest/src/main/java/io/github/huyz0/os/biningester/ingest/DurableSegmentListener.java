// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

/** Best-effort notification after a segment and its commit are durable. */
@FunctionalInterface
public interface DurableSegmentListener {
    void onDurable(String segmentKey);
}
