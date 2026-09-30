// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.client.SegmentFetchRetry;

/**
 * Fetch-retry policies for tests (M13.6c): the policy is always clocked now,
 * so a test states which clock rather than getting a clock-less default.
 */
final class TestRetries {

    private TestRetries() {
    }

    /**
     * The standard policy on a clock that FAILS the test if read: for a client
     * whose fetches do not fail, so a backoff the test did not expect is loud
     * rather than silently timed on a clock the test never set.
     */
    static SegmentFetchRetry noFailedFetch() {
        return SegmentFetchRetry.standard(() -> {
            throw new AssertionError("this test expected no failed segment fetch, "
                    + "but a backoff read the clock");
        });
    }
}
