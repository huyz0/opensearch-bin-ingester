// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

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

    /**
     * A policy on the test's own clock, advanced only by the policy's sleeper:
     * waiting a backoff out passes its due time, which is what the clock-less
     * policy's debt was (M13.6c). {@code sleeper} still sees every wait.
     */
    static SegmentFetchRetry sleepAdvanced(java.time.Duration floor, java.time.Duration ceiling,
            int maxAttempts, SegmentFetchRetry.Sleeper sleeper) {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong();
        return new SegmentFetchRetry(floor, ceiling, maxAttempts, wait -> {
            sleeper.sleep(wait);
            now.addAndGet(wait.toMillis());
        }, now::get);
    }
}
