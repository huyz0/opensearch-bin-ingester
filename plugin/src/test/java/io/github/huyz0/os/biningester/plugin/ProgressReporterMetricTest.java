// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionMetrics;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProgressReporterMetricTest {

    @Test
    void failedProgressPushUpdatesTheProcessCounter() {
        SubscriptionMetrics metrics = new SubscriptionMetrics();
        ProgressReporter reporter = new ProgressReporter(new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                return () -> { };
            }
        }, () -> List.of(new ProgressReporter.ShardPosition(
                "index-uuid", 0, "copy-0", 7)),
                () -> metrics.increment(SubscriptionMetrics.Counter.PROGRESS_PUSH_FAILURES));

        reporter.report();

        assertThat(reporter.pushFailures()).isEqualTo(1);
        assertThat(metrics.count(SubscriptionMetrics.Counter.PROGRESS_PUSH_FAILURES)).isEqualTo(1);
    }
}
