// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

/**
 * A resume is checked and cleared in ONE call, against a floor at least as
 * new as the report count that clears it (M8.44).
 *
 * <p>⚠️ **THE RACE WAS TWO READS IN THE WRONG ORDER.** The report count rose
 * before the floor did, and the shard read the count after its check, so a
 * report landing between them cleared a resume the new floor was never checked
 * against. Now the floor rises first and the count is read first: a count that
 * says "fresh" implies a floor at least that fresh. ⚠️ THE INTERLEAVING ITSELF
 * IS NOT PINNED HERE -- a single thread cannot land a report between two reads
 * -- only the contract the ordering serves.
 */
class FreshFloorResumeCheckTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000bb"), 0);

    private static ConsumerClient client() {
        return new ConsumerClient(KEY, 16, null, TestRetries.noFailedFetch());
    }

    @Test
    void aHELDFloorStillREFUSES() {
        ConsumerClient client = client();
        client.retainedFrom(500);
        long freshAfter = client.requestFreshFloor();

        assertThatThrownBy(() -> client.checkResume(499, freshAfter))
                .as("floors only rise, so a held one is a true lower bound")
                .isInstanceOf(PositionCollectedException.class);
    }

    @Test
    void aHELDFloorDoesNotCLEAR() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        long freshAfter = client.requestFreshFloor();

        assertThat(client.checkResume(900, freshAfter))
                .as("⚠️ NOT CLEARED: this floor predates the resume, and may be hours old")
                .isFalse();
    }

    @Test
    void aFRESHReportThatPassesCLEARS() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        long freshAfter = client.requestFreshFloor();
        client.retainedFrom(700);

        assertThat(client.checkResume(900, freshAfter)).as("checked and passed").isTrue();
    }

    @Test
    void aFRESHReportThatFailsREFUSESRatherThanClearing() {
        ConsumerClient client = client();
        client.retainedFrom(500);
        long freshAfter = client.requestFreshFloor();
        client.retainedFrom(950);

        assertThatThrownBy(() -> client.checkResume(900, freshAfter))
                .as("⚠️ THE FRESH FLOOR IS THE ONE CHECKED: 900 is below 950")
                .isInstanceOf(PositionCollectedException.class);
    }

    @Test
    void aReportAfterTheFloorCheckCannotClearTheResume() throws Exception {
        ConsumerClient client = client();
        client.retainedFrom(500);
        long freshAfter = client.requestFreshFloor();
        CountDownLatch countReadEntered = new CountDownLatch(1);
        CountDownLatch releaseCountRead = new CountDownLatch(1);
        CountDownLatch checkFinished = new CountDownLatch(1);
        AtomicReference<Object> outcome = new AtomicReference<>();
        LongSupplier delayedCountRead = () -> {
            countReadEntered.countDown();
            try {
                if (!releaseCountRead.await(2, TimeUnit.SECONDS)) {
                    throw new AssertionError("test did not release the report-count read");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("report-count read was interrupted", interrupted);
            }
            return client.floorReports();
        };

        try {
            Thread.ofVirtual().start(() -> {
                try {
                    outcome.set(client.checkResume(900, freshAfter, delayedCountRead));
                } catch (Throwable result) {
                    outcome.set(result);
                } finally {
                    checkFinished.countDown();
                }
            });

            assertThat(countReadEntered.await(1, TimeUnit.SECONDS))
                    .as("the check reached its controlled report-count snapshot")
                    .isTrue();
            client.retainedFrom(950);
        } finally {
            releaseCountRead.countDown();
            assertThat(checkFinished.await(1, TimeUnit.SECONDS))
                    .as("the controlled check finishes after the report is released")
                    .isTrue();
        }

        assertThat(outcome.get())
                .as("⚠️ the new floor is checked before its report can clear the resume")
                .isInstanceOf(PositionCollectedException.class);
    }
}
