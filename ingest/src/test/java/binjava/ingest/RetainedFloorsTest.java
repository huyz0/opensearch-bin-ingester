// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.Checkpoint;
import binjava.format.RunKey;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The serving pod's retained-floor cache (M8.6, ADR-0056).
 *
 * <p>⚠️ **THE READ COUNT IS THE PROPERTY.** Every case here that is about
 * correctness is cheap to get right; the one that is not is "a thousand
 * reconnects cost one checkpoint read", which is what keeps this inside
 * non-negotiable 6 when every shard on a restarted node resumes at once.
 */
class RetainedFloorsTest {

    private static final RunKey LOGS = new RunKey(new UUID(1, 1), 0);
    private static final RunKey METRICS = new RunKey(new UUID(2, 2), 3);
    private static final Duration REFRESH = Duration.ofMinutes(1);

    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-09-19T00:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static Checkpoint checkpoint(long logsFloor) {
        return new Checkpoint(10,
                Map.of(LOGS, new Checkpoint.StreamOffsets(1000, logsFloor)), Map.of());
    }

    private final MutableClock clock = new MutableClock();
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicReference<Optional<Checkpoint>> newest =
            new AtomicReference<>(Optional.of(checkpoint(400)));

    private RetainedFloors floors() {
        return new RetainedFloors(() -> {
            reads.incrementAndGet();
            return newest.get();
        }, clock, REFRESH);
    }

    @Test
    void aSTREAMsFloorIsTheCHECKPOINTsAndAStreamItDoesNotNameIsUNKNOWN() {
        RetainedFloors floors = floors();

        assertThat(floors.floorOf(LOGS)).hasValue(400);
        assertThat(floors.floorOf(METRICS))
                .as("⚠️ UNKNOWN, NOT ZERO: nothing has been collected is a different fact "
                        + "from nobody knows, and only the second is true here")
                .isEmpty();
    }

    @Test
    void aTHOUSANDSessionsInsideOneIntervalCostONERead() {
        // ⚠️ A ROLLING RESTART OF THE OPENSEARCH NODES RECONNECTS EVERY SHARD
        // AT ONCE. A cache refreshed per session would be a read per shard.
        RetainedFloors floors = floors();

        for (int i = 0; i < 1000; i++) {
            floors.floorOf(i % 2 == 0 ? LOGS : METRICS);
        }

        assertThat(reads).hasValue(1);
    }

    @Test
    void aREFRESHHappensOnceTheIntervalHasPassedAndNotBefore() {
        RetainedFloors floors = floors();
        floors.floorOf(LOGS);

        clock.advance(REFRESH.minusMillis(1));
        newest.set(Optional.of(checkpoint(900)));
        assertThat(floors.floorOf(LOGS)).as("inside the interval: the cached floor").hasValue(400);

        clock.advance(Duration.ofMillis(1));
        assertThat(floors.floorOf(LOGS)).as("at the interval: re-read").hasValue(900);
        assertThat(reads).hasValue(2);
    }

    @Test
    void aFLOORNeverFallsEvenWhenANewerCheckpointSaysLess() {
        // ⚠️ A TAKEOVER's FIRST CHECKPOINT HAS COLLECTED NOTHING YET and can
        // report a floor below the previous term's. Collection never
        // un-deletes, so the lower number is less informed, not more true.
        RetainedFloors floors = floors();
        floors.floorOf(LOGS);

        clock.advance(REFRESH);
        newest.set(Optional.of(checkpoint(0)));

        assertThat(floors.floorOf(LOGS)).hasValue(400);
    }

    @Test
    void aFAILEDReadKeepsWhatIsKnownAndStillCOUNTSAsTheIntervalsRead() {
        // ⚠️ STAMPED BEFORE THE READ. A store that is down would otherwise be
        // asked by every session that starts while it is down -- the burst the
        // bound exists for, arriving during an outage.
        AtomicReference<Boolean> down = new AtomicReference<>(false);
        RetainedFloors floors = new RetainedFloors(() -> {
            reads.incrementAndGet();
            if (down.get()) {
                throw new IOException("store unreachable");
            }
            return newest.get();
        }, clock, REFRESH);
        assertThat(floors.floorOf(LOGS)).hasValue(400);

        clock.advance(REFRESH);
        down.set(true);
        for (int i = 0; i < 50; i++) {
            assertThat(floors.floorOf(LOGS)).as("the floor already learned is still true")
                    .hasValue(400);
        }

        assertThat(reads).as("one failed read for the interval, not fifty").hasValue(2);
    }

    @Test
    void aTERMWithNoCheckpointYetKeepsWhatIsKnown() {
        RetainedFloors floors = floors();
        floors.floorOf(LOGS);

        clock.advance(REFRESH);
        newest.set(Optional.empty());

        assertThat(floors.floorOf(LOGS)).hasValue(400);
    }

    @Test
    void theUNKNOWNFloorsNeverKnowAnything() {
        assertThat(RetainedFloors.unknown().floorOf(LOGS)).isEmpty();
    }
}
