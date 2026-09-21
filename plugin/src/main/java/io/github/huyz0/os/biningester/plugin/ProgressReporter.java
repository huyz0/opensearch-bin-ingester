// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.ConsumerProgress;
import java.lang.System.Logger;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What one node tells the ingester about where its shard copies have got to
 * (M7.3, FR-9,
 * <a href="../../../../../../docs/internal/product/decisions/0005-no-consumer-offset-store.md">ADR-0005</a>,
 * <a href="../../../../../../docs/internal/product/decisions/0049-consumer-progress-is-a-format-type-on-the-subscription-channel.md">ADR-0049</a>).
 *
 * <p>⚠️ IT REPORTS WHAT THE SHARD HAS CONSUMED, NEVER WHAT THE NODE HAS BEEN
 * SENT. A reporter that sent the stream's tail offset — or a constant — sends
 * one frame per node per interval at zero object-store requests, which is
 * green on every count-and-cost assertion, and drives the ingester's
 * {@code min()} to the live edge forever. GC then deletes records no shard has
 * indexed. The distance between "delivered to this node" and "committed by
 * this shard" is exactly the data the retention window exists to protect.
 *
 * <p>⚠️ IT HOLDS NO CLOCK, NO EXECUTOR AND NO THREAD. The interval belongs to
 * whoever schedules {@link #report()}, which is the same choice
 * {@link IndexRegistrar} made about the applier thread and for the same reason:
 * a timer inside the class is untestable and turns an idle node into something
 * that runs of its own accord.
 *
 * <p>⚠️ AND IT IS LEVEL-TRIGGERED, WHICH IS WHY A FAILED PUSH IS NOT RETRIED.
 * {@link IndexRegistrar} retries three times because a registration is
 * EDGE-triggered: a lost push is lost until the next cluster-state change,
 * which may be hours away. A progress frame is superseded by the next interval,
 * so retrying one would re-send a number that is already stale. A missed
 * interval costs FRESHNESS, and an unfresh watermark makes GC keep — the
 * correct failure direction.
 *
 * <p>⚠️ A FAILURE NEVER LEAVES THIS CLASS. It runs on a pool shared with the
 * rest of the node; a throw would kill whatever schedules it and stop the
 * watermark for every stream here, not for one interval.
 */
public final class ProgressReporter {

    private static final Logger LOG =
            java.lang.System.getLogger(ProgressReporter.class.getName());

    /**
     * One shard copy on this node, and where it has got to.
     *
     * <p>⚠️ {@code consumedUpTo} IS EXCLUSIVE — the offset the copy would
     * resume FROM, which is what {@code StreamPoller.BATCH_START} means.
     * {@code shardCopy} is the allocation id, because every copy consumes the
     * partition independently and a lagging replica is a consumer.
     */
    public record ShardPosition(String indexUuid, int partition, String shardCopy,
            long consumedUpTo) {
    }

    /**
     * Where this node's shard copies have got to, read AFRESH each interval.
     *
     * <p>⚠️ A SEAM RATHER THAN A DIRECT REACH INTO THE NODE. The production
     * source is {@link ShardPositions}, which reads each copy's COMMITTED
     * {@code batch_start} (ADR-0051, M8.43); {@code readNext(pointer, ...)}
     * carries the engine's pointer only on a forced or reset pointer (research
     * 20/02 §2), and what the consumer delivered is ahead of the commit.
     */
    @FunctionalInterface
    public interface Positions {
        List<ShardPosition> current();
    }

    private final SubscriptionTransport transport;
    private final Positions positions;
    private final AtomicInteger pushes = new AtomicInteger();
    private final AtomicInteger failures = new AtomicInteger();

    public ProgressReporter(SubscriptionTransport transport, Positions positions) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.positions = Objects.requireNonNull(positions, "positions");
    }

    /**
     * Sends one frame for every shard copy this node hosts, or nothing at all
     * if it hosts none.
     *
     * <p>⚠️ A NODE WITH NO SHARDS SENDS NOTHING RATHER THAN AN EMPTY FRAME.
     * {@link ConsumerProgress} refuses an empty batch, because silence already
     * means "freeze this copy" and a frame that carried no position would keep
     * a dead node's copies looking fresh.
     */
    public void report() {
        List<ShardPosition> here;
        try {
            // ⚠️ THE SOURCE IS INSIDE THE TRY, AND THAT IS NOT DEFENSIVENESS.
            // M7.17's production source is the node's own ingestion state, and
            // every handle onto it throws unchecked when a shard is closing,
            // relocating or already gone -- `AlreadyClosedException`,
            // `ShardNotFoundException`, `IndexNotFoundException`. A throw out
            // of a scheduled task CANCELS THE SCHEDULE, silently, so progress
            // would stop for every stream on this node permanently while
            // `pushFailures()` never moved.
            here = positions.current();
        } catch (RuntimeException unavailable) {
            failures.incrementAndGet();
            LOG.log(Logger.Level.WARNING, () -> "this node's shard positions could not be "
                    + "read this interval; GC keeps meanwhile: " + unavailable);
            return;
        }
        if (here == null || here.isEmpty()) {
            return;
        }
        ConsumerProgress frame;
        try {
            frame = new ConsumerProgress(here.stream()
                    .map(p -> new ConsumerProgress.Entry(p.indexUuid(), p.partition(),
                            p.shardCopy(), p.consumedUpTo()))
                    .toList());
        } catch (RuntimeException refused) {
            // ⚠️ RuntimeException AND NOT IllegalArgumentException. The record
            // refuses a duplicate copy or a blank id with an IAE, but a null
            // element in the list arrives as an NPE from the mapping lambda,
            // and both are "this node's positions do not make a frame".
            failures.incrementAndGet();
            LOG.log(Logger.Level.WARNING, () -> "this node's shard positions do not make a "
                    + "progress frame: " + refused);
            return;
        }
        try {
            transport.report(frame);
            pushes.incrementAndGet();
        } catch (RuntimeException failed) {
            failures.incrementAndGet();
            LOG.log(Logger.Level.WARNING, () -> "progress for " + frame.entries().size()
                    + " shard copies did not reach the ingester; the next interval carries "
                    + "a newer position and GC keeps meanwhile: " + failed);
        }
    }

    /** Frames this node has successfully pushed. */
    public int pushes() {
        return pushes.get();
    }

    /**
     * Frames that did not reach the ingester.
     *
     * <p>⚠️ A DEPLOYMENT WHOSE TRANSPORT CANNOT CARRY PROGRESS COUNTS HERE
     * RATHER THAN LOOKING HEALTHY. The seam's default refuses, so this number
     * climbing is how that is visible before the data reaches
     * {@code maxRetention} and its alarm.
     */
    public int pushFailures() {
        return failures.get();
    }
}
