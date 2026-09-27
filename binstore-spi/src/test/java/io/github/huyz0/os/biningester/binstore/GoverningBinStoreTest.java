// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * The governing decorator (M10.10, ADR-0075, M10 criterion 13).
 *
 * <p>⚠️ THE DELEGATE RECORDS WHAT REACHED IT, so "refused" means the request
 * never left the pod, and "never refused" means it did.
 */
class GoverningBinStoreTest {

    private static final String PREFIX = "bins/c";

    private final List<String> reached = new CopyOnWriteArrayList<>();
    private final Clock frozen = Clock.fixed(Instant.ofEpochMilli(1_790_000_000_000L),
            ZoneOffset.UTC);

    private BinStore recording() {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    reached.add(method.getName());
                    return switch (method.getName()) {
                        case "list" -> new ListPage(List.of(), Optional.empty());
                        case "get", "getRange" -> new ByteArrayInputStream(new byte[0]);
                        case "stat", "putIfAbsent", "putIfMatch" -> Optional.empty();
                        case "put" -> new Version("v");
                        case "delete", "close" -> null;
                        case "multipart" -> null;
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
    }

    private static Body body(int n) {
        return Body.ofBytes(new byte[n]);
    }

    @Test
    void anUndeclaredListPastTheCeilingIsRefusedWhateverItsPrefix() throws Exception {
        CostGovernor governor = new CostGovernor(new CostGovernor.Settings(1.0, 2,
                Duration.ofSeconds(60), 8 << 20), frozen, () -> 5_000);
        BinStore store = new GoverningBinStore(recording(), governor);

        store.list(PREFIX + "/data/2026/09/27/10/", null, 1_000);
        store.list(PREFIX + "/data/2026/09/27/11/", null, 1_000);
        assertThatThrownBy(() -> store.list(PREFIX + "/data/2026/09/27/12/", null, 1_000))
                .isInstanceOf(GovernorRefusedException.class);
        assertThatThrownBy(() -> store.list(PREFIX + "/ctl/log/0/", null, 1_000))
                .as("⚠️ A COMMIT-LOG PREFIX IS NOT AN EXEMPTION: a reinstated poll of "
                        + "committed data lists exactly this")
                .isInstanceOf(GovernorRefusedException.class);

        assertThat(reached).as("a refused LIST never reaches the store")
                .containsExactly("list", "list");
        assertThat(governor.counts().listRefusals()).isEqualTo(2);
    }

    @Test
    void aDeclaredRecoveryListPassesWithTheBucketEmpty() throws Exception {
        CostGovernor governor = new CostGovernor(new CostGovernor.Settings(1.0, 1,
                Duration.ofSeconds(60), 8 << 20), frozen, () -> 5_000);
        BinStore store = new GoverningBinStore(recording(), governor);
        store.list(PREFIX + "/data/x/", null, 1);

        GovernorScope.recovery(() -> {
            for (int i = 0; i < 50; i++) {
                store.list(PREFIX + "/ctl/log/0/", null, 1_000);
            }
            return null;
        });

        assertThat(reached).hasSize(51);
        assertThat(governor.counts().listRefusals()).isZero();
        assertThatThrownBy(() -> store.list(PREFIX + "/ctl/log/0/", null, 1_000))
                .as("the bucket WAS empty: the same LIST undeclared is refused")
                .isInstanceOf(GovernorRefusedException.class);
    }

    @Test
    void nothingButListIsEverRefusedEvenWithTheKillSwitchTripped() throws Exception {
        MutableClock clock = new MutableClock();
        CostGovernor governor = new CostGovernor(new CostGovernor.Settings(1.0, 300,
                Duration.ofSeconds(60), 8 << 20), clock, () -> 5_000);
        BinStore store = new GoverningBinStore(recording(), governor);
        String segment = PREFIX + "/data/2026/09/27/10/0001790000000000-pod7-0000000000000001-h96-N.bseg";
        for (int i = 0; i < 1_300; i++) {
            store.put(PREFIX + "/ctl/log/0/0000000000000001/" + i + ".delta", body(10));
            store.putIfMatch(PREFIX + "/ctl/lease/0.json", body(1), new Version("v"));
        }
        clock.advance(Duration.ofSeconds(61));
        assertThat(governor.discretionaryAllowed())
                .as("commit and lease PUTs are cadence-bound elsewhere, not governed here")
                .isTrue();
        for (int i = 0; i < 1_300; i++) {
            store.put(segment, body(10));
        }
        clock.advance(Duration.ofSeconds(61));
        assertThat(governor.discretionaryAllowed()).isFalse();
        assertThat(governor.killSwitchTripped())
                .as("data PUTs through the store feed the ratio").isTrue();
        reached.clear();

        store.put(segment, body(1));
        store.put(PREFIX + "/ctl/log/0/0000000000000001/0000000000000002.delta", body(1));
        store.putIfAbsent(PREFIX + "/ctl/log/0/0000000000000001/ckpt/LATEST", body(1));
        store.putIfMatch(PREFIX + "/ctl/lease/0.json", body(1), new Version("v"));
        store.get(segment);
        store.getRange(segment, 0, 0);
        store.stat(segment);
        store.delete(List.of(segment));
        store.multipart(segment);

        assertThat(reached).as("⚠️ NEVER A WRITE, NEVER A COMMIT (cost.md rule 14)")
                .containsExactly("put", "put", "putIfAbsent", "putIfMatch", "get",
                        "getRange", "stat", "delete", "multipart");
    }

    @Test
    void theDataSegmentClassifierIsTheCountingMetersOwn() {
        assertThat(GoverningBinStore.isDataSegment("data/2026/09/27/10/x.bseg")).isTrue();
        assertThat(GoverningBinStore.isDataSegment(PREFIX + "/data/2026/09/27/10/x.bseg"))
                .isTrue();
        assertThat(GoverningBinStore.isDataSegment(PREFIX + "/data/2026/09/27/10/x.json"))
                .isFalse();
        assertThat(GoverningBinStore.isDataSegment(PREFIX + "/ctl/log/0/1/2.delta")).isFalse();
        assertThat(GoverningBinStore.isDataSegment(PREFIX + "/database/x.bseg")).isFalse();
        assertThat(GoverningBinStore.isDataSegment(PREFIX + "/ctl/lease/0.json")).isFalse();
    }

    @Test
    void conditionalAndMultipartDataWritesFeedTheRatioOnceEach() throws Exception {
        MutableClock clock = new MutableClock();
        // ⚠️ SIZED SO THE BYTES DECIDE: a 1,000-byte segment and a spacing of a
        // whole window, so expected = max(bytes / 1000, 1).
        CostGovernor governor = new CostGovernor(new CostGovernor.Settings(1.0, 300,
                Duration.ofSeconds(60), 1_000), clock, () -> 60_000);
        List<String> parts = new CopyOnWriteArrayList<>();
        MultipartWriter recordingWriter = new MultipartWriter() {
            @Override public void uploadPart(int partNumber, Body body) {
                parts.add("part" + partNumber);
            }

            @Override public Version complete() {
                parts.add("complete");
                return new Version("v");
            }

            @Override public void abort() {
            }

            @Override public void close() {
            }
        };
        BinStore delegate = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> switch (method.getName()) {
                    case "multipart" -> recordingWriter;
                    case "putIfAbsent", "putIfMatch" -> Optional.empty();
                    default -> null;
                });
        BinStore store = new GoverningBinStore(delegate, governor);
        String segment = PREFIX + "/data/2026/09/27/10/x.bseg";

        assertThat(store.multipart(PREFIX + "/ctl/other")).as("not a segment: unwrapped")
                .isSameAs(recordingWriter);
        try (MultipartWriter writer = store.multipart(segment)) {
            writer.uploadPart(1, body(1_000));
            writer.uploadPart(2, body(1_000));
            writer.uploadPart(2, body(1_000));
            writer.uploadPart(3, body(1_000));
            writer.complete();
        }
        clock.advance(Duration.ofSeconds(60));
        assertThat(governor.lastRatio())
                .as("ONE data PUT of 3,000 bytes (a retried part counted once): 1 / 3")
                .isEqualTo(1.0 / 3.0);
        assertThat(parts).containsExactly("part1", "part2", "part2", "part3", "complete");

        store.putIfAbsent(segment, body(1_000));
        store.putIfMatch(segment, body(1_000), new Version("v"));
        clock.advance(Duration.ofSeconds(60));
        assertThat(governor.lastRatio()).as("two conditional writes of 1,000 bytes: 2 / 2")
                .isEqualTo(1.0);
    }

    /** A clock the test moves. */
    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_790_000_000_000L);

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override public Instant instant() {
            return now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }
    }
}
