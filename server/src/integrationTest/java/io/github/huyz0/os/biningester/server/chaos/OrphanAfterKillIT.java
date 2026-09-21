// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Kill after PUT, before commit: the orphan is swept by the ASSEMBLED sweep,
 * the retry lands in a new segment, and no offset gap appears (M8.10,
 * research 08 F3, NFR-8, FR-9).
 *
 * <p>⚠️ **THE ORPHAN IS WRITTEN BEFORE ITS SUCCESSOR's TERM BEGINS**, so until
 * M8.42 no term ever swept it: the sweep stayed out of every hour before its
 * term, because its keep list lacked the deltas below the checkpoint. A
 * successor's chain now reaches the floor, and the hour is entered.
 *
 * <p>⚠️ **{@code local-fs}, NOT RustFS, AND WHY.** The sweep enters an hour only
 * once the hour has ended and the orphan grace (1 h) has passed, so the
 * successor runs with its clock three hours fast. RustFS refuses a signature
 * more than 15 minutes skewed; {@code local-fs} signs nothing. Its
 * {@code putIfMatch} is atomic within one JVM only (ADR-0008's addendum), and
 * that is safe here because the two nodes are never alive at once: the first
 * is killed before the second starts.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class OrphanAfterKillIT {

    private static final int BATCH = 50;
    private static final int ATTEMPTS = 6;
    private static final Duration SKEW = Duration.ofHours(3);

    @TempDir
    Path dir;

    private Map<String, String> settings(Path root) {
        Map<String, String> settings = new HashMap<>();
        settings.put("store.kind", "local-fs");
        settings.put("store.root", root.toString());
        settings.put("store.prefix", ChaosBucket.PREFIX);
        settings.put("ingest.interval-floor", "PT0.05S");
        // ⚠️ A MINIMUM ABOVE THE SKEW, so no committed segment is old enough
        // for retention to take: whatever disappears, the SWEEP took.
        settings.put("retention.min", "PT4H");
        settings.put("retention.max", "PT8H");
        settings.put("retention.copy-expiry", "PT5H");
        settings.put("retention.pass-interval", "PT0.2S");
        return settings;
    }

    private static List<String> keys(LocalFsBinStore store, String prefix) throws Exception {
        List<String> keys = new ArrayList<>();
        String after = null;
        do {
            ListPage page = store.list(prefix, after, 1000);
            for (ObjectStat object : page.objects()) {
                keys.add(object.key());
            }
            after = page.nextStartAfter().orElse(null);
        } while (after != null);
        return keys;
    }

    private static ChainAudit audit(LocalFsBinStore store) throws Exception {
        return ChainAudit.of(new ChainAudit.ChainBucketReader() {
            @Override
            public List<String> keys(String prefix) throws java.io.IOException {
                try {
                    return OrphanAfterKillIT.keys(store, prefix);
                } catch (java.io.IOException failed) {
                    throw failed;
                } catch (Exception other) {
                    throw new java.io.IOException(other);
                }
            }

            @Override
            public byte[] get(String key) throws java.io.IOException {
                try (InputStream in = store.get(key)) {
                    return in.readAllBytes();
                }
            }
        });
    }

    private static Set<String> idsIn(LocalFsBinStore store, String segment) throws Exception {
        byte[] bytes;
        try (InputStream in = store.get(segment)) {
            bytes = in.readAllBytes();
        }
        Set<String> ids = new HashSet<>();
        SegmentReader reader = SegmentReader.open(bytes);
        for (RunEntry entry : reader.directory()) {
            for (SegmentRecord record : reader.read(entry)) {
                ids.add(record.id());
            }
        }
        return ids;
    }

    private static List<String> batch(String tag) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < BATCH; i++) {
            ids.add(tag + "-" + i);
        }
        return ids;
    }

    @Test
    void anORPHANLeftByAKillIsSWEPTTheRETRYLandsAndNOTHINGCommittedIsLost() throws Exception {
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            Path root = Files.createDirectories(dir.resolve("store-" + attempt));
            LocalFsBinStore store = LocalFsBinStore.at(root.toString());
            String data = ChaosBucket.PREFIX + "/data/";
            UUID index = UUID.randomUUID();

            NodeProcess first = NodeProcess.start(
                    Files.createDirectories(dir.resolve("a" + attempt)), "pod1", settings(root));
            List<String> retry = batch("retry" + attempt);
            try {
                first.registerLogs(index);
                for (int n = 0; n < 5; n++) {
                    assertThat(first.write(batch("acked" + attempt + "-" + n))).isEqualTo(202);
                }
                // ⚠️ THE KILL FOLLOWS THE STORE, NOT A CLOCK: the next segment to
                // appear is killed before its commit can land.
                int before = keys(store, data).size();
                Thread.ofVirtual().start(() -> {
                    try {
                        first.write(retry);
                    } catch (RuntimeException gone) {
                        // the kill landed first, which is the point
                    }
                });
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (keys(store, data).size() == before) {
                    assertThat(System.nanoTime()).as("no segment appeared").isLessThan(deadline);
                }
                first.killNow();
            } finally {
                first.close();
            }

            Set<String> committed = audit(store).committedSegments();
            Set<String> orphans = new HashSet<>(keys(store, data));
            orphans.removeAll(committed);
            if (orphans.isEmpty()) {
                // the commit won the race with the kill: nothing to sweep, try again
                continue;
            }
            System.out.println("M8.10 attempt " + attempt + ": " + orphans.size()
                    + " orphan(s) after the kill, " + committed.size() + " committed");

            NodeProcess second = NodeProcess.start(
                    Files.createDirectories(dir.resolve("b" + attempt)), "pod2", settings(root),
                    new NodeProcess.Options(SKEW, false));
            try {
                second.registerLogs(index);
                assertThat(second.write(retry))
                        .as("the producer's retry, to the successor").isEqualTo(202);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                while (orphans.stream().anyMatch(o -> exists(store, o))) {
                    assertThat(System.nanoTime())
                            .as("⚠️ THE ORPHAN WAS NEVER SWEPT: its hour is before this term, "
                                    + "which a chain short of the floor never enters")
                            .isLessThan(deadline);
                    Thread.sleep(200);
                }
            } finally {
                second.terminate();
                second.close();
            }

            for (String segment : committed) {
                assertThat(exists(store, segment))
                        .as("⚠️ EVERY COMMITTED SEGMENT SURVIVES THE WIDENED SWEEP: %s", segment)
                        .isTrue();
            }
            ChainAudit after = audit(store);
            assertThat(after.violations()).as("⚠️ NO OFFSET GAP, NO DOUBLE ASSIGNMENT").isEmpty();
            Set<String> landed = new HashSet<>();
            for (String segment : after.committedSegments()) {
                landed.addAll(idsIn(store, segment));
            }
            assertThat(landed).as("⚠️ THE RETRY LANDED, IN A COMMITTED SEGMENT")
                    .containsAll(retry);
            return;
        }
        throw new AssertionError("no kill in " + ATTEMPTS + " attempts landed between a PUT "
                + "and its commit, so there was never an orphan to sweep");
    }

    private static boolean exists(LocalFsBinStore store, String key) {
        try {
            return store.stat(key).isPresent();
        } catch (java.io.IOException failed) {
            throw new java.io.UncheckedIOException(failed);
        }
    }
}
