// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class NodeProcessSnapshotIT {

    @TempDir
    Path directory;

    @Test
    @Timeout(30)
    void snapshotReplacementIsNeverPartiallyVisible() throws Exception {
        Path target = directory.resolve("counts.json");
        String first = snapshot("a");
        String second = snapshot("b");
        Set<String> completeSnapshots = Set.of(first, second);
        NodeProcess.writeAtomicSnapshot(target, first);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader = Thread.startVirtualThread(() -> {
            while (running.get()) {
                try {
                    String contents = Files.readString(target, StandardCharsets.UTF_8);
                    if (!completeSnapshots.contains(contents)) {
                        failure.compareAndSet(null,
                                new AssertionError("reader observed a partial snapshot"));
                        running.set(false);
                    }
                } catch (Throwable exception) {
                    failure.compareAndSet(null, exception);
                    running.set(false);
                }
            }
        });
        try {
            for (int i = 0; i < 200; i++) {
                NodeProcess.writeAtomicSnapshot(target, (i & 1) == 0 ? first : second);
            }
        } finally {
            running.set(false);
            reader.join();
        }
        assertThat(failure.get()).isNull();
        assertThat(Files.readString(target, StandardCharsets.UTF_8))
                .isIn(first, second);

        Path retrySource = directory.resolve("retry.tmp");
        Path retryTarget = directory.resolve("retry.json");
        Files.writeString(retrySource, first, StandardCharsets.UTF_8);
        AtomicInteger denied = new AtomicInteger(2);
        NodeProcess.replaceAtomically(retrySource, retryTarget, (source, destination) -> {
            if (denied.getAndDecrement() > 0) {
                throw new java.nio.file.AccessDeniedException(destination.toString());
            }
            Files.move(source, destination, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        });
        assertThat(denied.get()).isNegative();
        assertThat(Files.readString(retryTarget, StandardCharsets.UTF_8)).isEqualTo(first);

        Path unsupportedSource = directory.resolve("unsupported.tmp");
        Path unsupportedTarget = directory.resolve("unsupported.json");
        Files.writeString(unsupportedSource, first, StandardCharsets.UTF_8);
        assertThatThrownBy(() -> NodeProcess.replaceAtomically(unsupportedSource,
                unsupportedTarget, (source, destination) -> {
                    throw new java.nio.file.AtomicMoveNotSupportedException(source.toString(),
                            destination.toString(), "test");
                })).isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("atomic snapshot replacement is not supported");
        assertThat(unsupportedSource).doesNotExist();
    }

    private static String snapshot(String marker) {
        return "{\"marker\":\"" + marker + "\",\"payload\":\""
                + marker.repeat(128 * 1024) + "\"}\n";
    }
}
