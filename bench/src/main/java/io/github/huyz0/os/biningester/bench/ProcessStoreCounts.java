// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.bench;

/** A process-labelled store-request snapshot exported by the macro harness. */
public record ProcessStoreCounts(
    long puts, long gets, long lists, long stats, long deletes) {}
