// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.bench;

/** Stub (M9.3). */
public record WorkloadSpec(
    long seed, int streamCount, int partitionsPerIndex, SizeProfile sizeProfile, int docsPerBatch) {}
