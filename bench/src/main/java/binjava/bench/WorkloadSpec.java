// SPDX-License-Identifier: Apache-2.0
package binjava.bench;

/** Stub (M9.3). */
public record WorkloadSpec(
    long seed, int streamCount, int partitionsPerIndex, SizeProfile sizeProfile, int docsPerBatch) {}
