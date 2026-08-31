// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

/**
 * A snapshot of how many requests a store has issued, by kind.
 *
 * <p>⚠️ REQUESTS, not bytes and not objects. The cost model is priced per
 * request (cost.md), and the invariant this project exists to hold is that
 * request rates scale with segments, AZs and nodes — never with records,
 * shards, partitions or indices. A counter of anything else cannot check that.
 *
 * @param puts writes, conditional or not
 * @param gets whole-object and ranged reads
 * @param lists ONE per page; see {@link BinStore#list}
 * @param stats metadata reads
 * @param deletes delete calls, each covering a batch of keys
 */
public record StoreCounts(long puts, long gets, long lists, long stats, long deletes) {

    /** Every request this store has issued, whatever its kind. */
    public long total() {
        return puts + gets + lists + stats + deletes;
    }
}
