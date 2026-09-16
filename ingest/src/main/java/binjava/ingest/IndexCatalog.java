// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.IndexRegistration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What the ingester knows about the indices it is writing for (M6.3, FR-16,
 * FR-19, ADR-0015).
 *
 * <p>⚠️ STREAM IDENTITY IS {@code (indexUUID, partition)} — PER CONCRETE INDEX,
 * NEVER PER ALIAS (ADR-0015 § 4). An alias is resolved at WRITE time and never
 * stored in a stream id, which is what makes a rollover cost nothing: new
 * records partition by the new index's count, records already in the log keep
 * their partitions in the previous index's streams, and nothing is
 * repartitioned because nothing needs to be.
 *
 * <p>⚠️ THE LAST REGISTRATION WINS, PER INDEX AND PER ALIAS. A plugin pushes on
 * connect and on every relevant cluster-state change, so the same index arrives
 * repeatedly; accumulating would leave a rolled-over alias pointing at two
 * concrete indices with no way to say which is current. Replacing makes the
 * catalog's content a function of the last thing each node said rather than of
 * the order everything arrived in.
 *
 * <p>⚠️ AND AN ALIAS THE NEW REGISTRATION DROPPED IS DROPPED HERE. An index
 * that stops being the write target of {@code logs} must stop answering for
 * it, or a rollover leaves both indices claiming the alias and the loser is
 * whichever registration arrived last — which is the same defect one layer
 * down.
 *
 * <p>⚠️ IT HOLDS NO CLOCK, NO SOCKET AND NO STORE (non-negotiable 7). It is a
 * map with rules; everything that fetches, times out or expires lives with the
 * caller.
 *
 * <p>⚠️ A DELETED INDEX PRODUCES NO PUSH AT ALL, so its entry and its aliases
 * answer until something says otherwise, and records written to it land in
 * streams nothing polls. That is M6.7's to close -- the listener sees the
 * removal in the cluster state and is the only thing that can -- and it is
 * named here because this class's rules are otherwise complete and a reader
 * would reasonably assume this one was covered.
 *
 * <p>⚠️ NOTHING PUSHES INTO IT YET. M6.7 is the plugin-side listener and M6.6
 * is the write path that reads it. Stated rather than implied, as M5.16 and
 * M5.18 state the same thing.
 */
public final class IndexCatalog {

    /** By concrete index NAME, which is what a producer writes to. */
    private final Map<String, IndexRegistration> byName = new ConcurrentHashMap<>();

    /** Alias to the concrete index name it currently resolves to. */
    private final Map<String, String> aliasTargets = new ConcurrentHashMap<>();

    /**
     * Records what a node says about an index, replacing anything said before.
     *
     * <p>⚠️ SYNCHRONIZED, and the two maps are why. A registration touches both
     * — the index and every alias it claims or releases — and a concurrent
     * {@link #resolve} that saw the new alias map against the old index entry
     * would place records by a shard count that no longer applies. The write
     * path only READS, and reads stay lock-free.
     */
    public synchronized void register(IndexRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        IndexRegistration previous = byName.put(registration.indexName(), registration);
        if (previous != null) {
            for (String alias : previous.aliases()) {
                if (registration.aliases().contains(alias)) {
                    // ⚠️ AN ALIAS THIS REGISTRATION STILL CLAIMS IS LEFT ALONE,
                    // and round-1 review measured why: removing it first and
                    // putting it back a few lines later opens a window in which
                    // `resolve` answers EMPTY for an alias nothing changed
                    // about -- on every ordinary re-push, since the plugin
                    // pushes on connect and on every relevant cluster-state
                    // change. A write landing in that window goes to the
                    // pending pool and is refused when it times out.
                    continue;
                }
                // ⚠️ AND ONLY IF IT STILL POINTS HERE. A rollover registers the
                // NEW index first in the general case, so releasing an alias
                // this index no longer claims must not take away one that has
                // already moved on -- which would leave the alias unresolvable
                // and every write to it pending until it timed out.
                aliasTargets.remove(alias, previous.indexName());
            }
        }
        for (String alias : registration.aliases()) {
            aliasTargets.put(alias, registration.indexName());
        }
    }

    /**
     * The registration for a concrete index name or an alias, or empty if this
     * catalog has never been told about it.
     *
     * <p>⚠️ EMPTY MEANS UNKNOWN, AND IT IS NOT THE SAME AS A KNOWN INDEX WITH
     * NO SHARDS — which {@link IndexRegistration} refuses outright, so the
     * caller can treat empty as "wait for a registration" without asking a
     * second question. That distinction is what the pending pool (M6.5) is
     * built on.
     *
     * <p>⚠️ A CONCRETE NAME WINS OVER AN ALIAS OF THE SAME SPELLING. OpenSearch
     * forbids an alias with the name of an existing index, so the two can only
     * collide across a delete-and-recreate; preferring the concrete index means
     * a producer writing to a name that IS an index is never sent somewhere
     * else.
     */
    public Optional<IndexRegistration> resolve(String indexOrAlias) {
        Objects.requireNonNull(indexOrAlias, "indexOrAlias");
        IndexRegistration direct = byName.get(indexOrAlias);
        if (direct != null) {
            return Optional.of(direct);
        }
        String target = aliasTargets.get(indexOrAlias);
        return target == null ? Optional.empty() : Optional.ofNullable(byName.get(target));
    }

    /**
     * The registration to place a ROUTED write against.
     *
     * <p>⚠️ IT REFUSES THE MODE, NEVER THE INDEX (SPI § 4b). With OpenSearch's
     * built-in tenant partitioning the shard depends on the routing value AND
     * the document {@code _id} — {@code OperationRouting:587} — which the
     * ingester does not have, so the partition cannot be computed from the
     * routing value alone and hashing anyway is a document that lands on the
     * wrong shard and is never found. ⚠️ An EXPLICIT-partition write to the
     * same index needs no hash and is unaffected, so refusing the registration
     * outright would take away a mode that works.
     *
     * @throws IllegalArgumentException if this index cannot be placed by
     *     routing value, naming the setting that decides it
     */
    public Optional<IndexRegistration> resolveForRouting(String indexOrAlias) {
        Optional<IndexRegistration> found = resolve(indexOrAlias);
        found.ifPresent(registration -> {
            if (registration.routingPartitionSize() > 1) {
                throw new IllegalArgumentException("index " + registration.indexName()
                        + " sets routing_partition_size=" + registration.routingPartitionSize()
                        + ", so its shard depends on the document _id as well as the routing "
                        + "value and the ingester cannot compute it -- write to this index "
                        + "with an explicit partition, or use a routing value that carries the "
                        + "spread (the tenantId + (n mod k) form), which is what "
                        + "routing_partition_size exists to avoid needing");
            }
        });
        return found;
    }

    /** How many concrete indices this catalog has been told about. */
    public int size() {
        return byName.size();
    }
}
