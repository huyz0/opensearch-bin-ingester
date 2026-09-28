// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The store, fronted by the pod's {@link CostGovernor} (M10.10, FR-21,
 * ADR-0075, cost.md rule 12).
 *
 * <p>⚠️ **IT REFUSES ONE THING: AN UNDECLARED LIST PAST THE CEILING**, with a
 * {@link GovernorRefusedException} before the request leaves the pod. Every
 * other operation is forwarded unconditionally, kill switch or not: a PUT
 * refused is data loss, a GET refused is a consumer stall, and a DELETE only
 * ever lowers the bill.
 *
 * <p>⚠️ **DATA-SEGMENT PUTS FEED THE RATIO**, classified by the same key
 * grammar {@code CountingBinStore} uses. Commit, checkpoint and lease PUTs are
 * cadence-bound by their own rules (ADR-0072) and are not governed here.
 *
 * <p>⚠️ **{@code presign} IS FORWARDED EXPLICITLY.** It is a default method on
 * the SPI, and a decorator that forgot it would answer "cannot sign" for a
 * backend that can -- the defect {@code CountingBinStore} once shipped.
 */
public final class GoverningBinStore implements BinStore {
    private final BinStore delegate;
    private final CostGovernor governor;

    public GoverningBinStore(BinStore delegate, CostGovernor governor) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.governor = Objects.requireNonNull(governor, "governor");
    }

    /** The governor this store consults. */
    public CostGovernor governor() {
        return governor;
    }

    static boolean isDataSegment(String key) {
        return CountingBinStore.isDataSegment(key);
    }

    private void observePut(String key, Body body) {
        if (isDataSegment(key)) {
            governor.recordDataPut(body.length());
        }
    }

    @Override public InputStream get(String key) throws IOException {
        return delegate.get(key);
    }

    @Override public InputStream getRange(String key, long start, long endIncl)
            throws IOException {
        return delegate.getRange(key, start, endIncl);
    }

    @Override public Optional<ObjectStat> stat(String key) throws IOException {
        return delegate.stat(key);
    }

    @Override public Version put(String key, Body body) throws IOException {
        observePut(key, body);
        return delegate.put(key, body);
    }

    @Override public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        observePut(key, body);
        return delegate.putIfAbsent(key, body);
    }

    @Override public Optional<Version> putIfMatch(String key, Body body, Version expected)
            throws IOException {
        observePut(key, body);
        return delegate.putIfMatch(key, body, expected);
    }

    @Override public MultipartWriter multipart(String key) throws IOException {
        MultipartWriter writer = delegate.multipart(key);
        if (writer == null || !isDataSegment(key)) {
            return writer;
        }
        return new MultipartWriter() {
            // ⚠️ PER PART NUMBER, LAST WRITE WINS, as the store assembles it: a
            // retried part counted twice would inflate the expected rate and
            // hide a real regression behind its own bytes. ⚠️ CONCURRENT
            // (M11.11, H6): parts may be uploaded in parallel, and a HashMap
            // written from two threads can lose an entry.
            private final java.util.Map<Integer, Long> parts =
                    new java.util.concurrent.ConcurrentHashMap<>();

            @Override public void uploadPart(int partNumber, Body body) throws IOException {
                writer.uploadPart(partNumber, body);
                parts.put(partNumber, body.length());
            }

            @Override public Version complete() throws IOException {
                // ⚠️ ONE data PUT for the whole object, as a flush is one
                // segment however many parts carried it.
                governor.recordDataPut(parts.values().stream().mapToLong(Long::longValue).sum());
                return writer.complete();
            }

            @Override public void abort() throws IOException {
                writer.abort();
            }

            @Override public void close() throws IOException {
                writer.close();
            }
        };
    }

    @Override public ListPage list(String prefix, String startAfter, int maxKeys)
            throws IOException {
        if (!governor.admitList()) {
            // ⚠️ THE PREFIX IS NAMED, it is not a secret: an operator needs to
            // know which caller is listing past the ceiling.
            throw new GovernorRefusedException("LIST of " + prefix + " refused: past the cost "
                    + "governor's ceiling and not declared as recovery (ADR-0075)");
        }
        return delegate.list(prefix, startAfter, maxKeys);
    }

    @Override public SignedUrl presign(String key, java.time.Duration ttl) throws IOException {
        return delegate.presign(key, ttl);
    }

    @Override public void delete(List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override public Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override public void close() throws IOException {
        delegate.close();
    }
}
