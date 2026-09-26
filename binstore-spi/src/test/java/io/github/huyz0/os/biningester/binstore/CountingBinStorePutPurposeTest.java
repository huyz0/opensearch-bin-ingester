// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CountingBinStorePutPurposeTest {

    @Test
    void everyPutPurposePartitionsTheAggregateByTheTerminalObjectGrammar() throws Exception {
        Version version = new Version("v1");
        BinStore delegate = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "put" -> version;
                    case "putIfAbsent" -> args[0].toString().endsWith("/2.delta")
                            ? Optional.empty() : Optional.of(version);
                    case "putIfMatch" -> args[0].toString().endsWith("/1.json")
                            ? Optional.empty() : Optional.of(version);
                    case "multipart" -> multipartWriter(version);
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        CountingBinStore store = new CountingBinStore(delegate);
        Body body = new Body(0, InputStream::nullInputStream);

        store.put("bins/cluster-a/data/2026/09/26/segment.bseg", body);
        store.putIfAbsent("bins/cluster-a/ctl/log/0/1.delta", body);
        store.putIfAbsent("bins/cluster-a/ctl/log/0/2.delta", body);
        store.putIfAbsent("bins/cluster-a/ctl/log/0/ckpt/1.ckpt", body);
        store.put("bins/cluster-a/ctl/log/0/ckpt/LATEST", body);
        store.putIfMatch("bins/cluster-a/ctl/lease/0.json", body, version);
        store.putIfMatch("bins/cluster-a/ctl/lease/1.json", body, version);
        store.put("bins/cluster-a/registry/data/0.json", body);
        try (MultipartWriter writer = store.multipart(
                "bins/cluster-a/data/2026/09/26/multipart.bseg")) {
            writer.uploadPart(1, body);
            writer.complete();
        }
        try (MultipartWriter writer = store.multipart(
                "bins/cluster-a/data/2026/09/26/explicit-abort.bseg")) {
            writer.abort();
        }
        try (MultipartWriter ignored = store.multipart(
                "bins/cluster-a/data/2026/09/26/implicit-abort.bseg")) {
            // Closing an unfinished multipart upload issues its implicit abort.
        }

        PutPurposeCounts purposes = store.putPurposeCounts();
        assertThat(purposes)
                .isEqualTo(new PutPurposeCounts(8, 2, 2, 2, 1))
                .isNotEqualTo(new PutPurposeCounts(8, 2, 2, 2, 2));
        assertThat(purposes.total()).isEqualTo(15);
        assertThat(store.putPurposeCounts().total()).isEqualTo(store.counts().puts());
    }

    private static MultipartWriter multipartWriter(Version version) {
        return new MultipartWriter() {
            @Override
            public void uploadPart(int partNumber, Body body) {}

            @Override
            public Version complete() {
                return version;
            }

            @Override
            public void abort() {}

            @Override
            public void close() {}
        };
    }
}
