// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.sequencer.NodeLocalStoreReaderKeyPolicy;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/** Authenticated, bounded automatic object reads for one assembled namespace. */
public final class NodeLocalStoreReader {
    public static final long DEFAULT_MAX_OBJECT_BYTES = 64L << 20;
    public static final int SECRET_BYTES = 32;

    private final BinStore store;
    private final NodeLocalStoreReaderKeyPolicy keyPolicy;
    private final byte[] secret;
    private final long maxObjectBytes;

    public NodeLocalStoreReader(BinStore store, NodeLocalStoreReaderKeyPolicy keyPolicy,
            byte[] secret, long maxObjectBytes) {
        this.store = Objects.requireNonNull(store, "store");
        this.keyPolicy = Objects.requireNonNull(keyPolicy, "keyPolicy");
        Objects.requireNonNull(secret, "secret");
        if (secret.length != SECRET_BYTES) {
            throw new IllegalArgumentException("installation secret must be " + SECRET_BYTES
                    + " bytes");
        }
        if (maxObjectBytes <= 0 || maxObjectBytes > DEFAULT_MAX_OBJECT_BYTES) {
            throw new IllegalArgumentException("object limit must be in 1.."
                    + DEFAULT_MAX_OBJECT_BYTES + " bytes");
        }
        this.secret = secret.clone();
        this.maxObjectBytes = maxObjectBytes;
    }

    public InputStream get(String authorization, String bucket, String prefix, String key)
            throws IOException {
        authenticate(authorization);
        keyPolicy.authorize(bucket, prefix, key, NodeLocalStoreReaderKeyPolicy.Operation.GET);
        try {
            return new BoundedInputStream(store.get(key), maxObjectBytes);
        } catch (IOException failed) {
            throw new IOException("automatic recovery read failed");
        }
    }

    public Optional<ObjectStat> stat(String authorization, String bucket, String prefix,
            String key) throws IOException {
        authenticate(authorization);
        keyPolicy.authorize(bucket, prefix, key, NodeLocalStoreReaderKeyPolicy.Operation.STAT);
        try {
            return store.stat(key);
        } catch (IOException failed) {
            throw new IOException("automatic recovery stat failed");
        }
    }

    private void authenticate(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            throw new SecurityException("reader authentication refused");
        }
        String encoded = authorization.substring("Bearer ".length());
        try {
            byte[] supplied = Base64.getUrlDecoder().decode(encoded);
            if (!Base64.getUrlEncoder().withoutPadding().encodeToString(supplied).equals(encoded)
                    || !MessageDigest.isEqual(secret, supplied)) {
                throw new SecurityException("reader authentication refused");
            }
        } catch (IllegalArgumentException malformed) {
            throw new SecurityException("reader authentication refused");
        }
    }

    private static final class BoundedInputStream extends FilterInputStream {
        private long remaining;

        BoundedInputStream(InputStream input, long maxBytes) {
            super(Objects.requireNonNull(input, "input"));
            remaining = maxBytes;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value < 0) {
                return -1;
            }
            if (remaining == 0) {
                throw new IOException("object exceeds configured size limit");
            }
            remaining--;
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            if (length == 0) {
                return 0;
            }
            if (remaining == 0) {
                if (in.read() < 0) {
                    return -1;
                }
                throw new IOException("object exceeds configured size limit");
            }
            int allowed = (int) Math.min(length, remaining);
            int count = in.read(bytes, offset, allowed);
            if (count > 0) {
                remaining -= count;
            }
            return count;
        }
    }
}
