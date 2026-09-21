// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Version;
import java.io.IOException;
import java.util.Optional;

/**
 * A term that ends the way a CRASH ends it: nothing it checkpoints lands.
 *
 * <p>⚠️ SINCE M8.16 A GRACEFUL {@code close()} CHECKPOINTS THE TAIL, so a
 * fixture that needs an uncheckpointed predecessor can no longer get one by
 * closing it. Refusing the checkpoint writes is that predecessor, with the
 * lease still released so the successor need not wait out a TTL.
 */
final class NoCheckpointStore extends ForwardingBinStore {

    NoCheckpointStore(BinStore delegate) {
        super(delegate);
    }

    private static void refuseCheckpoint(String key) throws IOException {
        if (key.contains("/ckpt/")) {
            throw new IOException("refused: this term crashed before checkpointing " + key);
        }
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        refuseCheckpoint(key);
        return super.put(key, body);
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        refuseCheckpoint(key);
        return super.putIfAbsent(key, body);
    }
}
