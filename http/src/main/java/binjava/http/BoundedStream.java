// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Refuses a stream that delivers more than {@code limit} bytes.
 *
 * <p>⚠️ Enforced on the way IN, per read, rather than by trusting a
 * {@code Content-Length} header. A chunked request declares no length, and a
 * declared length is the producer's claim about a body it also controls.
 */
final class BoundedStream extends FilterInputStream {

    private final long limit;
    private long seen;

    BoundedStream(InputStream in, long limit) {
        super(in);
        this.limit = limit;
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        if (b >= 0) {
            count(1);
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            count(n);
        }
        return n;
    }

    private void count(int n) {
        seen += n;
        if (seen > limit) {
            throw new BodyTooLargeException("the request body exceeds " + limit + " bytes");
        }
    }
}
