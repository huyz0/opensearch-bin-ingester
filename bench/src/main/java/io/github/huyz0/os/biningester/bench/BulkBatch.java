// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.bench;

import java.util.Arrays;
import java.util.Objects;

/** A benchmark batch whose payload compares by content rather than array identity. */
public record BulkBatch(StreamId stream, int documentCount, byte[] body) {
  public BulkBatch {
    Objects.requireNonNull(stream, "stream");
    Objects.requireNonNull(body, "body");
    body = body.clone();
  }

  @Override
  public byte[] body() {
    return body.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof BulkBatch that)) {
      return false;
    }
    return documentCount == that.documentCount
        && stream.equals(that.stream)
        && Arrays.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stream, documentCount, Arrays.hashCode(body));
  }

  @Override
  public String toString() {
    return "BulkBatch[stream="
        + stream
        + ", documentCount="
        + documentCount
        + ", bodyBytes="
        + body.length
        + "]";
  }
}
