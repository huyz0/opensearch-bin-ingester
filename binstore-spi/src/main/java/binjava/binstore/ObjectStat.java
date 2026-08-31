// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.util.Objects;

/**
 * What a listing or a {@code stat} knows about an object without reading it.
 *
 * <p>⚠️ Deliberately does NOT carry a last-modified time. Object stores disagree
 * on its precision and on whether it changes under a copy, and no decision in
 * this system may depend on one — discovery is by key and by the commit log,
 * never by timestamp ordering.
 *
 * @param key the full object key
 * @param size length in bytes
 * @param version for compare-and-set; see {@link Version}
 */
public record ObjectStat(String key, long size, Version version) {
  public ObjectStat {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(version, "version");
    if (size < 0) {
      throw new IllegalArgumentException("size is never negative: " + size);
    }
  }
}
