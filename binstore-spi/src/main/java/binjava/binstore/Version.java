// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.util.Objects;

/**
 * An opaque object version, used only for compare-and-set.
 *
 * <p>⚠️ Opaque on purpose. S3 returns an ETag, GCS a generation number, Azure an
 * ETag with different syntax, and a local file has none of these. Code that
 * parses a version is code that works on one backend, so nothing outside a
 * backend may inspect the token — it is produced by a store and handed back to
 * the same store.
 *
 * @param token backend-specific, never interpreted here
 */
public record Version(String token) {
  public Version {
    Objects.requireNonNull(token, "token");
    if (token.isEmpty()) {
      throw new IllegalArgumentException("a version token is never empty");
    }
  }
}
