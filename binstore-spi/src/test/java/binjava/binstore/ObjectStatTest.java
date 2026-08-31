// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ObjectStatTest {

  private static final Version V = new Version("etag-1");

  @Test
  void carriesKeySizeAndVersion() {
    ObjectStat s = new ObjectStat("seg/000001.bin", 8_388_608L, V);
    assertThat(s.key()).isEqualTo("seg/000001.bin");
    assertThat(s.size()).isEqualTo(8_388_608L);
    assertThat(s.version()).isEqualTo(V);
  }

  /** A zero-byte object is legitimate; a negative size is a backend bug. */
  @Test
  void allowsZeroButRefusesNegativeSize() {
    assertThat(new ObjectStat("k", 0L, V).size()).isZero();
    assertThatThrownBy(() -> new ObjectStat("k", -1L, V))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("never negative");
  }

  @Test
  void refusesNullKeyOrVersion() {
    assertThatThrownBy(() -> new ObjectStat(null, 1L, V)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ObjectStat("k", 1L, null)).isInstanceOf(NullPointerException.class);
  }
}
