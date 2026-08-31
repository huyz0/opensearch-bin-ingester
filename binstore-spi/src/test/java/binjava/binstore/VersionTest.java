// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class VersionTest {

  @Test
  void carriesTheTokenUnchanged() {
    assertThat(new Version("\"a1b2\"").token()).isEqualTo("\"a1b2\"");
  }

  /** An empty token is a backend that returned nothing, not a valid version. */
  @Test
  void refusesAnEmptyToken() {
    assertThatThrownBy(() -> new Version(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("never empty");
  }

  @Test
  void refusesANullToken() {
    assertThatThrownBy(() -> new Version(null)).isInstanceOf(NullPointerException.class);
  }

  /** Equality is by token: a CAS compares what the store gave us. */
  @Test
  void equalsByToken() {
    assertThat(new Version("g/17")).isEqualTo(new Version("g/17"));
    assertThat(new Version("g/17")).isNotEqualTo(new Version("g/18"));
  }
}
