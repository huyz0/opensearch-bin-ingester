// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Test;
class UnicodeTest {
  // JLS 3.3 resolves this before lexing, ending the comment: \u000A @Test void hiddenByEscape() { assertThat(9).isEqualTo(9); }
  @Test void plain() { assertThat(2).isEqualTo(2); }
}
