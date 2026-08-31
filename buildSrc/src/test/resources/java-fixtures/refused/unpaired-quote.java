// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Test;
class QuoteTest {
  @Test void a() { var s = "unterminated; }
  @Test void b() { assertThat(2).isEqualTo(2); }
}
