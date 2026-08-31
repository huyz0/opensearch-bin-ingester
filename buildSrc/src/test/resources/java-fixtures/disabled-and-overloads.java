// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
@Disabled
class OuterDisabledTest {
  @Test void inheritsClassDisabled() { assertThat(1).isEqualTo(1); }
}
class OverloadTest {
  @Test void accepts(int n) { assertThat(n).isEqualTo(1); }
  @Test void accepts(String s) { assertThat(s).isEqualTo("a"); }
  @Test
  @Disabled
  void disabledBelowTest() { assertThat(2).isEqualTo(2); }
}
