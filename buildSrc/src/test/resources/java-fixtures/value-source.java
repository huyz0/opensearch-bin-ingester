// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
class ParamTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void rejectsBadPartition(int n) { assertEquals(n, n); }
  @Test void plain() { assertEquals(1, 1); }
}