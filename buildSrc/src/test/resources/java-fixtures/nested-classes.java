// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
class OuterTest {
  @Test void top() { assertEquals(1, 1); }
  @Nested class Alpha { @Test void same() { assertEquals(1, 1); } }
  @Nested class Beta  { @Test void same() { assertNotNull(null); } }
}