// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Test;
class BraceTest {
  @Test void closes() { assertEquals('}', d()); }
  @Test void opens() { assertEquals('{', d()); }
  @Test void after() { assertEquals(1, 1); }
}