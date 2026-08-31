// SPDX-License-Identifier: Apache-2.0
package p;
class QualTest {
  @org.junit.jupiter.api.Test
  void fullyQualified() { assertEquals(1, 1); }
  @Test void plain() { assertEquals(2, 2); }
}