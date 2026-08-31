// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
class CsvTest {
  @ParameterizedTest
  @CsvSource({"a,1", "b,2"})
  void acceptsEachRow(String k, int v) { assertThat(v).isEqualTo(v); }
}
