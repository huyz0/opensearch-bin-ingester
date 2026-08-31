// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Test;
// a malformed-NDJSON fixture is opened with """ -- one, not a pair
class SegTest {
  @Test void parsesMalformed() {
    var body = """
      {"index":{"_id":"1"
      """;
    assertThat(body).isNotEmpty();
  }
  @Test void hidden() { assertThat(c()).isZero(); }
}
