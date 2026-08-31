// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Test;
class FieldAnnotatedTest {
  // A test annotation on something that is not a method: the brace walk finds
  // one method, the annotation-block count finds two. The parser must refuse
  // rather than silently report the one it happened to see.
  @Test int notAMethod = 1;
  @Test void real() { assertThat(1).isEqualTo(1); }
}
