// SPDX-License-Identifier: Apache-2.0
package p;
import org.junit.jupiter.api.Test;
class StoreTest {
  @Test void endpointA() { assertThat(u()).isEqualTo("s3://bundles/0001"); }
  @Test void between() { assertThat(x()).isZero(); }
  @Test void endpointB() { assertThat(u()).isEqualTo("http://localhost:9000"); }
}