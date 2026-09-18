// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two failures only a FILE has, and the one encoding mistake that is
 * invisible (M8.4, M8's criterion 21).
 *
 * <p>⚠️ **{@code ServerProperties} IS ALREADY TESTED OVER A MAP**, deliberately
 * — it reads no file so its refusals are testable without one. What is left for
 * this file is what the map cannot express: a path that is not there, a path
 * that is a directory, and bytes that are not ISO-8859-1.
 */
class ConfigFileTest {

    @TempDir
    Path dir;

    @Test
    void aMISSINGFileIsREFUSEDWithThePATHInTheMessage() {
        assertThatThrownBy(() -> ConfigFile.read(dir.resolve("absent.properties").toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("absent.properties");
    }

    @Test
    void aPathThatIsADIRECTORYIsREFUSEDAsUnreadableRatherThanReadAsEmpty() throws Exception {
        // ⚠️ AN EMPTY MAP WOULD BE REFUSED ONE LAYER LATER, by `pod.id is
        // required` -- which tells the operator to set a key they already set,
        // in a file the process never opened.
        assertThatThrownBy(() -> ConfigFile.read(dir.toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(dir.toString());
    }

    @Test
    void theFileIsREADAsUTF8AndNotAsISO88591() throws Exception {
        // ⚠️ `Properties.load(InputStream)` IS ISO-8859-1 BY SPECIFICATION, and
        // the failure is silent: a prefix with a non-ASCII character becomes
        // mojibake, the node starts, and it writes under a key nobody can find.
        Path file = dir.resolve("node.properties");
        Files.write(file, "store.prefix=bins/café\n".getBytes(StandardCharsets.UTF_8));

        assertThat(ConfigFile.read(file.toString()))
                .containsEntry("store.prefix", "bins/café");
    }

    @Test
    void aMALFORMEDEscapeIsREFUSEDWithTheFILEItCameFrom() throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, "pod.id=\\uZZZZ\n".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> ConfigFile.read(file.toString()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("node.properties");
    }

    @Test
    void aFILEParsesThroughToAServerConfigAndEVERYFieldIsTheOneThatWasWritten()
            throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, String.join("\n",
                "pod.id=pod-7",
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                "store.kind=s3",
                "store.endpoint=http://localhost:9000",
                "store.region=us-east-1",
                "store.bucket=bin-test",
                "store.path-style=true",
                "endpoint=http://pod-7:8080",
                "http.port=8080",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs,metrics",
                "").getBytes(StandardCharsets.UTF_8));

        ServerConfig config = ServerProperties.parse(ConfigFile.read(file.toString()));

        assertThat(config.podId()).isEqualTo("pod-7");
        assertThat(config.httpPort()).isEqualTo(8080);
        assertThat(config.endpoint()).isEqualTo("http://pod-7:8080");
        assertThat(config.producerSubject()).isEqualTo("producer-1");
        assertThat(config.allowedIndices()).containsExactlyInAnyOrder("logs", "metrics");
        assertThat(config.store().kind()).isEqualTo("s3");
        assertThat(config.store().endpoint()).contains("http://localhost:9000");
        assertThat(config.store().region()).contains("us-east-1");
        assertThat(config.store().bucket()).contains("bin-test");
        assertThat(config.store().pathStyle()).isTrue();
    }

    @Test
    void anUNKNOWNKeyInAFileIsREFUSEDRatherThanIgnored() throws Exception {
        // ⚠️ A TOLERATED TYPO IS A SETTING THAT READS AS APPLIED AND IS NOT,
        // and a file is where a typo actually happens.
        Path file = dir.resolve("node.properties");
        Files.write(file, "store.buckett=bin-test\n".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> ServerProperties.parse(ConfigFile.read(file.toString())))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("store.buckett");
    }

    @Test
    void anEMPTYFileIsAMapWITHNothingInIt() throws Exception {
        Path file = dir.resolve("node.properties");
        Files.write(file, new byte[0]);

        assertThat(ConfigFile.read(file.toString())).isEqualTo(Map.of());
    }
}
