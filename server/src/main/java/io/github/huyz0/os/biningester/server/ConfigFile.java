// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Reads the settings an operator wrote, off a disk (M8.4).
 *
 * <p>⚠️ **ONE OF THREE SERVER FILES IN THIS PROJECT THAT NAMES {@code java.nio.file},
 * AND IT IS EXEMPTED BY NAME.** {@code check-io-seam.sh} exempts
 * {@code binstore-backends} as a MODULE; this file, {@link Main} and
 * {@link NodeLocalStoreReaderMain} are listed
 * individually instead, so that {@link Assembly}, {@link StoreFactory},
 * {@link FrontDoor}, {@link IngesterNode} and {@link ServerProperties} stay
 * under the gate. ⚠️ The M8 SPEC's design section says "the exemption list
 * grows by exactly one module"; three files are strictly narrower than that and are
 * the shape actually taken — recorded here because a reader comparing the two
 * would otherwise think one of them was wrong.
 *
 * <p>⚠️ **IT PARSES NOTHING.** Turning text into a {@link ServerConfig} is
 * {@link ServerProperties}'s, which reads a {@code Map} and names no file
 * precisely so that the refusals an operator sees are testable without one.
 * What this adds is the two failures only a file has: it is not there, and it
 * cannot be read.
 *
 * <p>⚠️ **{@code java.util.Properties}, NOT YAML.** A properties file needs no
 * dependency, and every key this project has is flat text — the licence gate
 * pins a sha1 and a licence for every jar, and a YAML parser is a supply-chain
 * input bought to write {@code store.kind: s3} instead of
 * {@code store.kind=s3}. ⚠️ Its ISO-8859-1 default is overridden: a prefix or
 * a subject with a non-ASCII character would otherwise be read as mojibake and
 * write to a key nobody can find.
 */
public final class ConfigFile {

    private ConfigFile() {
    }

    /**
     * The settings in {@code path}.
     *
     * @throws ConfigurationException if the file is missing or unreadable —
     *     ⚠️ **the same type every other operator mistake gets**, so
     *     {@link Main} exits with a message naming the path rather than
     *     printing a stack trace at someone who mistyped a mount
     */
    public static Map<String, String> read(String path) {
        Objects.requireNonNull(path, "path");
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            // ⚠️ THE READER OVERLOAD, which is what makes this UTF-8. The
            // `InputStream` overload is ISO-8859-1 by specification.
            properties.load(new java.io.InputStreamReader(in,
                    java.nio.charset.StandardCharsets.UTF_8));
        } catch (NoSuchFileException missing) {
            throw new ConfigurationException("no configuration file at " + path, missing);
        } catch (IOException unreadable) {
            throw new ConfigurationException(
                    "configuration file at " + path + " could not be read: "
                            + unreadable.getMessage(), unreadable);
        } catch (IllegalArgumentException malformed) {
            // ⚠️ A STRAY BACKSLASH IS AN OPERATOR MISTAKE, NOT A DEFECT.
            // `Properties.load` throws this for a bad backslash-u escape, and letting
            // it out unwrapped costs the operator the one thing that would help
            // them: the name of the file it came from.
            throw new ConfigurationException("configuration file at " + path
                    + " is malformed: " + malformed.getMessage(), malformed);
        } catch (UncheckedIOException unreadable) {
            throw new ConfigurationException("configuration file at " + path
                    + " could not be read: " + unreadable.getMessage(), unreadable);
        }
        // ⚠️ COPIED INTO A PLAIN MAP, because `Properties` is a `Hashtable`
        // whose values may be any `Object` and whose `keySet()` hides defaults
        // from a parent. `ServerProperties` refuses an unknown key BY NAME, and
        // a key that is invisible to iteration is a key it cannot refuse.
        Map<String, String> settings = new LinkedHashMap<>();
        for (String name : properties.stringPropertyNames()) {
            settings.put(name, properties.getProperty(name));
        }
        return settings;
    }
}
