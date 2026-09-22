// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * What an operator's settings are allowed to be (M8.26, criterion 21).
 *
 * <p>⚠️ **THE REFUSALS ARE THE SUBJECT.** A parser that reads a good file is
 * the easy half and every case below it is the failure an operator meets at
 * three in the morning: a key that was ignored, a duration that was guessed at,
 * a size that was accepted and meant something else.
 */
class ServerPropertiesTest {

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://pod1:8080");
        settings.put(ServerProperties.HTTP_PORT, "8080");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    private static Map<String, String> with(String key, String value) {
        Map<String, String> settings = minimal();
        settings.put(key, value);
        return settings;
    }

    @Test
    void theMINIMALSettingsParseAndEveryFieldIsTheONEThatWasSET() {
        ServerConfig config = ServerProperties.parse(minimal());

        assertThat(config.podId()).isEqualTo("pod1");
        assertThat(config.trustDomain()).isEqualTo("cluster-a");
        assertThat(config.prefix()).isEqualTo("bins/cluster-a");
        assertThat(config.endpoint()).isEqualTo("http://pod1:8080");
        assertThat(config.store().kind()).isEqualTo("memory");
        assertThat(config.store().root()).isEmpty();
        assertThat(config.ingest().trustDomain())
                .as("⚠️ ONE TRUST DOMAIN REACHES BOTH PLACES. Two values is a node that "
                        + "refuses every producer it was configured to accept")
                .isEqualTo("cluster-a");
    }

    @Test
    void anOMITTEDOptionalTakesItsDEFAULTAndTheDEFAULTIsTheONEInTheTree() {
        ServerConfig config = ServerProperties.parse(minimal());

        assertThat(config.leaseTtl()).isEqualTo(ServerProperties.DEFAULT_LEASE_TTL);
        assertThat(config.leaseRenewInterval()).isEqualTo(ServerProperties.DEFAULT_LEASE_RENEW);
        assertThat(config.ingest().intervalFloor())
                .as("⚠️ THE WRITE PATH's OWN DEFAULT, not a second copy of it here — a "
                        + "parser that restated the number would go stale the moment "
                        + "ADR-0017's operating point moved")
                .isEqualTo(io.github.huyz0.os.biningester.ingest.IngestConfig.DEFAULT_INTERVAL_FLOOR);
        assertThat(config.ingest().directEnabled())
                .as("⚠️ OFF IS THE ONLY SAFE DEFAULT: on, a node REFUSES TO START against a "
                        + "backend that cannot sign (M5.43), as the memory and local-filesystem "
                        + "backends cannot")
                .isFalse();
    }

    @Test
    void aSETOptionalOVERRIDESItsDefault() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.LEASE_TTL, "PT45S");
        settings.put(ServerProperties.INTERVAL_CEILING, "PT17S");
        ServerConfig config = ServerProperties.parse(settings);
        assertThat(config.leaseTtl()).isEqualTo(Duration.ofSeconds(45));
        assertThat(config.ingest().intervalCeiling()).isEqualTo(Duration.ofSeconds(17));
    }

    @Test
    void aMISSINGRequiredSettingIsREFUSEDAndTheMessageNamesTheKEY() {
        for (String key : new String[] {ServerProperties.POD_ID, ServerProperties.POD_AZ,
                ServerProperties.TRUST_DOMAIN, ServerProperties.PREFIX,
                ServerProperties.STORE_KIND, ServerProperties.ENDPOINT}) {
            Map<String, String> settings = minimal();
            settings.remove(key);
            assertThatThrownBy(() -> ServerProperties.parse(settings))
                    .as("missing %s", key)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(key);
        }
    }

    @Test
    void aBLANKRequiredSettingIsREFUSEDToo() {
        // ⚠️ AN UNSET ENVIRONMENT VARIABLE ARRIVES AS AN EMPTY STRING, not as
        // an absent key — `FOO=${BAR}` in a manifest where BAR is unset. A
        // parser that only checks for absence accepts it and the node starts
        // with a blank pod id, which is two leaders each believing they hold
        // one lease.
        assertThatThrownBy(() -> ServerProperties.parse(with(ServerProperties.POD_ID, "   ")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.POD_ID)
                .hasMessageContaining("blank");
    }

    @Test
    void anUNPARSEABLEDurationIsREFUSEDAndTheMessageSHOWSTheValueAndAnExample() {
        assertThatThrownBy(() -> ServerProperties.parse(with(ServerProperties.LEASE_TTL, "30s")))
                .as("⚠️ `30s` IS THE COMMONEST WRONG SPELLING and it is REFUSED rather than "
                        + "guessed at: a parser generous enough to read it would be generous "
                        + "enough to read `30` as something, and a lease TTL is not a "
                        + "setting to be generous about")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.LEASE_TTL)
                .hasMessageContaining("30s")
                .hasMessageContaining("PT30S");
    }

    @Test
    void aNONPOSITIVEDurationIsREFUSED() {
        assertThatThrownBy(() -> ServerProperties.parse(with(ServerProperties.LEASE_TTL, "PT0S")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() ->
                ServerProperties.parse(with(ServerProperties.LEASE_RENEW, "PT-5S")))
                .as("⚠️ A NEGATIVE RENEW INTERVAL IS ACCEPTED BY `Duration.parse` and means "
                        + "a renewer that never sleeps")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void aNEGATIVEOrZEROSizeIsREFUSED() {
        assertThatThrownBy(() ->
                ServerProperties.parse(with(ServerProperties.MAX_SEGMENT_BYTES, "-1")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.MAX_SEGMENT_BYTES)
                .hasMessageContaining("positive");
        assertThatThrownBy(() ->
                ServerProperties.parse(with(ServerProperties.MAX_SEGMENT_BYTES, "0")))
                .as("⚠️ THE BOUNDARY, AND IT MUST NAME THE KEY. Review MEASURED that "
                        + "`<= 0` weakened to `< 0` left this case green: zero reaches "
                        + "`IngestConfig`, which refuses it with a message containing "
                        + "\"positive\" and naming a RECORD FIELD rather than the setting "
                        + "the operator wrote. Asserting only the word is what let the "
                        + "boundary move with nothing red")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.MAX_SEGMENT_BYTES)
                .hasMessageContaining("positive");
    }

    @Test
    void aSizeThatIsNOTANumberIsREFUSEDNamingTheKeyAndTheValue() {
        assertThatThrownBy(() ->
                ServerProperties.parse(with(ServerProperties.MAX_SEGMENT_BYTES, "8MiB")))
                .as("⚠️ NO UNIT SUFFIXES, and refusing says so where a silent 0 would not")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.MAX_SEGMENT_BYTES)
                .hasMessageContaining("8MiB");
    }

    @Test
    void anUNKNOWNSettingIsREFUSEDAndTheMessageLISTSWhatIsKnown() {
        assertThatThrownBy(() -> ServerProperties.parse(with("lease.tll", "PT30S")))
                .as("⚠️ THE TYPO CASE, AND THE ONE MOST LIKELY TO BE ARGUED ABOUT. A "
                        + "tolerated `lease.tll` leaves the TTL at its default, the node "
                        + "runs, and the operator's evidence that they changed it is their "
                        + "own config file. Refusing costs a restart; tolerating costs an "
                        + "incident nobody can reproduce")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("lease.tll")
                .hasMessageContaining(ServerProperties.LEASE_TTL);
    }

    @Test
    void aMISSPELLEDBooleanIsREFUSEDRatherThanReadAsFALSE() {
        assertThatThrownBy(() ->
                ServerProperties.parse(with(ServerProperties.DIRECT_ENABLED, "yes")))
                .as("⚠️ `Boolean.parseBoolean` READS EVERYTHING BUT `true` AS FALSE, and "
                        + "this flag's true value is what makes a node REFUSE to start "
                        + "against a backend that cannot sign — so a typo silently disables "
                        + "the check it was set to turn on")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.DIRECT_ENABLED)
                .hasMessageContaining("yes");
    }

    @Test
    void aVALUEIsTRIMMEDBeforeItIsUSED() {
        // ⚠️ A TRAILING SPACE IN A MANIFEST IS INVISIBLE AND SURVIVES COPY-
        // PASTE. Untrimmed, `pod.id: "pod1 "` is a SECOND identity for one
        // node: it takes a lease under a name no peer matches, and a takeover
        // is attributed to a pod nobody can find.
        ServerConfig config = ServerProperties.parse(with(ServerProperties.POD_ID, " pod1 "));
        assertThat(config.podId()).isEqualTo("pod1");
        assertThat(ServerProperties.parse(with(ServerProperties.STORE_ROOT, " /var/x "))
                .store().root()).contains("/var/x");
    }

    @Test
    void aBOOLEANIsCASEInsensitive() {
        assertThat(ServerProperties.parse(with(ServerProperties.DIRECT_ENABLED, " TRUE "))
                .ingest().directEnabled()).isTrue();
        assertThat(ServerProperties.parse(with(ServerProperties.DIRECT_ENABLED, "False"))
                .ingest().directEnabled()).isFalse();
    }

    @Test
    void aSTOREROOTIsCARRIEDThroughToTheStoreConfig() {
        ServerConfig config = ServerProperties.parse(new HashMap<>(Map.of(
                ServerProperties.POD_ID, "pod1",
                ServerProperties.POD_AZ, "az-a",
                ServerProperties.TRUST_DOMAIN, "cluster-a",
                ServerProperties.PREFIX, "bins/cluster-a",
                ServerProperties.STORE_KIND, "local-fs",
                ServerProperties.STORE_ROOT, "/var/lib/io.github.huyz0.os.biningester",
                ServerProperties.ENDPOINT, "http://pod1:8080",
                ServerProperties.HTTP_PORT, "8080",
                ServerProperties.PRODUCER_SUBJECT, "producer-1",
                ServerProperties.PRODUCER_ALLOWED_INDICES, "logs")));

        assertThat(config.store().root()).contains("/var/lib/io.github.huyz0.os.biningester");
    }

    @Test
    void aROOTGivenToAKindThatHasNoFilesystemIsREFUSEDWhenTheStoreIsOPENED() {
        // ⚠️ REFUSED AT `StoreFactory`, not here: the parser's job is the text,
        // and whether a kind can honour a root is the factory's rule. What this
        // pins is that the root SURVIVES parsing, so that the factory gets to
        // see it -- a parser that dropped it would make the factory's refusal
        // unreachable and an ignored setting silent again.
        ServerConfig config = ServerProperties.parse(with(ServerProperties.STORE_ROOT, "/tmp/x"));
        assertThat(config.store().root()).contains("/tmp/x");
        assertThatThrownBy(() -> StoreFactory.open(config.store()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("root");
    }

    @Test
    void aCROSSFIELDRuleTheParserDoesNotKnowAboutArrivesAsAConfigurationException() {
        // ⚠️ THE CASE THAT REACHES THE WRAPPER, and review MEASURED that the
        // previous one did not: a blank `store.prefix` is refused by
        // `required()` BEFORE any record is constructed, so the whole try/catch
        // could be deleted with the suite green.
        //
        // ⚠️ `PT10S` IS WELL-FORMED, POSITIVE, AND PASSES EVERY CHECK IN THE
        // PARSER. It is refused by `IngestConfig`, whose interval CEILING is
        // 5 s -- a rule the parser deliberately does not restate, because a
        // second copy goes stale. What this pins is that the refusal reaches
        // the operator as a ConfigurationException rather than as a stack
        // trace from inside the write path.
        assertThatThrownBy(() ->
                ServerProperties.parse(with(ServerProperties.INTERVAL_FLOOR, "PT10S")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("intervalCeiling");
    }

    @Test
    void aBLANKOptionalSettingIsREFUSEDRatherThanTakenLiterally() {
        // ⚠️ `store.root` WAS THE ONE READ THAT SKIPPED THE BLANK CHECK, found
        // by review and MEASURED: `store.root=${DATA_DIR}` with DATA_DIR unset
        // parsed to a root of three spaces, and `StoreFactory` then RETURNED A
        // STORE rooted at a directory named "   " under the process's working
        // directory. A node that starts, acks, and writes where nothing reads.
        assertThatThrownBy(() -> ServerProperties.parse(with(ServerProperties.STORE_ROOT, "   ")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.STORE_ROOT)
                .hasMessageContaining("blank");
    }

    @Test
    void theTRUSTDomainReachesBOTHRecordsFromONESetting() {
        // ⚠️ WHAT THIS ACTUALLY PINS, and it is not what an earlier version of
        // this case claimed. `ServerConfig` refuses a trust domain that
        // disagrees with the ingest config's -- and that guard is UNREACHABLE
        // through `parse()`, because one variable feeds both. Review measured
        // the earlier version testing neither of the two things its comment
        // named. The property that IS this parser's is that one setting reaches
        // both places, so the guard has nothing to catch.
        ServerConfig parsed = ServerProperties.parse(minimal());
        assertThat(parsed.trustDomain()).isEqualTo(parsed.ingest().trustDomain());
    }

    @Test
    void aSettingWithNONameIsREFUSEDRatherThanTHROWING() {
        Map<String, String> settings = minimal();
        settings.put(null, "orphan");
        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .as("⚠️ A YAML `: value` LINE PARSES TO A NULL KEY and "
                        + "`Set.of(...).contains(null)` THROWS, so without a guard the "
                        + "operator gets a bare NullPointerException from inside the parser "
                        + "-- the one message shape this class exists to avoid handing them")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("empty key");
    }

    @Test
    void theKNOWNKeysCOMEBackSORTED() {
        assertThat(new java.util.ArrayList<>(ServerProperties.knownKeys()))
                .as("⚠️ SORTED, because the list is printed in a refusal message and an "
                        + "operator scanning it for their near-miss needs an order")
                .isSorted();
    }

    @Test
    void aNULLSettingsMapIsREFUSED() {
        assertThatThrownBy(() -> ServerProperties.parse(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void theKNOWNKeysAreREPORTABLEForAUsageMessage() {
        assertThat(ServerProperties.knownKeys())
                .contains(ServerProperties.POD_ID, ServerProperties.ENDPOINT,
                        ServerProperties.STORE_KIND, ServerProperties.INTERVAL_CEILING)
                .hasSize(30);
    }
}
