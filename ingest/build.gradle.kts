// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":binstore-spi"))
    api(project(":format"))
    api(project(":sequencer"))
    testImplementation(testFixtures(project(":sequencer")))

    // ⚠️ TEST ONLY. The production surface stays binstore-spi + format +
    // sequencer (architecture.md); a backend here would let ingest depend on a
    // concrete store. The tests need a real one to assert request COUNTS
    // against, which a hand-rolled fake could not do honestly.
    testImplementation(project(":binstore-backends"))
}
