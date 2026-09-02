// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":binstore-spi"))
    api(project(":format"))

    // ⚠️ TEST ONLY, and the same reasoning `ingest` records for the identical
    // line: the production surface stays binstore-spi + format
    // (architecture.md), and a backend here would let the coordination layer
    // depend on a concrete store. The tests need a real one to assert request
    // COUNTS against -- a hand-rolled fake could not do that honestly, and
    // request counts are what this project exists to control.
    testImplementation(project(":binstore-backends"))
}
