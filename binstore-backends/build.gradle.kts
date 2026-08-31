// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":binstore-spi"))
    // The conformance suite lives in the SPI's fixtures so every backend runs
    // the same contract; a suite that only ever ran against the in-memory fake
    // would prove nothing about S3.
    testImplementation(testFixtures(project(":binstore-spi")))
}
