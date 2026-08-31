// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":ingest"))
    // ⚠️ Helidon lives HERE and nowhere below. check-module.sh asserts no module
    // below `http` resolves an HTTP dependency (SPEC T6c) -- that classpath
    // constraint, not a test, is what keeps this an adapter rather than a
    // second implementation of the ingest path.
    implementation(libs.helidon.webserver)

    testImplementation(libs.helidon.webclient)
}
