// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":client"))

    // ⚠️ compileOnly: OpenSearch supplies these at runtime to a plugin installed
    // in its own node. Bundling them would ship a second copy of the engine and
    // break on any version skew (build.md § version skew).
    compileOnly(libs.opensearch)
    testImplementation(libs.opensearch)

    // ⚠️ TEST ONLY, and only for the end-to-end test: it drives the real
    // ingester and a real filesystem store through the whole path. The
    // production surface stays `client`, so the plugin cannot reach the
    // ingester at runtime -- check-module holds that line.
    testImplementation(project(":ingest"))
    testImplementation(project(":binstore-backends"))
}
