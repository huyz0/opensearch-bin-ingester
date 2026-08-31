// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":client"))

    // ⚠️ compileOnly: OpenSearch supplies these at runtime to a plugin installed
    // in its own node. Bundling them would ship a second copy of the engine and
    // break on any version skew (build.md § version skew).
    compileOnly(libs.opensearch)
    testImplementation(libs.opensearch)
}
