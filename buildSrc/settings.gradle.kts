// SPDX-License-Identifier: Apache-2.0
rootProject.name = "build-conventions"

dependencyResolutionManagement {
    repositories { mavenCentral() }
    versionCatalogs {
        create("libs") { from(files("../gradle/libs.versions.toml")) }
    }
}
