// SPDX-License-Identifier: Apache-2.0

plugins { id("binjava.java-conventions") }

dependencies {
    api(project(":binstore-spi"))

    // ⚠️ THE SDK IS `implementation`, NEVER `api`. `binstore-backends` is the
    // adapter (AGENTS.md non-negotiable 7) and the whole point of the SPI is
    // that nothing above it names a vendor type; an `api` here would put
    // `software.amazon.awssdk` on the compile classpath of every module that
    // depends on this one, and the first `S3Exception` caught in `ingest` would
    // compile.
    implementation(libs.awssdk.s3) {
        // ⚠️ THE SDK'S OWN HTTP CLIENTS EXCLUDED, from the one list that also
        // drives the root project's licence gate (`binjava.AwsSdkHttp`).
        // Without them the SDK brings Netty AND Apache HttpClient 5: two HTTP
        // stacks and a reactive runtime, to make calls this project makes
        // blocking on a virtual thread anyway.
        binjava.AwsSdkHttp.EXCLUDED_CLIENTS.forEach {
            exclude(group = binjava.AwsSdkHttp.GROUP, module = it)
        }
    }
    implementation(libs.awssdk.url.connection.client)
    // The conformance suite lives in the SPI's fixtures so every backend runs
    // the same contract; a suite that only ever ran against the in-memory fake
    // would prove nothing about S3.
    testImplementation(testFixtures(project(":binstore-spi")))

    // ⚠️ T0, AND ONLY FOR A STUB OF THE SDK's OWN CLIENT. Two properties of
    // this backend -- that an empty batch costs NO request, and that a per-key
    // failure inside a 200 is not success -- are invisible against a real
    // endpoint, which answers both happily.
    testImplementation(libs.awssdk.s3) {
        binjava.AwsSdkHttp.EXCLUDED_CLIENTS.forEach {
            exclude(group = binjava.AwsSdkHttp.GROUP, module = it)
        }
    }

    // ⚠️ T3. The MinIO cases need the SDK types the backend hides from every
    // other module, because what they assert is the wire behaviour this adapter
    // maps -- a 412 for a lost conditional write against a 404 for one with
    // nothing to match.
    "integrationTestImplementation"(testFixtures(project(":binstore-spi")))
    "integrationTestImplementation"(libs.awssdk.s3) {
        binjava.AwsSdkHttp.EXCLUDED_CLIENTS.forEach {
            exclude(group = binjava.AwsSdkHttp.GROUP, module = it)
        }
    }
    "integrationTestImplementation"(libs.awssdk.url.connection.client)
}
