// SPDX-License-Identifier: Apache-2.0

plugins { id("io.github.huyz0.os.biningester.java-conventions") }

dependencies {
    // ⚠️ THE COMPOSITION ROOT DEPENDS ON EVERYTHING AND NOTHING DEPENDS ON IT.
    // That is what makes it a leaf in the dependency surface rather than a
    // cycle, and it is why `main()` lives here and not in `http`: `http` is the
    // thin adapter (architecture.md rule 4, ADR-0019) and the in-process API is
    // the primary one, so a root there would make the HTTP surface the assembly
    // point and hand every in-process embedder a web server.
    // ⚠️ `implementation`, NOT `api`: nothing depends on this module, so there
    // is no consumer for an exported API -- and `api` on a leaf is a claim that
    // someone downstream compiles against these types, which would be false.
    implementation(project(":ingest"))
    implementation(project(":sequencer"))

    // ⚠️ THE ONLY MODULE THAT MAY NAME A BACKEND IN `src/main` (M8's criterion
    // 2). Every other module takes a `BinStore`; this one chooses which. M8.29
    // extends `check-module.sh` to refuse this dependency from anywhere else --
    // until it lands, the rule is a convention and this comment, which is rung
    // 7 and says so.
    implementation(project(":binstore-backends"))

    // ⚠️ THE FRONT DOOR (M8.4). `http` is the thin adapter and this is the only
    // module that starts one: the producer's `_bulk`, the peer's forwarded
    // commit and the consumer's long poll are three services on one listener,
    // because the `endpoint` a lease publishes is one address. `http` api-
    // exposes `client`, which is how the peer transport arrives here too.
    implementation(project(":http"))

    // ⚠️ DECLARED HERE TOO, because `http` takes Helidon as `implementation`
    // and not `api` -- deliberately, so that depending on the adapter does not
    // hand every consumer a web server. The composition root is the one place
    // that genuinely builds one, so it is the one place that names the
    // dependency. `check-module.sh`'s NO_HTTP set is `format binstore-spi
    // sequencer ingest`, and this module is above all four.
    implementation(libs.helidon.webserver)

    // ⚠️ TEST ONLY, and it is a PRODUCER's client rather than a consumer's:
    // `NodeStartTest` posts a `_bulk` over a real socket, because "the listener
    // is up and the three services are on it" is not a property an in-process
    // call can check. Nothing here reaches the production classpath.
    testImplementation(libs.helidon.webclient)

    // ⚠️ T3, AND AGAINST A REAL ENDPOINT. Criterion 1 is "the object is in the
    // BUCKET and its key matches the grammar" -- a process that acked from its
    // accumulator and served the read back out of the same JVM is green on the
    // 202 and on the read-back, and only a second, independently-opened client
    // looking at RustFS can tell the two apart.
    "integrationTestImplementation"(testFixtures(project(":binstore-backends")))
    "integrationTestImplementation"(project(":binstore-backends"))
    "integrationTestImplementation"(project(":http"))
    "integrationTestImplementation"(project(":client"))
    // M9.9 verifies the real consumer-node registration seam alongside the
    // RustFS serving-path request count; this is test-only and does not make
    // the server depend on the plugin at runtime.
    "integrationTestImplementation"(project(":plugin"))
    // ⚠️ TEST-ONLY: the macro workload is the independent benchmark leaf;
    // no shipped runtime classpath depends on it (ADR-0059).
    "integrationTestImplementation"(project(":bench"))
    "integrationTestImplementation"(libs.hdrhistogram)
    "integrationTestImplementation"(libs.helidon.webclient)
    // ⚠️ A SERVER IN THE TEST, and only to play the Kubernetes API for the
    // EndpointSlice watch (M8.13): `FakeKubeApi` streams the watch events a
    // cluster's endpoints controller would. The node's own server is the one
    // on the production classpath already.
    "integrationTestImplementation"(libs.helidon.webserver)
    // ⚠️ THE SDK IN THE TEST ONLY, and only to MAKE A BUCKET and to build the
    // second client that looks at it. The node under test never sees these
    // types: `binstore-backends` takes the SDK as `implementation`, so the
    // production path here names a store kind and a bucket, and nothing more.
    "integrationTestImplementation"(libs.awssdk.s3) {
        io.github.huyz0.os.biningester.AwsSdkHttp.EXCLUDED_CLIENTS.forEach {
            exclude(group = io.github.huyz0.os.biningester.AwsSdkHttp.GROUP, module = it)
        }
    }
    "integrationTestImplementation"(libs.awssdk.url.connection.client)

    "soakTestImplementation"(project(":binstore-backends"))
    "soakTestImplementation"(project(":http"))
    "soakTestImplementation"(project(":client"))
    "soakTestImplementation"(project(":sequencer"))
    "soakTestImplementation"(project(":ingest"))
}
