// SPDX-License-Identifier: Apache-2.0

plugins { id("io.github.huyz0.os.biningester.java-conventions") }

dependencies {
    api(project(":format"))

    // ⚠️ THE CONSUMER'S TRANSPORT IS HTTP AND IT LIVES HERE, next to the seam
    // it implements. `client` is deliberately NOT in `check-module.sh`'s
    // NO_HTTP set -- `format`, `binstore-spi`, `sequencer` and `ingest` are,
    // because ADR-0019's rule is that the INGEST path has one front door, and
    // the consumer subscribing to one is the other end of it rather than a
    // second implementation of anything.
    implementation(libs.helidon.webclient)
}
