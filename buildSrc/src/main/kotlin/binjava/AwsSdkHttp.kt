// SPDX-License-Identifier: Apache-2.0
package binjava

/**
 * The AWS SDK's own HTTP clients, which this build refuses.
 *
 * ⚠️ ONE COPY, BECAUSE TWO PLACES MUST AGREE. The module's `implementation`
 * declaration is what keeps these off a runtime classpath; the root project's
 * `licenseCheck` configuration is what decides which jars need a pinned sha and
 * a committed licence, and it is built from the version catalogue rather than
 * from any module, so a module-level exclude is invisible to it. Written twice,
 * they drift, and the drift is silent in the direction that matters: the gate
 * would stop demanding a licence for a jar that is still shipped.
 *
 * ⚠️ `apache5-client`, NOT `apache-client`. The SDK renamed it at 2.3x, and the
 * first draft of this exclusion used the old name -- which excludes nothing and
 * fails no build. It was caught by `updateShas` pinning 45 jars including
 * `httpclient5`, not by anything that refuses.
 *
 * What is used instead is `url-connection-client`: the JDK's own
 * `HttpURLConnection`, so the SDK arrives with no HTTP stack and no reactive
 * runtime. Every call this project makes through it is blocking on a virtual
 * thread (research 40-01 §1), which is what makes the smallest client the right
 * one rather than merely the lightest.
 */
object AwsSdkHttp {
    const val GROUP = "software.amazon.awssdk"

    val EXCLUDED_CLIENTS = listOf("netty-nio-client", "apache5-client", "apache-client")
}
