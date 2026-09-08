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

    // ⚠️ T4 only. The OpenSearch test framework boots a real node, which is the
    // ONLY way to answer criterion 2 -- "searchable" is a property of Lucene and
    // the ingestion engine, not of our seams. It is deliberately not on the T0
    // classpath: build.md keeps `./gradlew test` free of heavy tiers.
    "clusterTestImplementation"(libs.opensearch)
    "clusterTestImplementation"(libs.opensearch.testframework)
    "clusterTestImplementation"(libs.opensearch.agent.bootstrap)
    // ⚠️ OpenSearchSingleNodeTestCase extends LuceneTestCase, which is JUnit 4
    // + RandomizedRunner. The vintage engine lets Jupiter discover it; JUnit 4
    // itself comes from the framework, and declaring it AGAIN puts two copies on
    // the classpath, which OpenSearch's own checker rejects as "jar hell" from a
    // static initialiser before any test runs.
    "clusterTestRuntimeOnly"("org.junit.vintage:junit-vintage-engine:5.11.4") {
        exclude(group = "junit", module = "junit")
    }
    "clusterTestImplementation"(project(":ingest"))
    "clusterTestImplementation"(project(":binstore-backends"))
    // ⚠️ TEST ONLY, for `TestSequencers`: since M4.6d a `DefaultIngest` needs a
    // Sequencer, and these ITs drive the real ingester over a real store.
    "clusterTestImplementation"(testFixtures(project(":sequencer")))
}

// ⚠️ OpenSearch 3.x replaced the SecurityManager with a JAVA AGENT, and its test
// framework refuses to class-load without it attached: "the security agent is
// not attached", thrown from a static initialiser before any test runs. The
// agent jar must reach the JVM as -javaagent, so it is resolved into its own
// configuration rather than put on the classpath.
//
// ⚠️ isTransitive = false: the agent drags its dependencies, and a -javaagent
// argument names exactly ONE jar. With transitives the configuration resolves to
// several files and the build fails at execution time with "expected exactly one
// file", which is a confusing way to learn this.
val opensearchAgent: Configuration by configurations.creating {
    isTransitive = false
}

// ⚠️ The agent's own classes must be on the BOOT classpath, not the application
// one: the agent runs before the app classloader exists, so with the bootstrap
// jar only on -cp it dies with NoClassDefFoundError: AgentPolicy -- from inside
// the agent, before any test is discovered.
val opensearchAgentBootstrap: Configuration by configurations.creating {
    isTransitive = false
}

dependencies {
    opensearchAgent(libs.opensearch.agent)
    opensearchAgentBootstrap(libs.opensearch.agent.bootstrap)
}

// ⚠️ The PATH is captured at configuration time, not the Configuration object.
// A Configuration cannot be serialised into the configuration cache, and holding
// one in a task action fails the build with a cache problem rather than a
// missing-agent error.
val agentPath = opensearchAgent.elements.map { it.single().asFile.absolutePath }
val agentBootstrapPath =
    opensearchAgentBootstrap.elements.map { it.single().asFile.absolutePath }

tasks.named<Test>("clusterTest") {
    // ⚠️ jvmArgs, NOT a CommandLineArgumentProvider. A Kotlin lambda in a build
    // script captures the script object, which the configuration cache refuses
    // to serialise -- "cannot serialize Gradle script object references". The
    // paths are plain Strings resolved during configuration, so nothing script-
    // shaped reaches the task.
    jvmArgs(
        "-javaagent:" + agentPath.get(),
        "-Xbootclasspath/a:" + agentBootstrapPath.get(),
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
    )
    // ⚠️ A node needs more than the 512m the T0 suite caps at (build.md).
    maxHeapSize = "2g"
    // ⚠️ JaCoCo attaches its own agent and OpenSearch's agent policy rejects the
    // result; coverage of a T4 tier is not what the floors measure anyway.
    extensions.configure<JacocoTaskExtension> { isEnabled = false }
}
