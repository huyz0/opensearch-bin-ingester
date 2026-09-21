// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isRegularFile
import kotlin.io.path.readText

/** JVM implementations of the repository predicates formerly split across scripts. */
object RepositoryGateChecks {
    private val sourceExtensions = setOf("java", "kt", "kts", "gradle", "sh", "py")

    fun gateScope(root: Path, failures: MutableList<String>) {
        val ignore = root.resolve(".gitignore").takeIf { Files.isRegularFile(it) }?.readText().orEmpty()
        if (!ignore.lineSequence().any { it.trim() == ".tmp/" || it.trim() == ".tmp" })
            failures += ".gitignore does not quarantine .tmp/"
        if (root.parent == null) failures += "repository has no bounded parent root"
    }

    fun javadocCitations(root: Path, files: List<Path>, failures: MutableList<String>) {
        // The current tree has no executable test citations. Keep the check
        // deliberately narrow: if a main-source Javadoc names FooTest/FooIT,
        // the corresponding test source must exist in this repository.
        val names = files.filter { it.extension() == "java" && normalized(it).contains("/src/") && !normalized(it).contains("/src/main/") }
            .map { it.fileName.toString().removeSuffix(".java") }.toSet()
        val cite = Regex("\\b([A-Z][A-Za-z0-9_]*Test|[A-Z][A-Za-z0-9_]*[a-z]IT)\\b")
        files.filter { it.extension() == "java" && normalized(it).contains("/src/main/") }.forEach { file ->
            cite.findAll(file.readText()).map { it.groupValues[1] }.distinct().forEach { name ->
                if (name !in names) failures += "${root.relativize(file)} cites missing test $name"
            }
        }
    }

    fun portability(root: Path, failures: MutableList<String>) {
        val skills = root.resolve(".agents/skills")
        if (Files.isDirectory(skills)) Files.list(skills).use { dirs ->
            dirs.filter { Files.isDirectory(it) }.forEach { dir ->
                val name = dir.fileName.toString()
                val skill = dir.resolve("SKILL.md")
                if (!Files.isRegularFile(skill)) {
                    failures += "$name: missing SKILL.md"
                    return@forEach
                }
                val lines = Files.readAllLines(skill)
                if (lines.firstOrNull() != "---") failures += "$name: SKILL.md has no YAML front matter"
                val frontName = lines.firstOrNull { it.startsWith("name: ") }?.removePrefix("name: ")
                if (frontName != name) failures += "$name: frontmatter name is '$frontName'"
                val desc = lines.firstOrNull { it.startsWith("description: ") }
                if (desc == null || !Regex("Use (when|before|after|at|whenever|during)").containsMatchIn(desc))
                    failures += "$name: description has no trigger clause"
                if (lines.any { it.trimStart().startsWith("@") }) failures += "$name: SKILL.md contains vendor syntax"
                val adapter = root.resolve(".claude/commands/$name.md")
                if (!Files.isRegularFile(adapter)) failures += "$name: missing .claude adapter"
            }
        }
        val commands = root.resolve(".claude/commands")
        if (Files.isDirectory(commands)) Files.list(commands).use { stream ->
            stream.filter { it.extension() == "md" }.forEach { command ->
                val name = command.fileName.toString().removeSuffix(".md")
                val text = command.readText()
                if (!Files.isDirectory(skills.resolve(name))) failures += "$command targets no skill"
                if (!text.contains(".agents/skills/$name/SKILL.md")) failures += "$command does not point to its skill"
                if (Files.readAllLines(command).size > 20) failures += "$command is longer than 20 lines"
            }
        }
    }

    fun metricCardinality(root: Path, files: List<Path>, failures: MutableList<String>) {
        val forbidden = "index|indexName|partition|stream|streamId|tenant|tenantId|routing|objectKey|segmentKey|offset|docId|_id"
        val call = Regex("(?:tag|label|attribute|withTag|addTag|put)\\s*\\(\\s*\\\"($forbidden)\\\"", RegexOption.IGNORE_CASE)
        files.filter { it.extension() == "java" }.forEach { file ->
            file.readText().lineSequence().forEachIndexed { line, text ->
                if (call.containsMatchIn(text)) failures += "high-cardinality label at ${root.relativize(file)}:${line + 1}"
            }
        }
    }

    fun ioSeam(root: Path, files: List<Path>, failures: MutableList<String>) {
        val packages = listOf("java.nio.file", "java.nio.channels", "java.io", "java.util.zip", "java.util.jar", "java.util.prefs", "java.util.logging", "java.sql", "javax.sql", "javax.naming", "java.net", "javax.net")
        val constructs = listOf(".now(", "currentTimeMillis(", "nanoTime(", "Clock.system", "Clock.tick", "new Date(", "new GregorianCalendar(", "Calendar.getInstance(", "new ProcessBuilder(", ".exec(", ".getResourceAsStream(", "getSystemResourceAsStream(", ".toURL(", ".openStream(", ".openConnection(")
        val allowed = listOf("java.net.URI", "java.net.URLEncoder", "java.net.URLDecoder", "java.io.IOException", "java.io.UncheckedIOException", "java.io.FileNotFoundException", "java.io.InputStream", "java.io.OutputStream", "java.io.ByteArrayInputStream", "java.io.ByteArrayOutputStream", "java.io.FilterInputStream", "java.io.Closeable", "java.io.Flushable", "java.io.DataInputStream", "java.io.DataOutputStream", "java.io.Serializable", "java.util.zip.CRC32", "java.util.zip.CRC32C", "java.util.zip.Adler32")
        files.filter { it.extension() == "java" && normalized(it).contains("/src/main/java/") }
            .filterNot { normalized(it).contains("/binstore-backends/") }
            .filterNot { root.relativize(it).toString().replace('\\', '/') in setOf("server/src/main/java/io/github/huyz0/os/biningester/server/Main.java", "server/src/main/java/io/github/huyz0/os/biningester/server/ConfigFile.java") }
            .forEach { file ->
                val text = stripJavaNoise(file.readText())
                packages.forEach { pkg ->
                    val references = Regex("\\b${Regex.escape(pkg)}\\.[A-Za-z_$][A-Za-z0-9_$]*")
                        .findAll(text).map { it.value }.filterNot { it in allowed }.toList()
                    if (references.isNotEmpty()) failures += "I/O package $pkg in ${root.relativize(file)}"
                }
                constructs.filter { text.contains(it) }.forEach { failures += "I/O construct $it in ${root.relativize(file)}" }
            }
    }

    fun faultStore(root: Path, files: List<Path>, failures: MutableList<String>) {
        val file = files.firstOrNull { it.fileName.toString() == "FaultInjectingStore.java" }
        if (file == null) {
            failures += "fault-injecting store is missing"
            return
        }
        val lines = Files.readAllLines(file)
        var examined = 0
        lines.forEachIndexed { index, line ->
            if (!line.contains("@Override")) return@forEachIndexed
            examined++
            var opened = false
            var depth = 0
            var statement: String? = null
            var braceLine = -1
            for (i in index until lines.size) {
                lines[i].forEach { ch -> if (ch == '{') { opened = true; depth++ } else if (ch == '}') depth-- }
                if (opened && braceLine < 0) braceLine = i
                if (braceLine >= 0 && i > braceLine) {
                    val candidate = lines[i].trim()
                    if (candidate.isNotEmpty() && !candidate.startsWith("//") && !candidate.startsWith("/*") && !candidate.startsWith("*")) { statement = candidate; break }
                }
                if (opened && depth == 0) break
            }
            if (statement == null || !statement.startsWith("record(")) failures += "unmetered @Override at ${root.relativize(file)}:${index + 1}"
        }
        if (examined == 0) failures += "fault-injecting store has no @Override verbs"
    }

    fun testBudget(root: Path, failures: MutableList<String>) {
        val props = root.resolve("gradle.properties")
        if (!Files.isRegularFile(props)) return
        val text = props.readText()
        if (!Regex("(?m)^org\\.gradle\\.jvmargs=.*-Xmx\\S+").containsMatchIn(text)) failures += "gradle.properties has no Gradle heap limit"
        if (!Regex("(?m)^org\\.gradle\\.workers\\.max=\\d+").containsMatchIn(text)) failures += "gradle.properties has no worker limit"
        val conventions = root.resolve("buildSrc/src/main/kotlin/io.github.huyz0.os.biningester.java-conventions.gradle.kts")
        val build = if (Files.isRegularFile(conventions)) conventions.readText() else ""
        if (!build.contains("memoryMaximumSize")) failures += "conventions plugin has no compile-daemon heap limit"
        if (!build.contains("maxHeapSize")) failures += "conventions plugin has no test heap limit"
        if (!build.contains("timeout.set")) failures += "conventions plugin has no test timeout"
    }

    fun moduleDrift(root: Path, failures: MutableList<String>) {
        val settings = root.resolve("settings.gradle.kts").readText()
        val includeStart = settings.indexOf("include(")
        val includeEnd = settings.indexOf("\n)", includeStart.coerceAtLeast(0)).let { if (it >= 0) it else settings.indexOf(")", includeStart.coerceAtLeast(0)) }
        val includeBody = if (includeStart >= 0 && includeEnd > includeStart) settings.substring(includeStart, includeEnd) else ""
        val modules = Regex("\"([^\"]+)\"").findAll(includeBody).map { it.groupValues[1] }.toSet()
        if (modules.isEmpty()) failures += "settings.gradle.kts declares no modules"
        modules.filter { !Files.isRegularFile(root.resolve("$it/build.gradle.kts")) }
            .forEach { failures += "settings lists '$it' without build.gradle.kts" }
        Files.list(root).use { stream ->
            stream.filter { Files.isDirectory(it) && Files.isRegularFile(it.resolve("build.gradle.kts")) }
                .map { it.fileName.toString() }.filter { it != "buildSrc" && it !in modules }
                .forEach { failures += "$it has build.gradle.kts but is not in settings.gradle.kts" }
        }
    }

    /** Static dependency-surface rules formerly enforced by check-module.sh. */
    fun moduleDependencies(root: Path, failures: MutableList<String>) {
        val settings = root.resolve("settings.gradle.kts").readText()
        val modules = Regex("include\\(.*?\\)", setOf(RegexOption.DOT_MATCHES_ALL))
            .find(settings)?.value?.let { Regex("\\\":([^\\\"]+)\\\"").findAll(it).map { m -> m.groupValues[1] }.toSet() }.orEmpty()
        val projectDep = Regex("project\\(\\\":([^\\\"]+)\\\"\\)")
        val noHttp = setOf("format", "binstore-spi", "sequencer", "ingest")
        val noCloud = setOf("client", "plugin")
        val httpCoordinates = Regex("(?:io\\.helidon|org\\.eclipse\\.jetty|io\\.netty|jakarta\\.servlet|org\\.apache\\.httpcomponents|com\\.squareup\\.okhttp3|org\\.glassfish\\.jersey|io\\.undertow|io\\.vertx|org\\.apache\\.tomcat|com\\.linecorp\\.armeria|org\\.jboss\\.resteasy|io\\.projectreactor\\.netty|io\\.grpc|org\\.springframework)")
        val cloudCoordinates = Regex("(?:software\\.amazon\\.awssdk|com\\.amazonaws|com\\.google\\.cloud|com\\.azure)")
        modules.forEach { module ->
            val build = root.resolve(module).resolve("build.gradle.kts")
            if (!Files.isRegularFile(build)) return@forEach
            val text = build.readText()
            val deps = projectDep.findAll(text).map { it.groupValues[1] }.toSet()
            if (module != "http" && module != "server" && "http" in deps) failures += "$module depends on http"
            if (module != "server" && "server" in deps) failures += "$module depends on the server composition root"
            if (module != "server" && "binstore-backends" in deps) failures += "$module depends on binstore-backends"
            if (module !in setOf("server", "bench") && deps.any { it in setOf("server", "bench") }) failures += "$module depends on a leaf module"
            if (module in noHttp && httpCoordinates.containsMatchIn(text)) failures += "$module declares a forbidden HTTP dependency"
            if (module in noCloud && cloudCoordinates.containsMatchIn(text)) failures += "$module declares a forbidden cloud dependency"
        }
    }

    private fun stripJavaNoise(text: String): String = text
        .replace(Regex("(?s)/\\*.*?\\*/"), "")
        .replace(Regex("//.*"), "")
        .replace(Regex("(?s)\"(?:\\\\.|[^\"])*\""), "\"\"")

    private fun Path.extension(): String = fileName.toString().substringAfterLast('.', "")
    private fun normalized(path: Path): String = path.toString().replace('\\', '/')
}
