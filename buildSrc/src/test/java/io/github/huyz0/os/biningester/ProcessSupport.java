// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Starts the repository's script and interpreter commands on every supported OS. */
final class ProcessSupport {

  private ProcessSupport() {}

  static ProcessBuilder builder(String... command) {
    return builder(List.of(command));
  }

  static ProcessBuilder builder(List<String> command) {
    if (!isWindows() || command.isEmpty()) {
      return process(command);
    }

    String first = command.getFirst();
    if (first.equals("bash") || first.endsWith(".sh")) {
      List<String> shell = first.equals("bash") ? command.subList(1, command.size()) : command;
      List<String> adapted = new ArrayList<>(List.of(gitBash(), "--noprofile", "--norc", "-c"));
      String pythonPath = pythonUnixPath();
      String prefix = "export PATH=\"$PATH:" + gitCommandUnixPath()
          + ":/mingw64/bin:/usr/bin:/bin\"; "
          + "python3(){ \"$PYTHON3\" \"$@\"; }; export -f python3; ";
      if (!shell.isEmpty() && shell.getFirst().equals("-c")) {
        adapted.add(prefix + shell.get(1));
        adapted.addAll(shell.subList(Math.min(2, shell.size()), shell.size()));
      } else {
        StringBuilder commandText = new StringBuilder(prefix).append("exec bash ");
        for (String argument : shell) {
          commandText.append(bashQuote(argument)).append(' ');
        }
        adapted.add(commandText.toString().trim());
      }
      return process(adapted);
    }
    if (first.equals("python") || first.equals("python3")) {
      List<String> adapted = new ArrayList<>(command);
      adapted.set(0, python());
      return process(adapted);
    }
    return process(command);
  }

  private static ProcessBuilder process(List<String> command) {
    ProcessBuilder builder = new ProcessBuilder(command);
    if (isWindows()) {
      builder.environment().put("PYTHONUTF8", "1");
      builder.environment().put("PYTHON3", pythonUnixPath());
      String programFiles = System.getenv("ProgramFiles");
      if (programFiles == null) programFiles = System.getenv("PROGRAMFILES");
      if (programFiles != null) {
        String path = builder.environment().getOrDefault("Path", "");
        builder.environment().put("Path", Path.of(programFiles, "Git", "cmd") + ";"
            + Path.of(programFiles, "Git", "bin") + ";" + path);
      }
    }
    return builder;
  }

  private static boolean isWindows() {
    return System.getProperty("os.name").toLowerCase().contains("win");
  }

  private static String gitBash() {
    List<Path> candidates = new ArrayList<>();
    String programFiles = System.getenv("ProgramFiles");
    if (programFiles != null) {
      candidates.add(Path.of(programFiles, "Git", "bin", "bash.exe"));
    }
    String programFilesX86 = System.getenv("ProgramFiles(x86)");
    if (programFilesX86 != null) {
      candidates.add(Path.of(programFilesX86, "Git", "bin", "bash.exe"));
    }
    return candidates.stream()
        .filter(Files::isExecutable)
        .map(Path::toString)
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Windows tests require Git for Windows (Git Bash) to run repository gates"));
  }

  private static String python() {
    List<Path> candidates = new ArrayList<>();
    String localAppData = System.getenv("LocalAppData");
    if (localAppData == null) localAppData = System.getenv("LOCALAPPDATA");
    if (localAppData != null)
      candidates.add(Path.of(localAppData, "Programs", "Python", "Python313", "python.exe"));
    candidates.add(Path.of(System.getProperty("user.home"), "AppData", "Local", "Programs",
        "Python", "Python313", "python.exe"));
    String userProfile = System.getenv("USERPROFILE");
    if (userProfile != null)
      candidates.add(Path.of(userProfile, "AppData", "Local", "Programs", "Python", "Python313",
          "python.exe"));
    return candidates.stream()
        .filter(Files::exists)
        .map(Path::toString)
        .findFirst()
        .orElse("python3");
  }

  private static String pythonUnixPath() {
    String windowsPath = python();
    if (windowsPath.length() >= 2 && windowsPath.charAt(1) == ':') {
      return "/" + Character.toLowerCase(windowsPath.charAt(0))
          + windowsPath.substring(2).replace('\\', '/');
    }
    return windowsPath;
  }

  private static String gitCommandUnixPath() {
    Path bash = Path.of(gitBash());
    Path command = bash.getParent().getParent().resolve("cmd");
    String windowsPath = command.toString();
    return windowsPath.length() >= 2 && windowsPath.charAt(1) == ':'
        ? "/" + Character.toLowerCase(windowsPath.charAt(0))
            + windowsPath.substring(2).replace('\\', '/')
        : windowsPath.replace('\\', '/');
  }

  private static String bashQuote(String value) {
    return "'" + value.replace("'", "'\\''") + "'";
  }
}
