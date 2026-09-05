// SPDX-License-Identifier: Apache-2.0
package binjava;

/**
 * Ambient git state, removed from a child process's environment (M0.57).
 *
 * <p>⚠️ {@code GIT_INDEX_FILE} IS INHERITED, so a harness test that shells out
 * to {@code git add} inside its own temp repository writes into whatever index
 * its PARENT had set. That is not hypothetical: {@code review.sh context} runs
 * the whole gate loop, {@code check-harness-tests.sh} runs
 * {@code ./gradlew -p buildSrc test} inside it, and generating a review packet
 * under a private index therefore made this suite write its own fixture paths
 * into that index. Measured: {@code subject.txt} appeared in a private index
 * pointing at a blob unreachable from the real repository, and
 * {@code git checkout-index} then failed on it.
 *
 * <p>⚠️ SEPARATE FILE ON PURPOSE, not tidiness. A red record binds to the
 * sha256 of the TEST file it was observed in, so a helper living beside its own
 * test can never be mutated to produce one — mutating it changes the very hash
 * the record would bind to. Here the subject and the test are in different
 * files, so the mutation is recordable and the strip is falsifiable.
 *
 * <p>⚠️ THE SAME HOLE IS OPEN IN EVERY OTHER HARNESS TEST that runs git in a
 * temp repository — {@code ReviewRolesTest} and {@code FreshCheckoutTest} among
 * them. They are not changed here: M0.57 does not own them, and a silent
 * cross-file fix is how a row's scope stops meaning anything. This class exists
 * for them to adopt when that row is filed.
 */
final class GitEnv {

  private GitEnv() {
  }

  /** Removes every inherited {@code GIT_*} variable from {@code b}. */
  static ProcessBuilder stripped(ProcessBuilder b) {
    b.environment().keySet().removeIf(k -> k.startsWith("GIT_"));
    return b;
  }
}
