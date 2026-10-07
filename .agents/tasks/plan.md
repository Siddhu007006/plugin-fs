# Implementation Plan: RealtimeTrigger Production Hardening & Test Validity Fixes

## Overview
Fix 9 issues: 3 P0 production code reorderings, 1 P2 production cleanup, and 5 P0–P2 test rewrites to eliminate false-positive assertions, improve test isolation, and strengthen security validation.

---

## Production Changes

- [ ] 1. **Reorder evaluate() method: compile regex BEFORE creating WatchService**
      
      Current order is: render from → validate from → create WatchService → register directories → compile regex.
      Invalid regex patterns should fail **before** any resources are allocated.
      
      **New order**: render from → render regExp → compile regex (throws IllegalArgumentException immediately) → validate from path → create WatchService → register directories → event loop.
      
      This ensures fail-fast validation and prevents resource leaks on bad regex.
      
      **Files**: `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify**: Run `./gradlew test --tests RealtimeTriggerTest` and confirm all tests pass.

- [ ] 2. **Add validatePath() call for dynamically created directories**
      
      In the evaluate() event loop, when `ENTRY_CREATE` fires for a new subdirectory during recursive watching, the code currently skips validation because of the assumption that child directories inherit security from parent. Change this to always call `validatePath(eventPath, runContext)` **before** calling `registerDirectory(eventPath)`.
      
      Update the comment from "Dynamically created directories are children of already-validated allowed paths, so they inherit the security property" to "Dynamically created directories must be explicitly validated against allowed-paths to enforce consistent security policy".
      
      **Files**: `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify**: Run `./gradlew test --tests RealtimeTriggerTest` and confirm all tests pass, especially the security tests.

- [ ] 3. **Remove duplicate import statement**
      
      `import java.util.concurrent.ConcurrentHashMap;` appears twice in RealtimeTrigger.java (line 23 and line 29).
      Remove one occurrence.
      
      **Files**: `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify**: Run `./gradlew build` and confirm no build errors.

---

## Test Changes

### Context & Constraints

- **Test resource config** (`src/test/resources/application.yml`): RealtimeTrigger is configured with `allowed-paths: ["/tmp", "C:\\Users"]`.
- **Identified issue**: Several tests create temp directories under `/tmp` or `C:\Users`, which are already in the allowed-paths config, so the tests cannot properly isolate "missing" or "forbidden" paths.
- **RunContext limitation**: The `TestsUtils.mockTrigger()` factory uses the default application.yml config. There is **no public API** to override plugin configuration per-test. Tests must either:
  1. Use the test resource allowed-paths as-is and verify behavior within those boundaries (e.g., a path under `/tmp` is allowed, a path outside is denied).
  2. OR: call `trigger.evaluate()` directly with a manually constructed RunContext (if feasible).
  3. OR: construct test directories that satisfy the config: one inside `/tmp` (allowed) and demonstrate rejection outside.

- **Recommended approach for security tests**: Accept that the test resource config allows `/tmp` and `C:\Users`, and construct tests with **two real directories**—one verifiably inside the allowed-paths boundary (e.g., `/tmp/allowed-subdir`) and one verifiably outside (e.g., a sibling not under `/tmp`). Tests must verify the actual behavior under the real config, not hypothetical missing-config scenarios.

---

- [ ] 4. **Rewrite testSecurityMissingAllowedPathsRejected() to properly test isolation**
      
      **Current problem**: The test resource YAML already configures allowed-paths, so the "missing" scenario never happens. The test just checks `receivedExecution[0] == null`, which passes because no events are emitted, but doesn't prove a SecurityException was thrown.
      
      **Decision**: Treat "missing allowed-paths" as equivalent to "RunContext has no configuration at all". Since we cannot override the test resource config per-test, reframe the test as: "If the plugin configuration did not exist, evaluate() should throw SecurityException immediately." We simulate this by:
      1. Creating a trigger with `from` set to a path that does **not** exist (so validation fails early).
      2. Wrapping `trigger.evaluate()` in an `assertThrows(SecurityException.class, ...)` block.
      3. Calling `trigger.evaluate(context.getKey(), context.getValue())` **synchronously** (not in a background thread), wrapped in the assertion.
      4. Verify the SecurityException message mentions "allowed-paths".
      
      **Simpler alternative** (if the above is hard): Use two assertions:
      - Call `trigger.evaluate()` in a thread.
      - Assert that within 2 seconds, **either** `receivedExecution[0]` is null **AND** `capturedError[0]` is a SecurityException, **not** null.
      - Assert the error message contains "allowed-paths".
      
      **Files**: `src/test/java/io/kestra/plugin/fs/local/RealtimeTriggerTest.java`
      
      **Verify**: `./gradlew test --tests 'RealtimeTriggerTest.testSecurityMissingAllowedPathsRejected'` passes and logs confirm SecurityException was thrown.

- [ ] 5. **Rewrite testSecuritySymlinkRejected() to exercise real symlink attack scenario**
      
      **Current problem**: The test sets `recursive=false` and writes directly to the real directory, so the watcher never needs to traverse the symlink. The symlink is created but never used by the watcher.
      
      **New scenario**:
      1. Create `tempDir` as root.
      2. Create a **separate external directory** (outside tempDir): `externalDir = Files.createTempDirectory("external")`.
      3. Start trigger with `from=tempDir`, `recursive=true`.
      4. Call `waitForWatcherReady()`.
      5. **After watcher is ready**, create a symlink inside tempDir pointing to externalDir: `symlink = tempDir.resolve("evil-link") → externalDir`.
      6. This fires an `ENTRY_CREATE` event for the symlink.
      7. The watcher's code checks `Files.isSymbolicLink(symlink)` and rejects it (does not call `registerDirectory`).
      8. Create a file inside externalDir and verify it is **not** detected by the watcher (because the symlink was never registered).
      9. Use `Flux.from(trigger.evaluate(...)).take(1).blockFirst(Duration.ofSeconds(3))` to timeout and assert `null` (no execution).
      
      If symlinks are not supported (catch `UnsupportedOperationException`), gracefully skip with a log message.
      
      **Files**: `src/test/java/io/kestra/plugin/fs/local/RealtimeTriggerTest.java`
      
      **Verify**: `./gradlew test --tests 'RealtimeTriggerTest.testSecuritySymlinkRejected'` passes and confirms symlinks are rejected.

- [ ] 6. **Rewrite testSecurityPathOutsideAllowedRejected() to use verifiable allowed/forbidden boundaries**
      
      **Current problem**: Both temp directories are under `/tmp`, which is in the allowed-paths config, so both are allowed and the test doesn't prove path rejection.
      
      **New approach**:
      1. Read `application.yml` to confirm allowed-paths are `["/tmp", "C:\\Users"]`.
      2. Create allowed directory as a subdirectory of `/tmp`: `allowedDir = Files.createTempDirectory(Paths.get("/tmp"), "allowed")` (or under C:\Users on Windows).
      3. Create a forbidden directory **outside** the allowed-paths: `forbiddenDir = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), "forbidden")` if that tmpdir is not under `/tmp`, OR use a manually crafted path like `/var/forbidden` that is not in the config.
      4. Start a trigger with `from=allowedDir`, verify it succeeds and detects a file created in `allowedDir`.
      5. Create a second trigger with `from=forbiddenDir` and expect it to throw SecurityException during `evaluate()` (verify in `assertThrows` or check for exception in error handler).
      6. Verify the exception message mentions allowed-paths.
      
      **If Windows prevents /var/forbidden**: On Windows, create a temp dir under C:\Temp (not C:\Users), and configure the test to expect that to be forbidden.
      
      **Alternative if step 5 is not feasible**: Use one trigger watching allowedDir, then try to dynamically create a file with a path string that points outside allowedDir (by path manipulation), and verify the watcher rejects it via validatePath().
      
      **Files**: `src/test/java/io/kestra/plugin/fs/local/RealtimeTriggerTest.java`
      
      **Verify**: `./gradlew test --tests 'RealtimeTriggerTest.testSecurityPathOutsideAllowedRejected'` passes and confirms path validation works correctly.

- [ ] 7. **Strengthen testSecurityInvalidRegexThrowsException() assertion**
      
      **Current problem**: The test checks `capturedError[0] != null`, which is too loose and doesn't verify the error type or message.
      
      **Changes**:
      1. Assert `capturedError[0] instanceof IllegalArgumentException` (or wrap the check in an if and fail if wrong type).
      2. Assert the cause chain: if `capturedError[0].getCause() != null`, check `capturedError[0].getCause() instanceof PatternSyntaxException`.
      3. Assert the message contains "regex", "pattern", or "Invalid" (case-insensitive).
      4. With the regex-compiled-first reordering (item 1), confirm this exception is thrown **before** WatchService creation (i.e., immediately, not after a delay).
      
      **Files**: `src/test/java/io/kestra/plugin/fs/local/RealtimeTriggerTest.java`
      
      **Verify**: `./gradlew test --tests 'RealtimeTriggerTest.testSecurityInvalidRegexThrowsException'` passes with stricter assertions.

- [ ] 8. **Rewrite testMultipleEventsInSequence() to collect multiple events from ONE trigger instance**
      
      **Current problem**: The test uses two separate trigger instances, so it proves two triggers work, not one watcher detecting multiple events (required by Issue #365).
      
      **New scenario**:
      1. Create ONE trigger instance.
      2. Start it in a background thread.
      3. Call `waitForWatcherReady()`.
      4. Create file1.txt, wait 200ms, create file2.txt.
      5. Use `Flux.from(trigger.evaluate(...)).take(2).collectList().block(Duration.ofSeconds(15))` to collect exactly 2 executions from the same watcher instance.
      6. Assert the list has exactly 2 elements.
      7. Assert both executions are for the same changeType (CREATE or both CREATE_OR_UPDATE).
      8. Assert the files are file1.txt and file2.txt (in any order, or verify specific order if the watcher guarantees it).
      9. Call `trigger.stop()` after the list is collected.
      
      **Files**: `src/test/java/io/kestra/plugin/fs/local/RealtimeTriggerTest.java`
      
      **Verify**: `./gradlew test --tests 'RealtimeTriggerTest.testMultipleEventsInSequence'` passes and confirms one watcher detects multiple file events.

- [ ] 9. **Expand testRegexConsistencyAcrossEvents() to cover UPDATE and DELETE**
      
      **Current problem**: The test is named "RegexConsistencyAcrossEvents" but only tests CREATE. UPDATE and DELETE are not covered, so the name is misleading.
      
      **Option A**: Rename to `testRegexFilterDuringCreateOnly()` and document that the test covers CREATE only.
      
      **Option B** (preferred): Rename to `testRegexConsistencyAcrossEventTypes()` and expand to verify regex is applied to CREATE, UPDATE, and DELETE:
      1. Use a single trigger with `on=CREATE_OR_UPDATE` and `regExp=".*\\.csv$"`.
      2. Create a CSV file (should trigger).
      3. Modify the CSV file (should trigger).
      4. Create a non-CSV file, verify it is ignored.
      5. Collect multiple executions, verify all are for CSV files.
      6. (Optional) Use a second trigger with `on=DELETE` and verify DELETE events also respect the regex filter.
      
      Use `Flux.from(trigger.evaluate(...)).take(2).collectList().block()` to collect exactly the expected number of events.
      
      **Files**: `src/test/java/io/kestra/plugin/fs/local/RealtimeTriggerTest.java`
      
      **Verify**: `./gradlew test --tests 'RealtimeTriggerTest.testRegexConsistencyAcrossEventTypes'` passes and confirms regex is consistently applied across CREATE, UPDATE, and DELETE.

- [ ] 10. **Fix testThreadLifecycleAndNaming() to remove arbitrary sleep and assert thread name**
      
      **Current problem**: Uses `Thread.sleep(100)` before checking thread alive status (nondeterministic). Also prints thread name but doesn't assert it.
      
      **Changes**:
      1. Replace `Thread.sleep(100)` with the existing `waitForWatcherReady(trigger, 5)` helper.
      2. Add an assertion that the thread name contains "RealtimeTrigger" or matches the expected pattern.
      3. The test already has `watcherThread.setName("RealtimeTrigger-LifecycleTest")`, so assert: `assertTrue(watcherThread.getName().contains("RealtimeTrigger"))`.
      
      **Files**: `src/test/java/io/kestra/plugin/fs/local/RealtimeTriggerTest.java`
      
      **Verify**: `./gradlew test --tests 'RealtimeTriggerTest.testThreadLifecycleAndNaming'` passes without sleeps and with thread name assertion.

---

## Summary of Changes

| # | Type | File | Impact | Verification |
|---|------|------|--------|--------------|
| 1 | Prod | RealtimeTrigger.java | Reorder: compile regex first | All tests pass, fail-fast on bad regex |
| 2 | Prod | RealtimeTrigger.java | Add validatePath() for dynamic dirs | Security test passes |
| 3 | Prod | RealtimeTrigger.java | Remove duplicate import | Build succeeds |
| 4 | Test | RealtimeTriggerTest.java | Rewrite missing-allowed-paths test | SecurityException assertion passes |
| 5 | Test | RealtimeTriggerTest.java | Rewrite symlink rejection test | Symlink rejection confirmed |
| 6 | Test | RealtimeTriggerTest.java | Rewrite outside-allowed-path test | Path boundary validation confirmed |
| 7 | Test | RealtimeTriggerTest.java | Strengthen invalid-regex assertion | Strict error type checking |
| 8 | Test | RealtimeTriggerTest.java | Rewrite multiple events (one watcher) | One watcher, multiple events confirmed |
| 9 | Test | RealtimeTriggerTest.java | Expand regex across event types | Regex applied to CREATE, UPDATE, DELETE |
| 10 | Test | RealtimeTriggerTest.java | Fix thread lifecycle (remove sleep, assert name) | No arbitrary sleeps, thread name asserted |

---

## Known Ambiguities & Resolutions

1. **RunContext plugin config override for tests**: No public API exists to override allowed-paths per-test. **Resolution**: Tests accept the config from application.yml and construct directories that satisfy the real config (e.g., paths under `/tmp` are allowed, paths outside are denied). Tests verify actual behavior, not hypothetical missing-config scenarios.

2. **Symlink support on Windows**: Symlink creation may fail on Windows without elevated privileges. **Resolution**: Catch `UnsupportedOperationException` and gracefully skip the test with a log message.

3. **testSecurityPathOutsideAllowedRejected directory creation**: Creating a directory outside `/tmp` on Unix may fail due to permissions. **Resolution**: On Unix, use a path that is known to exist but not in the allowed-paths config (e.g., `/var/forbidden` if writable, or `/root` if testable). On Windows, use C:\Temp if not in the config. If none are writable, the test documents the limitation and skips gracefully.

4. **Multiple events collection**: `Flux.take(N).collectList()` blocks until N events arrive or timeout. **Resolution**: Set timeout to 15 seconds, which matches test `@Timeout`. If test completes before timeout, success; if timeout fires, test fails with clear message.

