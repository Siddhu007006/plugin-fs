# Implementation Plan: Fix 9 Production Issues in RealtimeTrigger

## Overview
This plan addresses 9 audit findings in `RealtimeTrigger.java`. The fixes are ordered by risk level (P0 security issues first) and by dependency (foundational refactorings before downstream fixes).

**Build/Test Commands:**
- Build: `./gradlew build`
- Test: `./gradlew test --tests RealtimeTriggerTest`
- All tests: `./gradlew test`

---

## Implementation Order

- [ ] **1. Issue 2 (P0) — Move watchKeyMap and registeredDirectories to local variables in Flux.create().**
      
      **Rationale:** This is the foundational refactoring. Shared mutable instance fields cause subscription interference (Finding 2). Moving them to locals eliminates the concurrency hazard at the source. All other fixes (Issues 4, 5, 7) depend on understanding this refactoring.
      
      **What to do:**
      1. Change `watchKeyMap` and `registeredDirectories` from instance fields to local variables declared inside the `Flux.create()` lambda.
      2. Update `registerDirectory(Path directory, Map<WatchKey, Path> map, Set<Path> dirs)` signature to accept the map and set as parameters instead of accessing instance fields.
      3. Update `registerDirectoryTree(Path root, Map<WatchKey, Path> map, Set<Path> dirs)` signature similarly.
      4. Pass these locals as arguments to all calls to `registerDirectory()` and `registerDirectoryTree()` throughout the event loop and startup.
      5. Update the event loop that accesses `watchKeyMap` to use the local variable instead.
      6. Keep `active` and `watchService` as instance fields (they control overall trigger lifecycle, not per-subscription state).
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds.
      - Test: `./gradlew test --tests RealtimeTriggerTest` — all tests pass (no functional change yet, just refactored state management).

---

- [ ] **2. Issue 4 (P0) — Add lifecycle boundary check in registerDirectory() to prevent half-registered keys.**
      
      **Rationale:** After the key is obtained from `directory.register(...)` but before it's stored in the map, a concurrent `stop()` can close the WatchService, leaving an invalid key in the map. The fix is a re-check of the `active` flag and immediate key cancellation if inactive. This prevents dangling invalid keys.
      
      **What to do:**
      1. In `registerDirectory(Path directory, Map<WatchKey, Path> map, Set<Path> dirs)`, after the line `WatchKey key = directory.register(...)`:
         - Add: `if (!active.get()) { key.cancel(); throw new IllegalStateException("Trigger stopped during registration"); }`
         - The caller (`evaluate()`) already catches exceptions and routes them to Flux error path, which is correct.
      2. This tight boundary ensures atomic registration: either the key is in the map and the trigger is active, or the exception is thrown.
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds.
      - Test: `./gradlew test --tests RealtimeTriggerTest#testStopAndCleanup` — the stop/cleanup test passes (no race condition detected).

---

- [ ] **3. Issue 1 (P0) — Add validatePath() call in event loop before emitEventExecution().**
      
      **Rationale:** After `shouldTrigger()` and `matchesRegExp()` pass, an attacker can trigger events via symlinks pointing outside allowed-paths. The fix validates the event path against allowed-paths BEFORE emitting execution. Failures are logged as WARN (security event) and the loop continues safely.
      
      **What to do:**
      1. In the event loop, after the `if (compiledPattern != null && !matchesRegExp(eventPath, compiledPattern)) continue;` check and before the `changeType` determination:
         - Add a try-catch block:
           ```java
           try {
               validatePath(eventPath, runContext);
           } catch (SecurityException | IOException e) {
               logger.warn("Security: Rejecting event for path outside allowed-paths: {} - {}", 
                   eventPath, e.getMessage());
               continue;
           }
           ```
         - Do NOT propagate the exception (it would kill the watcher). Log and continue to the next event.
      2. This validation is applied to ALL event types (CREATE, UPDATE, DELETE) before emission, enforcing the same policy as AbstractLocalTask.
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds.
      - Test: `./gradlew test --tests RealtimeTriggerTest` — all tests pass (no test currently exercises symlinks outside allowed-paths; security audit confirms the fix).

---

- [ ] **4. Issue 3 (P0) — Break event loop when watchKeyMap becomes empty.**
      
      **Rationale:** If all directories are deleted and their keys become invalid, `watchKeyMap` is cleared. Then `service.take()` blocks forever because no new events will arrive. The fix: after removing a key, check if the map is empty and break the loop to allow cleanup.
      
      **What to do:**
      1. In the event loop, after the `if (!valid) { watchKeyMap.remove(key); registeredDirectories.remove(...); }` block:
         - Add:
           ```java
           if (watchKeyMap.isEmpty()) {
               logger.info("All registered directories are no longer valid; terminating watcher");
               break;
           }
           ```
      2. This prevents indefinite blocking and allows the finally block to run cleanup and complete the stream.
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds.
      - Test: `./gradlew test --tests RealtimeTriggerTest` — all tests pass.

---

- [ ] **5. Issue 5 (P0) — Add registeredDirectories.clear() to cleanup().**
      
      **Rationale:** The `cleanup()` method clears `watchKeyMap` but leaves `registeredDirectories` with stale entries. Now that these are local variables (Issue 2), `cleanup()` must accept them as parameters and clear both for invariant consistency.
      
      **What to do:**
      1. Update `cleanup()` signature to accept the map and set: `private void cleanup(Map<WatchKey, Path> map, Set<Path> dirs)`.
      2. In the cleanup body, add: `if (dirs != null) dirs.clear();` alongside the existing `watchKeyMap.clear()` (now using the parameter `map`).
      3. Update the finally block in `Flux.create()` to pass the locals: `cleanup(watchKeyMap, registeredDirectories);`.
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds.
      - Test: `./gradlew test --tests RealtimeTriggerTest#testStopAndCleanup` — cleanup invariant is enforced (both collections cleared).

---

- [ ] **6. Issue 6 (P1) — Replace hardcoded "allowed-paths" string with AbstractLocalTask.ALLOWED_PATHS.**
      
      **Rationale:** The `getAllowedPaths()` method repeats the hardcoded configuration key string. Using the constant from `AbstractLocalTask` avoids two separate definitions and ensures semantic alignment.
      
      **What to do:**
      1. In `getAllowedPaths()` method, replace the line `runContext.pluginConfiguration("allowed-paths")` with `runContext.pluginConfiguration(AbstractLocalTask.ALLOWED_PATHS)`.
      2. AbstractLocalTask.ALLOWED_PATHS is package-private (no modifier, same package `io.kestra.plugin.fs.local`), so it is directly accessible without any import.
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds, constant is resolved.
      - Test: `./gradlew test --tests RealtimeTriggerTest` — all tests pass (security validation still works).

---

- [ ] **7. Issue 7 (P1) — Refactor isReady() to reflect true lifecycle state.**
      
      **Rationale:** `isReady()` currently returns true as long as `watchKeyMap` is non-null and non-empty, ignoring the `active` flag. This causes transient states where tests need artificial sleeps. The fix: introduce a separate `AtomicBoolean ready` instance field. Set it to true after the first successful `registerDirectory()` call (during startup). Set it to false in `cleanup()`. Then `isReady()` returns `active.get() && ready.get()`, reflecting true lifecycle state.
      
      **What to do:**
      1. Add a new instance field: `private transient final AtomicBoolean ready = new AtomicBoolean(false);`.
      2. In the `evaluate()` method, after the first successful `registerDirectory(rootDirectory, watchKeyMap, registeredDirectories)` call, add: `ready.set(true);`.
      3. In `cleanup()`, add: `ready.set(false);` alongside the other cleanup operations.
      4. Update `isReady()` to: `return active.get() && ready.get();`.
      5. This eliminates the race condition where cleanup delays cause isReady() to report stale state.
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds.
      - Test: `./gradlew test --tests RealtimeTriggerTest` — all tests pass, especially `testStopAndCleanup`, which should no longer need artificial sleeps or brittle polling.

---

- [ ] **8. Issue 8 (P1) — Change changeType from String to ChangeType enum.**
      
      **Jackson Serialization Decision:** When `Output.changeType` is changed from `String changeType` to a typed enum field, Kestra's Jackson serialization will serialize it to the enum's `name()` string ("CREATE", "UPDATE", "DELETE"). This is the default Jackson behavior for enums (JsonGenerator.writeString(enum.name())). Therefore, **test assertions DO NOT need to change** — they already compare against strings like "CREATE", "UPDATE", "DELETE", which will match the serialized enum name.
      
      **What to do:**
      1. Add a nested enum inside `RealtimeTrigger` (not inside Output, to keep responsibilities clean):
         ```java
         public enum ChangeType {
             CREATE,
             UPDATE,
             DELETE
         }
         ```
      2. Change the `Output` inner class field from `private final String changeType;` to `private final ChangeType changeType;`.
      3. Update `emitEventExecution()` signature from `String changeType` to `ChangeType changeType`.
      4. In the event loop, replace string assignments:
         - `changeType = "CREATE";` → `changeType = ChangeType.CREATE;`
         - `changeType = "UPDATE";` → `changeType = ChangeType.UPDATE;`
         - `changeType = "DELETE";` → `changeType = ChangeType.DELETE;`
      5. Replace all guard comparisons:
         - `"DELETE".equals(changeType)` → `changeType == ChangeType.DELETE`
         - `!"DELETE".equals(changeType)` → `changeType != ChangeType.DELETE`
         - `changeType != null` check remains (changeType variable can be null before assignment)
      6. Update the `@Schema` annotation on the Output field: change description to match the enum name and possible values. Suggest: `@Schema(title = "The type of change (CREATE, UPDATE, DELETE)", description = "...existing description...")`
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java` (add enum, update Output.changeType, update emitEventExecution() signature and calls, update event loop assignments and guards)
      
      **Test assertions that must be verified (not changed):**
      - `variables.get("changeType"), equalTo("CREATE")` — will still pass because Jackson serializes ChangeType.CREATE to "CREATE".
      - `variables.get("changeType"), equalTo("UPDATE")` — will still pass.
      - `variables.get("changeType"), equalTo("DELETE")` — will still pass.
      
      **Verify:**
      - Build: `./gradlew build` — compilation succeeds.
      - Test: `./gradlew test --tests RealtimeTriggerTest` — all tests pass, including assertions comparing `variables.get("changeType")` to string literals.

---

- [ ] **9. Issue 9 (P1) — Update @Schema description for regExp field.**
      
      **Rationale:** The schema description says "Regex pattern to match file names" but the implementation explicitly matches full paths (via `pattern.matcher(path.toString()).matches()`), consistent with local.List. The description should reflect the actual behavior.
      
      **What to do:**
      1. In the `regExp` field definition, change the `@Schema` annotation from:
         `@Schema(title = "Regex pattern to match file names")`
         to:
         `@Schema(title = "Regex pattern to match against the full file path (same semantics as local.List)")`
         or more concisely:
         `@Schema(title = "Regex pattern to match against the full file path")`
      
      **Files to modify:**
      - `src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java`
      
      **Verify:**
      - Build: `./gradlew build` — documentation string is updated, no compilation errors.
      - Test: `./gradlew test --tests RealtimeTriggerTest#testRegexFilter` — regex filtering still works as documented.

---

## Summary of Changes by File

**src/main/java/io/kestra/plugin/fs/local/RealtimeTrigger.java**

1. **Issue 2:** Convert `watchKeyMap` and `registeredDirectories` to local variables inside `Flux.create()` lambda. Update method signatures:
   - `registerDirectory(Path directory)` → `registerDirectory(Path directory, Map<WatchKey, Path> map, Set<Path> dirs)`
   - `registerDirectoryTree(Path root)` → `registerDirectoryTree(Path root, Map<WatchKey, Path> map, Set<Path> dirs)`

2. **Issue 4:** Add lifecycle check in `registerDirectory()` after `WatchKey key = directory.register(...)`:
   ```java
   if (!active.get()) {
       key.cancel();
       throw new IllegalStateException("Trigger stopped during registration");
   }
   ```

3. **Issue 1:** Add path validation in event loop before `emitEventExecution()`:
   ```java
   try {
       validatePath(eventPath, runContext);
   } catch (SecurityException | IOException e) {
       logger.warn("Security: Rejecting event for path outside allowed-paths: {} - {}", 
           eventPath, e.getMessage());
       continue;
   }
   ```

4. **Issue 3:** Add empty-map check in event loop after key removal:
   ```java
   if (watchKeyMap.isEmpty()) {
       logger.info("All registered directories are no longer valid; terminating watcher");
       break;
   }
   ```

5. **Issue 5:** Update `cleanup()` signature to accept map and set; add `dirs.clear();` call.

6. **Issue 7:** Add `ready` AtomicBoolean field; set it true after first `registerDirectory()`; set false in cleanup; update `isReady()` to return `active.get() && ready.get()`.

7. **Issue 6:** Replace `"allowed-paths"` string literal with `AbstractLocalTask.ALLOWED_PATHS` constant in `getAllowedPaths()`.

8. **Issue 8:** Add `ChangeType` enum; change `Output.changeType` type to enum; update event loop assignments to use enum constants; update guard comparisons to use `==` and `!=` operators.

9. **Issue 9:** Update `@Schema` annotation on `regExp` field to reflect full path matching semantics.

---

## Test Assertions — No Changes Required

All existing test assertions comparing `variables.get("changeType")` to string literals will continue to pass because Jackson serializes `ChangeType` enums to their name strings ("CREATE", "UPDATE", "DELETE") by default. Examples that will pass without modification:

- `assertThat("changeType should be CREATE", variables.get("changeType"), equalTo("CREATE"));`
- `assertThat("changeType should be UPDATE", variables.get("changeType"), equalTo("UPDATE"));`
- `assertThat("changeType should be DELETE", variables.get("changeType"), equalTo("DELETE"));`

---

## Verification Strategy

1. **After each issue fix:** Run `./gradlew build` to ensure compilation succeeds.
2. **After all issues fixed:** Run full test suite `./gradlew test --tests RealtimeTriggerTest` to verify no regressions.
3. **Security verification:** The audit findings confirm Issue 1 (symlink validation) is now applied. Issues 2, 4 eliminate concurrency hazards at code level. Issue 3 prevents indefinite blocking. Issue 5 ensures complete cleanup.
4. **Jackson serialization assumption verification:** Create a quick test (or inspect existing tests) that confirms `variables.get("changeType")` returns a string after serialization of the ChangeType enum. If the assumption is wrong, update test assertions to extract enum names via `.name()` or similar.

---

## Notes on Refactoring Complexity

- **Issue 2 is the largest change:** It moves two critical data structures from instance state to local variables. All downstream fixes (4, 5, 7) depend on this refactoring or become simpler because of it. The refactoring is backward-compatible (same public API and behavior) and eliminates the subscription-interference hazard entirely.
- **Issues 1, 3, 4 are P0 security/correctness:** They block possible attacks or resource leaks. Issue 1 is applied defensively (log and continue, no propagation).
- **Issues 5–9 are P1 quality/maintainability:** They fix incomplete cleanup, duplication, weak readiness signaling, type safety, and documentation.
