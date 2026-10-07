package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Security-focused integration tests for RealtimeTrigger.
 * Tests allowed-paths, symlinks, regex validation, and path boundary enforcement.
 */
@KestraTest
class RealtimeTriggerSecurityTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static void waitForWatcherReady(RealtimeTrigger trigger, int timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(timeoutSeconds).toNanos();

        while (!trigger.isReady() && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }

        if (!trigger.isReady()) {
            throw new AssertionError("Watcher not ready after " + timeoutSeconds + " seconds");
        }
    }

    @Test
    @Timeout(15)
    void testSecurityMissingAllowedPathsRejected() throws Exception {
        // Test that trigger fails when allowed-paths is not configured.
        // The test resource config allows /tmp and C:\Users, so we attempt
        // to start a watcher on a path outside those boundaries and expect rejection.

        Path outsideDir = null;
        try {
            // Create a temp directory that is guaranteed NOT under /tmp or C:\Users
            // On Windows, use a different drive/path. On Unix, use /var/tmp or similar.
            String osName = System.getProperty("os.name");
            if (osName.contains("Windows")) {
                // Windows: try to use C:\Temp (not C:\Users which is allowed)
                outsideDir = Paths.get("C:\\Temp\\realtime-trigger-test-outside");
                try {
                    Files.createDirectories(outsideDir);
                } catch (IOException e) {
                    // C:\Temp may not be writable; try an alternative
                    outsideDir = Paths.get(System.getProperty("user.home")).resolve("..\\..\\realtime-trigger-test-outside");
                    Files.createDirectories(outsideDir);
                }
            } else {
                // Unix/Linux: /var/tmp or use the temp directory but verify it's not under /tmp
                String tmpDir = System.getProperty("java.io.tmpdir");
                if (tmpDir.startsWith("/tmp")) {
                    // java.io.tmpdir is /tmp, so create a sibling directory
                    outsideDir = Paths.get("/var/realtime-trigger-test-outside");
                    try {
                        Files.createDirectories(outsideDir);
                    } catch (IOException e) {
                        // /var may not be writable; use home directory approach
                        outsideDir = Paths.get(System.getProperty("user.home")).resolve("../realtime-trigger-test-outside");
                        Files.createDirectories(outsideDir);
                    }
                } else {
                    outsideDir = Paths.get(tmpDir).resolve("realtime-trigger-test-outside");
                    Files.createDirectories(outsideDir);
                }
            }

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-missing-allowed-paths")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(outsideDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            Throwable[] capturedError = new Throwable[1];
            Thread watcherThread = new Thread(() -> {
                try {
                    Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(5));
                } catch (Throwable e) {
                    capturedError[0] = e;
                }
            });

            watcherThread.setName("RealtimeTrigger-SecurityTest-MissingAllowedPaths");
            watcherThread.start();
            watcherThread.join(8000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify that a security exception occurred
            assertThat("Trigger should have failed with a security error for path outside allowed-paths",
                capturedError[0], notNullValue());

            String errorMessage = capturedError[0].getMessage().toLowerCase();
            assertThat("Error should mention allowed-paths configuration",
                errorMessage, anyOf(
                    containsString("allowed-path"),
                    containsString("access denied"),
                    containsString("security")
                ));

            System.out.println("\n✓ Security Test: Missing/Outside Allowed-Paths Rejected Passed:");
            System.out.println("  Trigger rejected access to path outside allowed-paths: " + outsideDir);
            System.out.println("  Error type: " + capturedError[0].getClass().getSimpleName());
            System.out.println("  Error message: " + capturedError[0].getMessage());

        } finally {
            if (outsideDir != null) {
                try {
                    Files.deleteIfExists(outsideDir);
                } catch (Exception e) {
                    // Cleanup may fail; ignore
                }
            }
        }
    }

    @Test
    @Timeout(15)
    void testSecuritySymlinkRejected() throws Exception {
        // Test that symlinks are rejected to prevent directory traversal attacks.
        // Scenario:
        //   1. Start watcher with recursive=true on tempDir
        //   2. After startup, create a symlink inside tempDir
        //   3. Symlink ENTRY_CREATE event is received
        //   4. Code checks Files.isSymbolicLink() and rejects it
        //   5. Watcher does not register the symlink target
        //   6. Files created through the symlink path are not detected

        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path externalDir = Files.createTempDirectory("realtime-trigger-external");
        Path symlinkInTempDir = tempDir.resolve("evil-link");
        Path fileInExternalDir = externalDir.resolve("file.txt");

        boolean symlinkCreated = false;

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-symlink-rejection")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .recursive(Property.ofValue(true))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            Execution[] receivedExecution = new Execution[1];
            Thread watcherThread = new Thread(() -> {
                try {
                    receivedExecution[0] = Flux.from(
                            trigger.evaluate(context.getKey(), context.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    System.err.println("✗ Watcher error: " + e.getMessage());
                }
            });

            watcherThread.setName("RealtimeTrigger-SecurityTest-Symlink");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);
            System.out.println("✓ Watcher initialized and ready");

            // Now create a symlink AFTER the watcher is running (this triggers ENTRY_CREATE)
            try {
                Files.createSymbolicLink(symlinkInTempDir, externalDir);
                symlinkCreated = true;
                System.out.println("✓ Created symlink AFTER watcher startup: " + symlinkInTempDir + " → " + externalDir);
            } catch (UnsupportedOperationException | IOException e) {
                System.out.println("⊘ Symlink creation not supported on this platform; skipping symlink test");
                trigger.stop();
                watcherThread.join(2000);
                return;
            }

            // Give the watcher a moment to process the ENTRY_CREATE for the symlink
            Thread.sleep(500);

            // Now try to create a file inside the external directory
            // The symlink should have been rejected, so this file should NOT be detected
            Files.write(fileInExternalDir, "content".getBytes());
            System.out.println("✓ Created file in external directory (via symlink path): " + fileInExternalDir);

            // Wait for execution with timeout
            // Since the symlink was rejected, no execution should occur
            receivedExecution[0] = Flux.from(
                    trigger.evaluate(context.getKey(), context.getValue())
                )
                .subscribeOn(Schedulers.boundedElastic())
                .blockFirst(Duration.ofSeconds(3));

            watcherThread.join(12000);
            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Symlinks should be rejected - no execution should be received
            assertThat("Symlink should be rejected; file through symlink should not be detected",
                receivedExecution[0], nullValue());

            System.out.println("\n✓ Security Test: Symlink Rejection Passed:");
            System.out.println("  Symlinks are blocked to prevent directory traversal");
            System.out.println("  File created through symlink was not detected");

        } finally {
            if (symlinkCreated) {
                try {
                    Files.deleteIfExists(symlinkInTempDir);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
            Files.deleteIfExists(fileInExternalDir);
            Files.deleteIfExists(externalDir);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityPathOutsideAllowedRejected() throws Exception {
        // Test that files outside allowed-paths are rejected.
        // Two-part test:
        // 1. Start watcher on a directory inside allowed-paths (should succeed)
        // 2. Verify file creation is detected
        // 3. Verify attempt to start watcher on a directory outside allowed-paths fails

        // From application.yml, allowed-paths are /tmp and C:\Users
        // Create allowed directory under /tmp
        Path allowedDir = Files.createTempDirectory("allowed");
        Path allowedFile = allowedDir.resolve("allowed.txt");

        // Create forbidden directory outside allowed-paths
        Path forbiddenDir = null;
        try {
            String osName = System.getProperty("os.name");
            if (osName.contains("Windows")) {
                // C:\Temp is not in allowed-paths (only C:\Users is allowed)
                forbiddenDir = Paths.get("C:\\Temp\\realtime-trigger-forbidden");
                try {
                    Files.createDirectories(forbiddenDir);
                } catch (IOException e) {
                    System.out.println("⊘ Cannot create C:\\Temp; skipping forbidden dir part of test");
                    forbiddenDir = null;
                }
            } else {
                // /var/tmp is not in allowed-paths (only /tmp is allowed)
                forbiddenDir = Paths.get("/var/realtime-trigger-forbidden");
                try {
                    Files.createDirectories(forbiddenDir);
                } catch (IOException e) {
                    System.out.println("⊘ Cannot create /var directory; skipping forbidden dir part of test");
                    forbiddenDir = null;
                }
            }

            // Part 1: Start watcher on ALLOWED directory and verify it detects files
            RealtimeTrigger allowedTrigger = RealtimeTrigger.builder()
                .id("test-path-outside-allowed-1")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(allowedDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var allowedContext = TestsUtils.mockTrigger(runContextFactory, allowedTrigger);

            Execution[] allowedExecution = new Execution[1];
            Thread allowedWatcher = new Thread(() -> {
                try {
                    allowedExecution[0] = Flux.from(
                            allowedTrigger.evaluate(allowedContext.getKey(), allowedContext.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    System.err.println("✗ Allowed watcher error: " + e.getMessage());
                }
            });

            allowedWatcher.setName("RealtimeTrigger-SecurityTest-AllowedPath");
            allowedWatcher.start();

            waitForWatcherReady(allowedTrigger, 5);
            System.out.println("✓ Watcher started on ALLOWED directory: " + allowedDir);

            // Create file in allowed directory
            Files.write(allowedFile, "allowed".getBytes());
            System.out.println("✓ Created file in allowed directory");

            allowedWatcher.join(12000);
            if (allowedWatcher.isAlive()) {
                allowedTrigger.stop();
                allowedWatcher.join(2000);
            }

            // Verify execution was received for allowed path
            assertThat("Should have received execution for file in allowed directory",
                allowedExecution[0], notNullValue());

            System.out.println("✓ File in allowed directory was detected");

            // Part 2: Attempt to start watcher on FORBIDDEN directory (if we could create one)
            if (forbiddenDir != null) {
                RealtimeTrigger forbiddenTrigger = RealtimeTrigger.builder()
                    .id("test-path-outside-allowed-2")
                    .type(RealtimeTrigger.class.getName())
                    .from(Property.ofValue(forbiddenDir.toString()))
                    .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                    .build();

                var forbiddenContext = TestsUtils.mockTrigger(runContextFactory, forbiddenTrigger);

                Throwable[] forbiddenError = new Throwable[1];
                Thread forbiddenWatcher = new Thread(() -> {
                    try {
                        Flux.from(forbiddenTrigger.evaluate(forbiddenContext.getKey(), forbiddenContext.getValue()))
                            .subscribeOn(Schedulers.boundedElastic())
                            .blockFirst(Duration.ofSeconds(5));
                    } catch (Throwable e) {
                        forbiddenError[0] = e;
                    }
                });

                forbiddenWatcher.setName("RealtimeTrigger-SecurityTest-ForbiddenPath");
                forbiddenWatcher.start();
                forbiddenWatcher.join(8000);

                if (forbiddenWatcher.isAlive()) {
                    forbiddenTrigger.stop();
                    forbiddenWatcher.join(2000);
                }

                assertThat("Should have failed to start watcher on forbidden directory",
                    forbiddenError[0], notNullValue());

                String errorMsg = forbiddenError[0].getMessage().toLowerCase();
                assertThat("Error should mention allowed-paths",
                    errorMsg, anyOf(
                        containsString("allowed-path"),
                        containsString("access denied"),
                        containsString("security")
                    ));

                System.out.println("✓ Watcher correctly rejected forbidden directory: " + forbiddenDir);
            }

            System.out.println("\n✓ Security Test: Path Outside Allowed Rejected Passed:");
            System.out.println("  Allowed directory: files were detected");
            if (forbiddenDir != null) {
                System.out.println("  Forbidden directory: watcher startup was rejected");
            }

        } finally {
            Files.deleteIfExists(allowedFile);
            Files.deleteIfExists(allowedDir);
            if (forbiddenDir != null) {
                try {
                    Files.deleteIfExists(forbiddenDir);
                } catch (Exception e) {
                    // Cleanup may fail; ignore
                }
            }
        }
    }

    @Test
    @Timeout(15)
    void testSecurityInvalidRegexThrowsException() throws Exception {
        // Test that invalid regex patterns fail early with clear error.
        // With the reordering, regex compilation happens BEFORE WatchService creation.

        Path tempDir = Files.createTempDirectory("realtime-trigger-test");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-invalid-regex")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue("[invalid(regex"))  // Invalid: unclosed bracket
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            Throwable[] capturedError = new Throwable[1];
            Thread watcherThread = new Thread(() -> {
                try {
                    Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(5));
                } catch (Throwable e) {
                    capturedError[0] = e;
                }
            });

            watcherThread.setName("RealtimeTrigger-SecurityTest-InvalidRegex");
            watcherThread.start();
            watcherThread.join(8000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify that an error was thrown
            assertThat("Invalid regex should cause an exception",
                capturedError[0], notNullValue());

            // Check for IllegalArgumentException (our wrapper)
            assertThat("Error should be IllegalArgumentException or wrapper",
                capturedError[0].getClass().getSimpleName(),
                anyOf(equalTo("IllegalArgumentException"), containsString("Exception")));

            // Check cause chain for PatternSyntaxException
            if (capturedError[0].getCause() != null) {
                assertThat("Root cause should be PatternSyntaxException",
                    capturedError[0].getCause().getClass().getSimpleName(),
                    equalTo("PatternSyntaxException"));
            }

            String errorMessage = capturedError[0].getMessage().toLowerCase();
            assertThat("Error message should indicate regex validation failure",
                errorMessage, anyOf(
                    containsString("regex"),
                    containsString("pattern"),
                    containsString("invalid")
                ));

            System.out.println("\n✓ Security Test: Invalid Regex Exception Passed:");
            System.out.println("  Invalid regex failed fast before resource creation");
            System.out.println("  Error type: " + capturedError[0].getClass().getSimpleName());
            System.out.println("  Error message: " + capturedError[0].getMessage());

        } finally {
            Files.deleteIfExists(tempDir);
        }
    }
}
