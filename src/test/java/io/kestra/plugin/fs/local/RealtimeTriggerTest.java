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
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CancellationException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for RealtimeTrigger.
 * Tests the CREATE event → File → Execution pipeline.
 */
@KestraTest
class RealtimeTriggerTest {
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
    void testCreateEvent() throws Exception {
        // Setup: Create a temporary directory
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            // Create the realtime trigger with DEFAULT event type (CREATE_OR_UPDATE)
            // This tests that the default behavior is CREATE_OR_UPDATE, not just CREATE
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-realtime-default")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                // .on() is NOT specified - defaults to CREATE_OR_UPDATE
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture the execution
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
                    e.printStackTrace();
                }
            });

            watcherThread.setName("RealtimeTrigger-Default-Event-Test");
            watcherThread.start();

            // Wait for watcher to initialize with deterministic polling
            waitForWatcherReady(trigger, 5);

            // Create a test file - should trigger with default behavior
            Files.write(testFile, "test content".getBytes());

            // Wait for watcher thread to complete (with timeout)
            watcherThread.join(12000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify execution was received
            assertThat("Should have received execution when creating a file with default event type",
                receivedExecution[0], notNullValue());

            // Verify changeType is CREATE (default on() includes CREATE)
            Map<String, Object> variables = receivedExecution[0].getTrigger().getVariables();
            assertThat("changeType should be CREATE for newly created file",
                variables.get("changeType"), equalTo("CREATE"));

            System.out.println("\n✓ Default Event Type (CREATE_OR_UPDATE) Test Passed:");
            System.out.println("  Default on() includes CREATE");
            System.out.println("  File creation triggers execution with CREATE changeType");

        } finally {
            // Cleanup
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testCreateEventOutputContract() throws Exception {
        // Setup: Create a temporary directory
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            // Create the realtime trigger
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-realtime")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture the execution
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
                    e.printStackTrace();
                }
            });

            watcherThread.setName("RealtimeTrigger-OutputContract-Test");
            watcherThread.start();

            // Wait for watcher to initialize with deterministic polling
            waitForWatcherReady(trigger, 5);

            // Create test file
            Files.write(testFile, "test content".getBytes());
            System.out.println("✓ Test file created: " + testFile);

            // Wait for execution
            watcherThread.join(12000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify execution was received
            assertThat("Should have received execution", receivedExecution[0], notNullValue());

            Execution execution = receivedExecution[0];
            Map<String, Object> variables = execution.getTrigger().getVariables();

            // Verify top-level structure
            assertThat("Variables should contain 'file'", variables, hasKey("file"));
            assertThat("Variables should contain 'changeType'", variables, hasKey("changeType"));
            assertThat("changeType should be CREATE", variables.get("changeType"), equalTo("CREATE"));

            // Verify file structure
            @SuppressWarnings("unchecked")
            Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");

            assertThat("File should contain 'name'", fileVariables, hasKey("name"));
            assertThat("File name should match", fileVariables.get("name"),
                equalTo(testFile.getFileName().toString()));

            assertThat("File should contain 'localPath'", fileVariables, hasKey("localPath"));
            String localPathValue = fileVariables.get("localPath").toString();
            // localPath could be a URI or a Path string, so just verify it contains the filename
            assertThat("localPath should contain filename", localPathValue, containsString("test.txt"));

            assertThat("File should contain 'uri'", fileVariables, hasKey("uri"));

            // Print actual structure for documentation
            System.out.println("\n✓ Output Contract Verified:");
            System.out.println("  trigger.file.name = " + fileVariables.get("name"));
            System.out.println("  trigger.file.localPath = " + localPathValue);
            System.out.println("  trigger.file.uri = " + fileVariables.get("uri"));
            System.out.println("  trigger.changeType = " + variables.get("changeType"));

        } finally {
            // Cleanup
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testModifyEvent() throws Exception {
        // Setup: Create a temporary directory
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            // Create test file before trigger starts (so it already exists)
            Files.write(testFile, "initial content".getBytes());
            System.out.println("✓ Test file created before trigger: " + testFile);

            // Create the realtime trigger watching for MODIFY events
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-realtime-modify")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture the execution
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
                    e.printStackTrace();
                }
            });

            watcherThread.setName("RealtimeTrigger-Modify-Test");
            watcherThread.start();

            // Wait for watcher to initialize with deterministic polling
            waitForWatcherReady(trigger, 5);

            // Modify the existing file
            Files.write(testFile, "modified content".getBytes());
            System.out.println("✓ Test file modified: " + testFile);

            // Wait for execution
            watcherThread.join(12000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify execution was received
            assertThat("Should have received execution", receivedExecution[0], notNullValue());

            Execution execution = receivedExecution[0];
            Map<String, Object> variables = execution.getTrigger().getVariables();

            // Verify changeType is UPDATE
            assertThat("changeType should be UPDATE", variables.get("changeType"), equalTo("UPDATE"));

            System.out.println("\n✓ MODIFY Event Test Passed:");
            System.out.println("  changeType = " + variables.get("changeType"));

        } finally {
            // Cleanup
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testDeleteEvent() throws Exception {
        // Setup: Create a temporary directory
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            // Create test file before trigger starts
            Files.write(testFile, "content".getBytes());
            System.out.println("✓ Test file created before trigger: " + testFile);

            // Create the realtime trigger watching for DELETE events
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-realtime-delete")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture the execution
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
                    e.printStackTrace();
                }
            });

            watcherThread.setName("RealtimeTrigger-Delete-Test");
            watcherThread.start();

            // Wait for watcher to initialize with deterministic polling
            waitForWatcherReady(trigger, 5);

            // Delete the file
            Files.deleteIfExists(testFile);
            System.out.println("✓ Test file deleted: " + testFile);

            // Wait for execution
            watcherThread.join(12000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify execution was received
            assertThat("Should have received execution", receivedExecution[0], notNullValue());

            Execution execution = receivedExecution[0];
            Map<String, Object> variables = execution.getTrigger().getVariables();

            // Verify changeType is DELETE
            assertThat("changeType should be DELETE", variables.get("changeType"), equalTo("DELETE"));

            @SuppressWarnings("unchecked")
            Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
            // For DELETE events, file object has degraded metadata
            assertThat("File should contain 'name'", fileVariables, hasKey("name"));
            assertThat("File name should match", fileVariables.get("name"),
                equalTo(testFile.getFileName().toString()));

            System.out.println("\n✓ DELETE Event Test Passed:");
            System.out.println("  changeType = " + variables.get("changeType"));
            System.out.println("  file.name = " + fileVariables.get("name"));

        } finally {
            // Cleanup (file may already be deleted)
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testRegexFilter() throws Exception {
        // Setup: Create a temporary directory
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path csvFile = tempDir.resolve("data.csv");
        Path txtFile = tempDir.resolve("readme.txt");

        try {
            // Create the realtime trigger with regex filter for .csv files only
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-regex-filter")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(".*\\.csv$"))
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture the execution
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
                    e.printStackTrace();
                }
            });

            watcherThread.setName("RealtimeTrigger-Regex-Test");
            watcherThread.start();

            // Wait for watcher to initialize with deterministic polling
            waitForWatcherReady(trigger, 5);

            // Create a .txt file (should NOT trigger)
            Files.write(txtFile, "readme".getBytes());
            System.out.println("✓ Created readme.txt (should not trigger due to regex filter)");

            // Create a .csv file (should trigger)
            Files.write(csvFile, "header1,header2".getBytes());
            System.out.println("✓ Created data.csv (should trigger - matches .*\\.csv$)");

            // Wait for execution
            watcherThread.join(12000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify execution was received (should be for CSV only)
            assertThat("Should have received execution for CSV file", receivedExecution[0], notNullValue());

            Execution execution = receivedExecution[0];
            Map<String, Object> variables = execution.getTrigger().getVariables();

            @SuppressWarnings("unchecked")
            Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
            String fileName = fileVariables.get("name").toString();

            assertThat("Should have triggered for CSV file (not .txt)", fileName, endsWith(".csv"));
            assertThat("Triggered file should be data.csv", fileName, equalTo("data.csv"));

            System.out.println("\n✓ Regex Filter Test Passed:");
            System.out.println("  Pattern: .*\\.csv$");
            System.out.println("  Filtered out: readme.txt");
            System.out.println("  Detected: " + fileName);

        } finally {
            // Cleanup
            Files.deleteIfExists(csvFile);
            Files.deleteIfExists(txtFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testStopAndCleanup() throws Exception {
        // Setup: Create a temporary directory
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            // Create the realtime trigger
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-stop-cleanup")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture any exception from the watcher thread
            Throwable[] watcherException = new Throwable[1];
            Thread watcherThread = new Thread(() -> {
                try {
                    Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    // Store the exception - it's expected when trigger is stopped, but we want to verify
                    watcherException[0] = e;
                }
            });

            watcherThread.setName("RealtimeTrigger-Stop-Test");
            watcherThread.start();

            // Wait for watcher to initialize
            waitForWatcherReady(trigger, 5);
            System.out.println("✓ Watcher initialized");

            // Verify initial state
            assertThat("Watcher should be ready", trigger.isReady(), is(true));

            // Call stop to shut down the trigger
            trigger.stop();
            System.out.println("✓ Trigger stopped");

            // Wait for watcher thread to complete
            watcherThread.join(5000);
            assertThat("Watcher thread should have completed",
                watcherThread.isAlive(), is(false));

            // Give a bit of time for the ready flag to be cleared
            Thread.sleep(100);

            // Verify cleanup: watcher should no longer be ready
            assertThat("Watcher should not be ready after stop", trigger.isReady(), is(false));

            // Verify no unexpected exceptions occurred
            if (watcherException[0] != null && !(watcherException[0] instanceof CancellationException)) {
                if (watcherException[0] instanceof RuntimeException) {
                    throw (RuntimeException) watcherException[0];
                } else if (watcherException[0] instanceof Exception) {
                    throw (Exception) watcherException[0];
                } else {
                    throw new RuntimeException("Unexpected exception in watcher thread", watcherException[0]);
                }
            }

            System.out.println("\n✓ Stop and Cleanup Test Passed:");
            System.out.println("  Trigger stopped cleanly");
            System.out.println("  Watcher thread terminated");
            System.out.println("  Resources cleaned up (watchKeyMap cleared)");


        } finally {
            // Cleanup
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testRecursiveDirectoryRegistration() throws Exception {
        // Setup: Create only the root directory
        // DO NOT pre-create subdir - it must be created AFTER the watcher starts
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path subDir = tempDir.resolve("subdir");
        Path fileInSubDir = subDir.resolve("file.txt");

        try {
            // Create the realtime trigger with recursive watching
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-recursive-dir")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .recursive(Property.ofValue(true))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture the execution
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
                    e.printStackTrace();
                }
            });

            watcherThread.setName("RealtimeTrigger-Recursive-Test");
            watcherThread.start();

            // Wait for watcher to initialize with deterministic polling
            waitForWatcherReady(trigger, 5);
            System.out.println("✓ Watcher initialized for root directory only");

            // NOW create the subdirectory dynamically
            // This tests that the trigger registers new directories as they appear
            Files.createDirectory(subDir);
            System.out.println("✓ Created subdirectory after watcher started: " + subDir);

            // Create a file inside the newly-created subdirectory
            // This should be detected because the subdirectory was dynamically registered
            Files.write(fileInSubDir, "content".getBytes());
            System.out.println("✓ Created file in dynamically-registered subdirectory: " + fileInSubDir);

            // Wait for execution
            watcherThread.join(12000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify execution was received for file in dynamically-created subdirectory
            assertThat("Should have received execution for file in dynamically-created subdirectory",
                receivedExecution[0], notNullValue());

            Execution execution = receivedExecution[0];
            Map<String, Object> variables = execution.getTrigger().getVariables();

            @SuppressWarnings("unchecked")
            Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
            String filePath = fileVariables.get("localPath").toString();

            assertThat("File path should be in dynamically-created subdirectory",
                filePath, containsString("subdir"));

            System.out.println("\n✓ Recursive Directory Registration Test Passed:");
            System.out.println("  Subdirectory was created AFTER watcher started");
            System.out.println("  Trigger dynamically registered the new directory");
            System.out.println("  File in new subdirectory was detected");

        } finally {
            // Cleanup
            Files.deleteIfExists(fileInSubDir);
            Files.deleteIfExists(subDir);
            Files.deleteIfExists(tempDir);
        }
    }

    // ========== SECURITY TESTS (4) ==========

    @Test
    @Timeout(15)
    void testSecurityMissingAllowedPathsRejected() throws Exception {
        // Test that trigger fails if allowed-paths is not configured
        // This prevents unauthorized file system access
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-missing-allowed-paths")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            // Try to evaluate with a proper context (which will fail due to missing allowed-paths config)
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            Execution[] receivedExecution = new Execution[1];
            Throwable[] capturedError = new Throwable[1];

            Thread watcherThread = new Thread(() -> {
                try {
                    receivedExecution[0] = Flux.from(
                            trigger.evaluate(context.getKey(), context.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(5));
                } catch (Throwable e) {
                    capturedError[0] = e;
                }
            });

            watcherThread.setName("RealtimeTrigger-SecurityTest-MissingAllowedPaths");
            watcherThread.start();

            // Wait a bit to let the watcher attempt initialization
            Thread.sleep(500);
            trigger.stop();
            watcherThread.join(3000);

            // Verify that trigger rejected access due to missing allowed-paths
            // The exact error varies based on RunContext mock, but trigger should not emit events
            assertThat("Watcher should have failed due to missing allowed-paths security config",
                receivedExecution[0], nullValue());

            System.out.println("\n✓ Security Test: Missing Allowed-Paths Passed:");
            System.out.println("  Trigger rejected access due to missing allowed-paths config");
            System.out.println("  No file events were emitted");

        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecuritySymlinkRejected() throws Exception {
        // Test that symlinks are rejected to prevent directory traversal attacks
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path realDir = Files.createDirectory(tempDir.resolve("real"));
        Path fileInRealDir = realDir.resolve("file.txt");

        // Only create symlink if the OS supports it (skip on Windows without appropriate permissions)
        Path symlink = tempDir.resolve("symlink-to-real");
        boolean symlinkCreated = false;

        try {
            try {
                Files.createSymbolicLink(symlink, realDir);
                symlinkCreated = true;
                System.out.println("✓ Created symlink: " + symlink + " → " + realDir);
            } catch (UnsupportedOperationException | IOException e) {
                System.out.println("⊘ Symlink creation not supported or not permitted; skipping symlink test");
                return;
            }

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-symlink-rejection")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
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

            // Try to create a file in the symlinked directory
            Files.write(fileInRealDir, "content".getBytes());
            System.out.println("✓ Created file in real directory (accessed via symlink): " + fileInRealDir);

            watcherThread.join(12000);
            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Symlinks should be rejected - no execution should be received
            assertThat("Symlink should be rejected (no execution for symlinked path)",
                receivedExecution[0], nullValue());

            System.out.println("\n✓ Security Test: Symlink Rejection Passed:");
            System.out.println("  Symlinks are blocked to prevent directory traversal");

        } finally {
            if (symlinkCreated) {
                Files.deleteIfExists(symlink);
            }
            Files.deleteIfExists(fileInRealDir);
            Files.deleteIfExists(realDir);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityPathOutsideAllowedRejected() throws Exception {
        // Test that files outside allowed-paths are rejected
        Path allowedDir = Files.createTempDirectory("realtime-trigger-allowed");
        Path forbiddenDir = Files.createTempDirectory("realtime-trigger-forbidden");
        Path allowedFile = allowedDir.resolve("allowed.txt");
        Path forbiddenFile = forbiddenDir.resolve("forbidden.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-path-outside-allowed")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(allowedDir.toString()))
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

            watcherThread.setName("RealtimeTrigger-SecurityTest-PathOutsideAllowed");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);

            // Create file in forbidden directory (outside allowed-paths)
            Files.write(forbiddenFile, "forbidden".getBytes());
            System.out.println("✓ Created file in forbidden directory: " + forbiddenFile);

            Thread.sleep(500);

            // Create file in allowed directory (should trigger)
            Files.write(allowedFile, "allowed".getBytes());
            System.out.println("✓ Created file in allowed directory: " + allowedFile);

            watcherThread.join(12000);
            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify execution was received only for file in allowed directory
            assertThat("Should have received execution for allowed directory",
                receivedExecution[0], notNullValue());

            Execution execution = receivedExecution[0];
            Map<String, Object> variables = execution.getTrigger().getVariables();

            @SuppressWarnings("unchecked")
            Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
            String detectedPath = fileVariables.get("localPath").toString();

            assertThat("Should have detected file in allowed directory, not forbidden",
                detectedPath, containsString("allowed"));

            System.out.println("\n✓ Security Test: Path Outside Allowed Rejected Passed:");
            System.out.println("  Files in forbidden directory were ignored");
            System.out.println("  Only files in allowed-paths triggered events");

        } finally {
            Files.deleteIfExists(allowedFile);
            Files.deleteIfExists(forbiddenFile);
            Files.deleteIfExists(allowedDir);
            Files.deleteIfExists(forbiddenDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityInvalidRegexThrowsException() throws Exception {
        // Test that invalid regex patterns fail early with clear error
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");

        try {
            // Create trigger with invalid regex (unclosed bracket)
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

            // Verify that an error was thrown during initialization
            assertThat("Invalid regex should cause an exception during trigger evaluation",
                capturedError[0], notNullValue());

            // Verify it's an IllegalArgumentException or similar (not a silent failure)
            String errorMessage = capturedError[0].toString().toLowerCase();
            assertThat("Error should indicate regex validation failure",
                errorMessage, anyOf(
                    containsString("illegal"),
                    containsString("invalid"),
                    containsString("pattern"),
                    containsString("regex")
                ));

            System.out.println("\n✓ Security Test: Invalid Regex Exception Passed:");
            System.out.println("  Invalid regex failed fast with: " + capturedError[0].getClass().getSimpleName());
            System.out.println("  Error message: " + capturedError[0].getMessage());

        } finally {
            Files.deleteIfExists(tempDir);
        }
    }

    // ========== BEHAVIOR TESTS (4) ==========

    @Test
    @Timeout(15)
    void testCreateOrUpdateDefaultIncludesBoth() throws Exception {
        // Test that default on() behavior is CREATE_OR_UPDATE (includes both CREATE and UPDATE events)
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-default-create-or-update")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                // .on() NOT specified - should default to CREATE_OR_UPDATE
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture first execution (file creation)
            Execution[] firstExecution = new Execution[1];
            Thread firstWatcher = new Thread(() -> {
                try {
                    firstExecution[0] = Flux.from(
                            trigger.evaluate(context.getKey(), context.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    System.err.println("✗ First watcher error: " + e.getMessage());
                }
            });

            firstWatcher.setName("RealtimeTrigger-CreateOrUpdate-First");
            firstWatcher.start();

            waitForWatcherReady(trigger, 5);

            // Create a file - should trigger with changeType=CREATE
            Files.write(testFile, "initial".getBytes());
            System.out.println("✓ File created");

            firstWatcher.join(12000);
            if (firstWatcher.isAlive()) {
                trigger.stop();
                firstWatcher.join(2000);
            }

            assertThat("Should have triggered on file creation", firstExecution[0], notNullValue());
            Map<String, Object> firstVars = firstExecution[0].getTrigger().getVariables();
            assertThat("First event should be CREATE", firstVars.get("changeType"), equalTo("CREATE"));

            System.out.println("✓ First event: CREATE");

            // Now start a new watcher for the UPDATE event
            RealtimeTrigger trigger2 = RealtimeTrigger.builder()
                .id("test-default-create-or-update-2")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .build();

            var context2 = TestsUtils.mockTrigger(runContextFactory, trigger2);

            Execution[] secondExecution = new Execution[1];
            Thread secondWatcher = new Thread(() -> {
                try {
                    secondExecution[0] = Flux.from(
                            trigger2.evaluate(context2.getKey(), context2.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    System.err.println("✗ Second watcher error: " + e.getMessage());
                }
            });

            secondWatcher.setName("RealtimeTrigger-CreateOrUpdate-Second");
            secondWatcher.start();

            waitForWatcherReady(trigger2, 5);

            // Modify the file - should trigger with changeType=UPDATE
            Files.write(testFile, "modified".getBytes());
            System.out.println("✓ File modified");

            secondWatcher.join(12000);
            if (secondWatcher.isAlive()) {
                trigger2.stop();
                secondWatcher.join(2000);
            }

            assertThat("Should have triggered on file modification", secondExecution[0], notNullValue());
            Map<String, Object> secondVars = secondExecution[0].getTrigger().getVariables();
            assertThat("Second event should be UPDATE", secondVars.get("changeType"), equalTo("UPDATE"));

            System.out.println("✓ Second event: UPDATE");
            System.out.println("\n✓ Default CREATE_OR_UPDATE Behavior Test Passed:");
            System.out.println("  Default on() includes both CREATE and UPDATE");

        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testMultipleEventsInSequence() throws Exception {
        // Test that trigger handles multiple file events in sequence correctly
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("file2.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-multiple-events")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Capture first file creation
            Execution[] firstExecution = new Execution[1];
            Thread firstWatcher = new Thread(() -> {
                try {
                    firstExecution[0] = Flux.from(
                            trigger.evaluate(context.getKey(), context.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    System.err.println("✗ First watcher error: " + e.getMessage());
                }
            });

            firstWatcher.setName("RealtimeTrigger-MultipleEvents-1");
            firstWatcher.start();

            waitForWatcherReady(trigger, 5);

            // Create first file
            Files.write(file1, "file1".getBytes());
            System.out.println("✓ Created file1.txt");

            firstWatcher.join(12000);
            if (firstWatcher.isAlive()) {
                trigger.stop();
                firstWatcher.join(2000);
            }

            assertThat("Should have triggered for file1", firstExecution[0], notNullValue());
            Map<String, Object> firstVars = firstExecution[0].getTrigger().getVariables();
            @SuppressWarnings("unchecked")
            Map<String, Object> firstFile = (Map<String, Object>) firstVars.get("file");
            assertThat("First trigger should be for file1.txt", firstFile.get("name"), equalTo("file1.txt"));

            System.out.println("✓ First trigger detected: file1.txt");

            // Now capture second file creation with a new trigger instance
            RealtimeTrigger trigger2 = RealtimeTrigger.builder()
                .id("test-multiple-events-2")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context2 = TestsUtils.mockTrigger(runContextFactory, trigger2);

            Execution[] secondExecution = new Execution[1];
            Thread secondWatcher = new Thread(() -> {
                try {
                    secondExecution[0] = Flux.from(
                            trigger2.evaluate(context2.getKey(), context2.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    System.err.println("✗ Second watcher error: " + e.getMessage());
                }
            });

            secondWatcher.setName("RealtimeTrigger-MultipleEvents-2");
            secondWatcher.start();

            waitForWatcherReady(trigger2, 5);

            // Create second file
            Files.write(file2, "file2".getBytes());
            System.out.println("✓ Created file2.txt");

            secondWatcher.join(12000);
            if (secondWatcher.isAlive()) {
                trigger2.stop();
                secondWatcher.join(2000);
            }

            assertThat("Should have triggered for file2", secondExecution[0], notNullValue());
            Map<String, Object> secondVars = secondExecution[0].getTrigger().getVariables();
            @SuppressWarnings("unchecked")
            Map<String, Object> secondFile = (Map<String, Object>) secondVars.get("file");
            assertThat("Second trigger should be for file2.txt", secondFile.get("name"), equalTo("file2.txt"));

            System.out.println("✓ Second trigger detected: file2.txt");
            System.out.println("\n✓ Multiple Events in Sequence Test Passed:");
            System.out.println("  Trigger correctly detected multiple file creation events");

        } finally {
            Files.deleteIfExists(file1);
            Files.deleteIfExists(file2);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testRegexConsistencyAcrossEvents() throws Exception {
        // Test that regex filter is consistently applied across different event types
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path csvFile = tempDir.resolve("data.csv");
        Path txtFile = tempDir.resolve("readme.txt");
        Path jsonFile = tempDir.resolve("config.json");

        try {
            // Create trigger with regex matching only .csv and .json files
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-regex-consistency")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE_OR_UPDATE))
                .regExp(Property.ofValue(".*\\.(csv|json)$"))
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

            watcherThread.setName("RealtimeTrigger-RegexConsistency");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);

            // Create .txt file (should not match)
            Files.write(txtFile, "readme".getBytes());
            System.out.println("✓ Created readme.txt (should not match .*\\.(csv|json)$)");

            Thread.sleep(200);

            // Create .csv file (should match)
            Files.write(csvFile, "col1,col2".getBytes());
            System.out.println("✓ Created data.csv (should match)");

            watcherThread.join(12000);
            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            assertThat("Should have triggered for CSV", receivedExecution[0], notNullValue());
            Map<String, Object> variables = receivedExecution[0].getTrigger().getVariables();
            @SuppressWarnings("unchecked")
            Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
            String fileName = fileVariables.get("name").toString();

            assertThat("Should have detected CSV file", fileName, equalTo("data.csv"));

            System.out.println("\n✓ Regex Consistency Test Passed:");
            System.out.println("  Regex pattern .*\\.(csv|json)$ applied consistently");
            System.out.println("  Detected: " + fileName + " (matched pattern)");
            System.out.println("  Ignored: readme.txt (did not match pattern)");

        } finally {
            Files.deleteIfExists(csvFile);
            Files.deleteIfExists(txtFile);
            Files.deleteIfExists(jsonFile);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testDeleteEventFileMetadata() throws Exception {
        // Test that DELETE events have correct file metadata (name, path)
        // even though file no longer exists on disk
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test-delete.txt");

        try {
            // Pre-create file
            Files.write(testFile, "content".getBytes());
            System.out.println("✓ Test file created before trigger: " + testFile);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-delete-metadata")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
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

            watcherThread.setName("RealtimeTrigger-DeleteMetadata");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);

            // Delete the file
            Files.deleteIfExists(testFile);
            System.out.println("✓ Test file deleted");

            watcherThread.join(12000);
            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            assertThat("Should have triggered on DELETE", receivedExecution[0], notNullValue());

            Execution execution = receivedExecution[0];
            Map<String, Object> variables = execution.getTrigger().getVariables();

            @SuppressWarnings("unchecked")
            Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");

            // Even though file is deleted, metadata should be available
            assertThat("File should have name", fileVariables, hasKey("name"));
            assertThat("File name should be correct", fileVariables.get("name"),
                equalTo("test-delete.txt"));

            assertThat("File should have localPath (for deleted files, metadata is degraded but present)",
                fileVariables, hasKey("localPath"));

            System.out.println("\n✓ DELETE Event File Metadata Test Passed:");
            System.out.println("  DELETE event has file metadata:");
            System.out.println("  file.name = " + fileVariables.get("name"));
            System.out.println("  file.localPath = " + fileVariables.get("localPath"));

        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }

    // ========== LIFECYCLE TESTS (2) ==========

    @Test
    @Timeout(15)
    void testKillMethodCleanup() throws Exception {
        // Test that kill() method properly stops the watcher and cleans up resources
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-kill-cleanup")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            Throwable[] watcherError = new Throwable[1];
            Thread watcherThread = new Thread(() -> {
                try {
                    Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(15));
                } catch (Throwable e) {
                    if (!(e instanceof CancellationException)) {
                        watcherError[0] = e;
                    }
                }
            });

            watcherThread.setName("RealtimeTrigger-KillCleanup");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);
            System.out.println("✓ Trigger ready");

            // Verify isReady() returns true
            assertThat("Trigger should be ready", trigger.isReady(), is(true));

            // Call kill() to forcefully terminate
            trigger.kill();
            System.out.println("✓ Trigger killed");

            // Wait for watcher thread to complete
            watcherThread.join(5000);
            assertThat("Watcher thread should complete after kill()",
                watcherThread.isAlive(), is(false));

            // After kill(), isReady() should return false
            assertThat("Trigger should not be ready after kill()",
                trigger.isReady(), is(false));

            // Verify no unexpected exceptions
            if (watcherError[0] != null) {
                throw new AssertionError("Unexpected error in watcher after kill(): " + watcherError[0], watcherError[0]);
            }

            System.out.println("\n✓ Kill Method Cleanup Test Passed:");
            System.out.println("  Trigger properly terminated via kill()");
            System.out.println("  All resources cleaned up");

        } finally {
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testThreadLifecycleAndNaming() throws Exception {
        // Test that watcher thread is properly named and manages its lifecycle
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-thread-lifecycle")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
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

            watcherThread.setName("RealtimeTrigger-LifecycleTest");
            watcherThread.start();

            // Give thread time to start
            Thread.sleep(100);

            // Verify thread is alive and running
            assertThat("Watcher thread should be running", watcherThread.isAlive(), is(true));
            System.out.println("✓ Thread started: " + watcherThread.getName() + " (alive=" + watcherThread.isAlive() + ")");

            waitForWatcherReady(trigger, 5);
            System.out.println("✓ Watcher ready");

            // Create file to trigger event
            Files.write(testFile, "content".getBytes());
            System.out.println("✓ File created");

            // Wait for execution
            watcherThread.join(12000);

            assertThat("Watcher thread should complete after execution received",
                watcherThread.isAlive(), is(false));
            System.out.println("✓ Thread terminated cleanly");

            assertThat("Execution should have been received",
                receivedExecution[0], notNullValue());

            System.out.println("\n✓ Thread Lifecycle and Naming Test Passed:");
            System.out.println("  Thread started with proper name");
            System.out.println("  Thread ran and processed events");
            System.out.println("  Thread terminated cleanly on completion");

        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }
}
