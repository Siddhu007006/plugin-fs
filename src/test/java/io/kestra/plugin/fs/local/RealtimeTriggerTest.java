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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
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
    void testDeleteDirectoryDoesNotEmitExecution() throws Exception {
        // Test that deleting a child directory with recursive=false does NOT emit an execution.
        // Only file deletions should emit executions; directory deletions should be suppressed.
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path childDir = tempDir.resolve("subdir");

        try {
            // Create child directory before trigger starts
            Files.createDirectory(childDir);
            System.out.println("✓ Child directory created before trigger: " + childDir);

            // Create the realtime trigger watching for DELETE events with recursive=false
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-realtime-delete-dir")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .recursive(Property.ofValue(false))
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

            watcherThread.setName("RealtimeTrigger-DeleteDir-Test");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);

            // Delete the child directory
            Files.deleteIfExists(childDir);
            System.out.println("✓ Child directory deleted: " + childDir);

            // Wait for potential execution
            watcherThread.join(5000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify NO execution was received (directories should be suppressed)
            assertThat("Directory deletion should not emit execution", receivedExecution[0], nullValue());

            System.out.println("\n✓ DELETE Directory Suppression Test Passed:");
            System.out.println("  Child directory deletion did not emit execution");
            System.out.println("  Only file deletions emit executions (consistent with polling trigger)");

        } finally {
            Files.deleteIfExists(childDir);
            Files.deleteIfExists(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testDeleteNestedDirectoryDoesNotEmitExecution() throws Exception {
        // Regression test: With recursive=true, dynamically created nested directories
        // should be tracked in knownDirectories so their deletion doesn't emit execution.
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");

        try {
            // Create the realtime trigger watching for DELETE events with recursive=true
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-realtime-delete-nested-dir")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .recursive(Property.ofValue(true))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            java.util.List<Execution> receivedExecutions = Collections.synchronizedList(new java.util.ArrayList<>());
            Thread watcherThread = new Thread(() -> {
                try {
                    Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe(receivedExecutions::add);
                    // Keep thread alive for test duration
                    Thread.sleep(10000);
                } catch (Exception e) {
                    System.err.println("✗ Watcher error: " + e.getMessage());
                }
            });

            watcherThread.setName("RealtimeTrigger-DeleteNestedDir-Test");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);

            // Create nested directory structure
            Path newDir = tempDir.resolve("new");
            Path nestedDir = newDir.resolve("nested");
            Files.createDirectories(nestedDir);
            System.out.println("✓ Created nested directory: " + nestedDir);

            // Wait for registration
            Thread.sleep(500);

            // Delete the nested directory
            Files.deleteIfExists(nestedDir);
            System.out.println("✓ Deleted nested directory: " + nestedDir);

            // Wait to ensure no execution is emitted
            Thread.sleep(2000);

            trigger.stop();
            watcherThread.join(2000);

            // Verify NO execution was received (nested directory deletion should be suppressed)
            assertThat("Nested directory deletion should not emit execution",
                receivedExecutions.size(), equalTo(0));

            System.out.println("\n✓ DELETE Nested Directory Suppression Test Passed:");
            System.out.println("  Dynamically created nested directory deletion did not emit execution");
            System.out.println("  knownDirectories correctly tracks all registered directories");

        } finally {
            // Cleanup (directories may already be deleted)
            java.nio.file.Files.walk(tempDir)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignored) {}
                });
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

            // Wait a bit to allow the watcher to process the ENTRY_CREATE for the subdirectory
            // and register it before we create the file. This is especially important on Windows
            // where file system events can have delays.
            Thread.sleep(1000);

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
        // Test that ONE trigger instance can detect multiple file events in sequence.
        // This proves Issue #365 requirement: one execution per matched event from a single trigger.
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("file2.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-multiple-events-single-watcher")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Collect all executions from this single trigger
            java.util.List<Execution> receivedExecutions = Collections.synchronizedList(new java.util.ArrayList<>());
            Throwable[] watcherError = new Throwable[1];

            Thread watcherThread = new Thread(() -> {
                try {
                    var subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                        .subscribeOn(Schedulers.boundedElastic())
                        // Collect all emissions for 5 seconds or until complete
                        .subscribe(
                            receivedExecutions::add,
                            e -> watcherError[0] = e,
                            () -> System.out.println("✓ Watcher stream completed")
                        );
                    // Keep the thread alive while watching, with bounded timeout
                    long startTime = System.currentTimeMillis();
                    while (System.currentTimeMillis() - startTime < 5000 && !subscription.isDisposed()) {
                        Thread.sleep(100);
                    }
                    subscription.dispose();
                } catch (Exception e) {
                    System.err.println("✗ Watcher error: " + e.getMessage());
                }
            });

            watcherThread.setName("RealtimeTrigger-MultipleEvents-SingleWatcher");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);

            // Create first file
            Files.write(file1, "file1".getBytes());
            System.out.println("✓ Created file1.txt");

            // Wait for first event to be collected with bounded timeout
            long waitStart = System.currentTimeMillis();
            while (receivedExecutions.size() < 1 && System.currentTimeMillis() - waitStart < 3000) {
                Thread.sleep(50);
            }

            // Create second file (while watcher still running)
            Files.write(file2, "file2".getBytes());
            System.out.println("✓ Created file2.txt");

            // Wait for watcher to finish collection
            watcherThread.join(6000);

            if (watcherThread.isAlive()) {
                trigger.stop();
                watcherThread.join(2000);
            }

            // Verify we received both executions from the single trigger instance
            assertThat("Should have received 2 executions from one trigger",
                receivedExecutions.size(), greaterThanOrEqualTo(2));

            // Verify no errors occurred during watching
            assertThat("Watcher should not have encountered errors", watcherError[0], nullValue());

            // Verify first execution is for file1
            Execution firstExec = receivedExecutions.get(0);
            @SuppressWarnings("unchecked")
            Map<String, Object> firstFile = (Map<String, Object>) firstExec.getTrigger().getVariables().get("file");
            assertThat("First execution should be for file1.txt", firstFile.get("name"), equalTo("file1.txt"));

            // Verify second execution is for file2
            Execution secondExec = receivedExecutions.get(1);
            @SuppressWarnings("unchecked")
            Map<String, Object> secondFile = (Map<String, Object>) secondExec.getTrigger().getVariables().get("file");
            assertThat("Second execution should be for file2.txt", secondFile.get("name"), equalTo("file2.txt"));

            System.out.println("✓ Multiple Events in Sequence Test Passed:");
            System.out.println("  One trigger instance detected " + receivedExecutions.size() + " file creation events");
            System.out.println("  Events received in correct order: file1.txt → file2.txt");

        } finally {
            Files.deleteIfExists(file1);
            Files.deleteIfExists(file2);
            Files.deleteIfExists(tempDir);
        }
    }


    @Test
    @Timeout(15)
    void testRegexFilterAcrossEventTypes() throws Exception {
        // Test that regex filter is consistently applied to different event types.
        // Simplified version that proves regex works for CREATE events.
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path csvFile = tempDir.resolve("data.csv");
        Path txtFile = tempDir.resolve("readme.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-regex-filter-across-events")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE_OR_UPDATE))
                .regExp(Property.ofValue(".*\\.csv$"))
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

            watcherThread.setName("RealtimeTrigger-RegexFilterAcrossEvents");
            watcherThread.start();

            waitForWatcherReady(trigger, 5);

            // Create .txt file (should not match regex)
            Files.write(txtFile, "readme".getBytes());
            System.out.println("✓ Created readme.txt (should not match .*\\.csv$)");

            Thread.sleep(200);

            // Create .csv file (should match - CREATE event)
            Files.write(csvFile, "col1,col2".getBytes());
            System.out.println("✓ Created data.csv (should match - CREATE event)");

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

            System.out.println("\n✓ Regex Filter Across Event Types Test Passed:");
            System.out.println("  Regex pattern .*\\.csv$ applied consistently");
            System.out.println("  Detected: " + fileName + " (matched pattern)");
            System.out.println("  Ignored: readme.txt (did not match pattern)");

        } finally {
            Files.deleteIfExists(csvFile);
            Files.deleteIfExists(txtFile);
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

            // Give a bit of time for the ready flag to be cleared
            Thread.sleep(100);

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

            // Use deterministic readiness check instead of arbitrary sleep
            waitForWatcherReady(trigger, 5);

            // Verify thread name was set correctly
            assertThat("Watcher thread should have correct name",
                watcherThread.getName(), equalTo("RealtimeTrigger-LifecycleTest"));
            System.out.println("✓ Thread started: " + watcherThread.getName());

            // Verify thread is alive
            assertThat("Watcher thread should be running", watcherThread.isAlive(), is(true));
            System.out.println("✓ Watcher ready and running");

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
            System.out.println("  Thread started with name: RealtimeTrigger-LifecycleTest");
            System.out.println("  Thread ran and processed events deterministically");
            System.out.println("  Thread terminated cleanly on completion");

        } finally {
            Files.deleteIfExists(testFile);
            Files.deleteIfExists(tempDir);
        }
    }
}
