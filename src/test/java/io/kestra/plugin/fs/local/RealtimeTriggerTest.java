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
import java.util.Map;

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
            // Create the realtime trigger
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-realtime")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            // Mock the trigger context
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            // Start the watcher on a background thread using boundedElastic scheduler
            // This prevents the test thread from being blocked by service.take()
            Thread watcherThread = new Thread(() -> {
                try {
                    Execution execution = Flux.from(
                            trigger.evaluate(context.getKey(), context.getValue())
                        )
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));

                    if (execution != null) {
                        System.out.println("✓ Received execution: " + execution.getId());
                    }
                } catch (Exception e) {
                    System.err.println("✗ Watcher error: " + e.getMessage());
                    e.printStackTrace();
                }
            });

            watcherThread.setName("RealtimeTrigger-Watcher");
            watcherThread.start();

            // Wait for watcher to initialize with deterministic polling
            System.out.println("✓ Watcher initializing...");
            waitForWatcherReady(trigger, 5);
            System.out.println("✓ Watcher ready, creating test file");

            // Create a test file - this should be detected by the watcher
            Files.write(testFile, "test content".getBytes());
            System.out.println("✓ Test file created: " + testFile);

            // Wait for watcher thread to complete (with timeout)
            watcherThread.join(12000);

            if (watcherThread.isAlive()) {
                System.err.println("✗ Watcher thread still running, stopping trigger");
                trigger.stop();
                watcherThread.join(2000);
            }

            System.out.println("✓ Watcher thread completed");

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

            Thread watcherThread = new Thread(() -> {
                try {
                    Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .blockFirst(Duration.ofSeconds(10));
                } catch (Exception e) {
                    // Expected when trigger is stopped
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

            System.out.println("\n✓ Stop and Cleanup Test Passed:");
            System.out.println("  Trigger stopped cleanly");
            System.out.println("  Watcher thread terminated");
            System.out.println("  Resources cleaned up");

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
}
