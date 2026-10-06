package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.local.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.nio.file.*;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on local filesystem events (realtime)",
    description = """
        Watches a local directory for filesystem events (create, modify, delete) using Java NIO WatchService.
        Fires immediately when files matching the filter are detected, without polling.
        
        Note: On macOS, WatchService uses a polling-based fallback and may not detect events as quickly as on Linux or Windows.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "React immediately when a file is created or updated",
            full = true,
            code = """
                id: local_realtime_trigger
                namespace: company.team

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.local.RealtimeTrigger
                    from: /data/incoming
                    on: CREATE_OR_UPDATE

                tasks:
                  - id: log_file
                    type: io.kestra.plugin.core.log.Log
                    message: "New file: {{ trigger.file.localPath }}"
                """
        ),
        @Example(
            title = "Watch recursively and filter by file extension",
            full = true,
            code = """
                id: local_realtime_recursive
                namespace: company.team

                triggers:
                  - id: watch_csv
                    type: io.kestra.plugin.fs.local.RealtimeTrigger
                    from: /data/incoming
                    recursive: true
                    regExp: ".*\\\\.csv$"

                tasks:
                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "CSV ready: {{ trigger.file.localPath }}"
                """
        ),
        @Example(
            title = "React to file deletions for cleanup tracking",
            full = true,
            code = """
                id: local_realtime_delete
                namespace: company.team

                triggers:
                  - id: watch_deletes
                    type: io.kestra.plugin.fs.local.RealtimeTrigger
                    from: /data/incoming
                    on: DELETE

                tasks:
                  - id: log_removed
                    type: io.kestra.plugin.core.log.Log
                    message: "File removed: {{ trigger.file.localPath }}"
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger
    implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output> {

    /**
     * Event types that trigger execution.
     * Matches WatchService StandardWatchEventKinds semantics:
     * CREATE  → ENTRY_CREATE
     * UPDATE  → ENTRY_MODIFY
     * DELETE  → ENTRY_DELETE
     */
    public enum EventType {
        CREATE,
        UPDATE,
        CREATE_OR_UPDATE,
        DELETE
    }

    @Schema(title = "Directory to watch")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(title = "Include files in subdirectories")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(title = "Regex pattern to match file names")
    @PluginProperty(group = "advanced")
    private Property<String> regExp;

    @Schema(title = "Event types to trigger on")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<EventType> on = Property.ofValue(EventType.CREATE_OR_UPDATE);

    // Runtime state fields
    private transient final AtomicBoolean active = new AtomicBoolean(false);
    private transient final AtomicReference<WatchService> watchService = new AtomicReference<>();
    private transient Logger logger;
    private transient Map<WatchKey, Path> watchKeyMap;

    /**
     * Package-private readiness check for testing.
     * Returns true when the trigger has initialized and is ready to watch for events.
     */
    boolean isReady() {
        return watchKeyMap != null && !watchKeyMap.isEmpty();
    }

    /**
     * Retrieves allowed paths from plugin configuration.
     * Matches AbstractLocalTask.allowedPaths() for consistent security policy.
     * Throws SecurityException if not configured (required for all environments).
     */
    private List<String> getAllowedPaths(RunContext runContext) {
        Optional<List<String>> allowedPathConfig = runContext.pluginConfiguration("allowed-paths");

        if (allowedPathConfig.isEmpty() || allowedPathConfig.get().isEmpty()) {
            throw new SecurityException(
                "The 'allowed-paths' configuration is required to enable access to the local filesystem. " +
                "You must define at least one allowed path in the plugin configuration, `kestra.plugins.configurations`. " +
                "Example: kestra.plugins.configurations[io.kestra.plugin.fs.local.RealtimeTrigger].allowed-paths: [\"/data/incoming\"] " +
                "Refer to: https://kestra.io/docs/configuration#set-default-values"
            );
        }

        return allowedPathConfig.get();
    }

    /**
     * Validates that a path is within the configured allowed-paths.
     * Matches AbstractLocalTask.validatePath() for consistent security policy.
     * Throws SecurityException if the path is not allowed or if allowed-paths is not configured.
     */
    private void validatePath(Path path, RunContext runContext) throws IOException {
        List<String> renderedAllowedPaths = getAllowedPaths(runContext);

        Path realPath;
        try {
            realPath = path.toRealPath();
        } catch (IOException e) {
            realPath = path.toAbsolutePath().normalize();
        }

        List<Path> normalizedAllowedPaths = renderedAllowedPaths.stream()
            .map(allowed -> {
                try {
                    return Paths.get(allowed).toRealPath();
                } catch (IOException e) {
                    return Paths.get(allowed).toAbsolutePath().normalize();
                }
            })
            .toList();

        boolean isAllowed = normalizedAllowedPaths.stream()
            .anyMatch(realPath::startsWith);

        if (!isAllowed) {
            String formattedAllowedPaths = normalizedAllowedPaths.stream()
                .map(Path::toString)
                .collect(Collectors.joining("', '", "'", "'"));
            throw new SecurityException(
                "Access to path '" + realPath + "' is denied. " +
                "The specified path must be within one of the configured 'allowed-paths': " + formattedAllowedPaths + ". " +
                "Refer to: https://kestra.io/docs/configuration#set-default-values"
            );
        }
    }

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        this.logger = runContext.logger();

        logger.info("RealtimeTrigger starting for directory: {}", this.from);

        return Flux.<Execution>create(emitter -> {
            try {
                active.set(true);
                watchKeyMap = new ConcurrentHashMap<>();

                // Render and validate the `from` property
                String renderedFrom = runContext.render(this.from).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("`from` is required"));
                Path rootDirectory = Paths.get(renderedFrom).toAbsolutePath().normalize();

                // Validate against allowed-paths configuration
                validatePath(rootDirectory, runContext);

                if (!Files.exists(rootDirectory)) {
                    throw new IllegalArgumentException("Directory does not exist: " + rootDirectory);
                }
                if (!Files.isDirectory(rootDirectory)) {
                    throw new IllegalArgumentException("Path is not a directory: " + rootDirectory);
                }

                logger.info("Watching directory: {}", rootDirectory);

                // Create WatchService
                createWatchService();

                // Register root directory
                registerDirectory(rootDirectory);

                // If recursive, walk and register all subdirectories
                boolean rRecursive = runContext.render(this.recursive).as(Boolean.class).orElse(false);
                if (rRecursive) {
                    registerDirectoryTree(rootDirectory);
                }

                logger.info("RealtimeTrigger initialized with {} watch keys. Waiting for filesystem events", watchKeyMap.size());

                // Render filter properties once before entering the event loop
                EventType renderedOn = runContext.render(this.on).as(EventType.class)
                    .orElse(EventType.CREATE_OR_UPDATE);
                String renderedRegExp = null;
                if (this.regExp != null) {
                    renderedRegExp = runContext.render(this.regExp).as(String.class).orElse(null);
                }

                // Capture the WatchService reference once at the beginning to avoid null-dereference race with stop()
                WatchService service = watchService.get();
                if (service == null) {
                    throw new IllegalStateException("WatchService not initialized");
                }

                while (active.get()) {
                    WatchKey key;
                    try {
                        key = service.take();
                    } catch (ClosedWatchServiceException e) {
                        // Normal shutdown: stop() closed the WatchService, so take() unblocked.
                        break;
                    } catch (InterruptedException e) {
                        // Thread interrupted, but check active flag before assuming shutdown.
                        if (!active.get()) {
                            break;
                        }
                        // If still active, it's an unexpected interrupt; continue watching.
                        continue;
                    }

                    // Get the directory associated with this WatchKey
                    Path directory = watchKeyMap.get(key);
                    if (directory == null) {
                        // Race condition: the key was removed or map was cleared during shutdown.
                        // Safely discard this key without trying to reset it.
                        continue;
                    }

                    // Process all events for this key
                    for (WatchEvent<?> event : key.pollEvents()) {
                        try {
                            WatchEvent.Kind<?> kind = event.kind();

                            if (kind == StandardWatchEventKinds.OVERFLOW) {
                                logger.warn("WatchService overflow event; some filesystem events may have been missed");
                                continue;
                            }

                            Path eventPath = directory.resolve((Path) event.context());

                            if (kind == StandardWatchEventKinds.ENTRY_CREATE && rRecursive && Files.isDirectory(eventPath)) {
                                try {
                                    registerDirectory(eventPath);
                                    registerDirectoryTree(eventPath);
                                } catch (IOException e) {
                                    // Could not register newly created directory (may have been deleted)
                                }
                                continue;
                            }

                            if (!shouldTrigger(kind, renderedOn)) {
                                continue;
                            }

                            if (renderedRegExp != null && !matchesRegExp(eventPath, renderedRegExp)) {
                                continue;
                            }

                            if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                try {
                                    if (Files.isDirectory(eventPath)) {
                                        continue;
                                    }

                                    BasicFileAttributes attrs = Files.readAttributes(eventPath, BasicFileAttributes.class);
                                    File file = File.from(eventPath, attrs);

                                    Output output = Output.builder()
                                        .file(file)
                                        .changeType("CREATE")
                                        .build();

                                    Execution execution = TriggerService.generateRealtimeExecution(
                                        RealtimeTrigger.this,
                                        conditionContext,
                                        context,
                                        output
                                    );

                                    emitter.next(execution);

                                } catch (NoSuchFileException e) {
                                    // Race: file was created but deleted before we could read it
                                } catch (Exception e) {
                                    logger.warn("Error processing CREATE event for {}: {}", eventPath, e.getMessage(), e);
                                }
                            } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                                try {
                                    if (Files.isDirectory(eventPath)) {
                                        continue;
                                    }

                                    BasicFileAttributes attrs = Files.readAttributes(eventPath, BasicFileAttributes.class);
                                    File file = File.from(eventPath, attrs);

                                    Output output = Output.builder()
                                        .file(file)
                                        .changeType("UPDATE")
                                        .build();

                                    Execution execution = TriggerService.generateRealtimeExecution(
                                        RealtimeTrigger.this,
                                        conditionContext,
                                        context,
                                        output
                                    );

                                    emitter.next(execution);

                                } catch (NoSuchFileException e) {
                                    // Race: file was modified but deleted before we could read it
                                } catch (Exception e) {
                                    logger.warn("Error processing MODIFY event for {}: {}", eventPath, e.getMessage(), e);
                                }
                            } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                try {
                                    File file = File.from(eventPath, null);

                                    Output output = Output.builder()
                                        .file(file)
                                        .changeType("DELETE")
                                        .build();

                                    Execution execution = TriggerService.generateRealtimeExecution(
                                        RealtimeTrigger.this,
                                        conditionContext,
                                        context,
                                        output
                                    );

                                    emitter.next(execution);

                                } catch (Exception e) {
                                    logger.warn("Error processing DELETE event for {}: {}", eventPath, e.getMessage(), e);
                                }
                            }

                        } catch (Exception e) {
                            // An error processing this individual event should not terminate the entire watcher.
                            logger.warn("Error processing filesystem event: {}", e.getMessage(), e);
                            // Continue to the next event
                        }
                    }

                    // Reset the key for future events
                    boolean valid = key.reset();
                    if (!valid) {
                        // Key is no longer valid (directory deleted, etc.)
                        watchKeyMap.remove(key);
                    }
                }

                logger.info("RealtimeTrigger event loop completed");

                emitter.complete();
            } catch (Exception e) {
                logger.error("RealtimeTrigger error: {}", e.getMessage(), e);
                emitter.error(e);
            } finally {
                cleanup();
            }
        }).doOnCancel(() -> {
            logger.info("RealtimeTrigger cancelled");
            stop();
        });
    }

    /**
     * Determines whether a WatchEvent.Kind matches the configured EventType filter.
     * Maps WatchService StandardWatchEventKinds to EventType values.
     *
     * @param kind the WatchEvent.Kind from the WatchService
     * @param eventType the configured EventType filter
     * @return true if the event should be processed, false otherwise
     */
    private boolean shouldTrigger(WatchEvent.Kind<?> kind, EventType eventType) {
        if (eventType == EventType.CREATE) {
            return kind == StandardWatchEventKinds.ENTRY_CREATE;
        } else if (eventType == EventType.UPDATE) {
            return kind == StandardWatchEventKinds.ENTRY_MODIFY;
        } else if (eventType == EventType.CREATE_OR_UPDATE) {
            return kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_MODIFY;
        } else if (eventType == EventType.DELETE) {
            return kind == StandardWatchEventKinds.ENTRY_DELETE;
        }
        return false;
    }

    /**
     * Tests whether a file path matches the configured regex pattern.
     * The pattern is applied against the full absolute path of the file.
     *
     * @param path the file path to test
     * @param pattern the regex pattern (may be null)
     * @return true if the pattern is null (no filter) or the path matches; false otherwise
     */
    private boolean matchesRegExp(Path path, String pattern) {
        if (pattern == null) {
            return true;
        }
        try {
            String pathStr = path.toString();
            return pathStr.matches(pattern);
        } catch (Exception e) {
            logger.warn("Error matching regex pattern '{}' against path '{}': {}", 
                pattern, path, e.getMessage());
            return false;
        }
    }

    /**
     * Creates the WatchService for filesystem monitoring.
     */
    private void createWatchService() throws Exception {
        if (watchService.get() != null) {
            throw new IllegalStateException("WatchService already created");
        }
        watchService.set(FileSystems.getDefault().newWatchService());
    }

    /**
     * Registers a single directory to be watched for events.
     * Events monitored: ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE
     *
     * @param directory the directory to register
     * @throws Exception if registration fails
     */
    private void registerDirectory(Path directory) throws Exception {
        WatchService svc = watchService.get();
        if (svc == null) {
            throw new IllegalStateException("WatchService not initialized");
        }
        WatchKey key = directory.register(
            svc,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE
        );
        watchKeyMap.put(key, directory);
    }

    /**
     * Recursively walks the directory tree and registers all subdirectories.
     * Called during startup when recursive: true.
     * 
     * If a child directory disappears during the walk (race condition),
     * it is skipped and registration continues. Only the root directory
     * disappearance is treated as a fatal error (handled by caller).
     *
     * @param root the root directory to walk
     * @throws Exception if walking the root fails
     */
    private void registerDirectoryTree(Path root) throws Exception {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry) && !Files.isSymbolicLink(entry)) {
                    try {
                        registerDirectory(entry);
                        // Recursively register subdirectories
                        registerDirectoryTree(entry);
                    } catch (IOException e) {
                        // Child directory disappeared or became inaccessible during the walk.
                        // This is a race condition, not a startup failure. Skip and continue.
                    }
                }
            }
        }
    }

    @Override
    public void stop() {
        if (active.compareAndSet(true, false)) {
            // Unblock the event loop by closing the WatchService.
            // The evaluate() thread will perform final cleanup in its finally block.
            closeWatchService();
        }
    }

    @Override
    public void kill() {
        if (logger != null) {
            logger.debug("RealtimeTrigger kill signal received");
        }
        stop();
    }

    /**
     * Closes the WatchService to unblock the event loop.
     * Uses getAndSet(null) to atomically claim ownership.
     * Only the thread that successfully gets a non-null reference will close it.
     */
    private void closeWatchService() {
        WatchService svc = watchService.getAndSet(null);
        if (svc == null) {
            return;
        }

        try {
            svc.close();
            logger.info("RealtimeTrigger WatchService closed");
        } catch (Exception e) {
            logger.warn("Error closing WatchService: {}", e.getMessage());
        }
    }

    private void cleanup() {
        // Atomically claim ownership of the WatchService.
        // getAndSet(null) ensures only one thread gets a non-null reference.
        WatchService svc = watchService.getAndSet(null);

        try {
            if (svc != null) {
                svc.close();
                logger.info("RealtimeTrigger WatchService closed");
            }
        } catch (Exception e) {
            logger.warn("Error closing WatchService during cleanup: {}", e.getMessage());
        } finally {
            // Establish invariant: trigger is fully inactive
            active.set(false);
            
            if (watchKeyMap != null) {
                watchKeyMap.clear();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The file that triggered the event")
        private final File file;

        @Schema(title = "The type of change that occurred (CREATE, UPDATE, DELETE)")
        private final String changeType;
    }
}
