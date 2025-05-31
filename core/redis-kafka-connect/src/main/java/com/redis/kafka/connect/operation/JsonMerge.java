package com.redis.kafka.connect.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.redis.kafka.connect.adapter.RedisFutureAdapter;
import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JsonMerge<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(JsonMerge.class);

    public static final String ROOT_PATH = "$";
    private static final Function<Object, String> DEFAULT_PATH_FUNCTION = t -> ROOT_PATH;

    private Function<T, String> pathFunction;
    private Function<T, String> subPathFunction;
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;

    private final ObjectMapper mapper;

    public JsonMerge() {
        // Set default path and subpath functions (standardmäßig auf ROOT_PATH)
        this.pathFunction = (Function<T, String>) DEFAULT_PATH_FUNCTION;
        this.subPathFunction = (Function<T, String>) (t -> ""); // Default: leer
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public void setPath(String path) {
        this.pathFunction = t -> path;
    }

    public void setPathFunction(Function<T, String> pathFunction) {
        this.pathFunction = pathFunction;
    }

    /**
     * Setzt die Funktion zur Bestimmung des Sub-Pfads.
     * Beim Delete wird dieser Sub-Pfad an den Hauptpfad angehängt.
     */
    public void setSubPathFunction(Function<T, String> subPathFunction) {
        this.subPathFunction = subPathFunction;
    }

    public void setValueFunction(Function<T, V> valueFunction) {
        this.valueFunction = valueFunction;
    }

    public void setConditionFunction(Function<T, Boolean> conditionFunction) {
        this.conditionFunction = conditionFunction;
    }

    /**
     * Für Merge-Operationen wird nur der Hauptpfad verwendet.
     */
    private String determineMergePath(T item) {
        String mainPath = this.pathFunction.apply(item);
        log.debug("Determined merge path: {}", mainPath);
        return mainPath;
    }

    /**
     * Für Delete-Operationen wird der Sub-Pfad (falls definiert) an den Hauptpfad angehängt.
     */
    private String determineDeletePath(T item) {
        String mainPath = this.pathFunction.apply(item);
        String subPath = this.subPathFunction.apply(item);
        if (subPath != null && !subPath.trim().isEmpty()) {
            // Falls subPath nicht mit einem Punkt beginnt, diesen ergänzen.
            if (!subPath.trim().startsWith(".")) {
                subPath = "." + subPath.trim();
            }
            String fullPath = mainPath + subPath;
            log.debug("Determined delete path: mainPath={} subPath={} -> fullPath={}", mainPath, subPath, fullPath);
            return fullPath;
        }
        log.debug("No subpath configured; delete path equals merge path: {}", mainPath);
        return mainPath;
    }

    @Override
    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        try {
            // Für Merge-Operationen verwenden wir den Merge-Pfad
            String mergePath = determineMergePath(item);
            logPath(mergePath);
            V value = this.valueFunction.apply(item);
            log.info("Value: {}", value);

            if (conditionFunction.apply(item)) {
                // Für Löschoperationen wird der Delete-Pfad (Hauptpfad + Subpfad) verwendet
                if (isPathSet()) {
                    String deletePath = determineDeletePath(item);
                    if (deletePath.contains(",")) {
                        return deleteMultipleJsonPathsParallel(commands, key, deletePath);
                    } else {
                        log.debug("Deleting single JSON path: {}", deletePath);
                        return deleteJsonPath(commands, key, deletePath);
                    }
                } else {
                    log.debug("Condition met and no specific path set, deleting entire key.");
                    return deleteKey(commands, key);
                }
            }

            log.info("isPathSet: {}  mergePath: {}", isPathSet(), mergePath);

            // Bei Merge-Operationen verwenden wir den Merge-Pfad (ohne Subpfad)
            if (isPathSet()) {
                return performJsonMerge(commands, key, mergePath, value);
            } else {
                return performJsonSet(commands, key, value);
            }
        } catch (JsonProcessingException e) {
            log.error("Error processing JSON", e);
            return null;
        } catch (Exception e) {
            log.error("Error executing Redis command", e);
            return null;
        }
    }

    private RedisFuture<String> deleteKey(BaseRedisAsyncCommands<K, V> commands, K key) {
        log.debug("Deleting key: {}", key);
        return ((RedisKeyAsyncCommands) commands).del(key);
    }

    private RedisFuture<String> deleteJsonPath(BaseRedisAsyncCommands<K, V> commands, K key, String path) {
        log.debug("Deleting JSON path: {}", path);
        return ((RedisJSONAsyncCommands) commands).jsonDel(key, path);
    }

    /**
     * Löscht mehrere durch Komma getrennte JSON-Pfade parallel.
     * Falls kein Pfad definiert ist, wird der gesamte Schlüssel gelöscht.
     */
    private RedisFuture<String> deleteMultipleJsonPathsParallel(BaseRedisAsyncCommands<K, V> commands, K key, String pathsStr) {
        List<String> pathsToDelete = Arrays.stream(pathsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
        log.debug("Parsed paths to delete: {}", pathsToDelete);
        if (pathsToDelete.isEmpty()) {
            log.debug("No paths found; deleting entire key: {}", key);
            return deleteKey(commands, key);
        }

        List<CompletableFuture<String>> futures = pathsToDelete.stream()
                .map(path -> {
                    log.debug("Starting deletion for JSON path: {}", path);
                    return deleteJsonPath(commands, key, path)
                            .toCompletableFuture()
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    log.error("Error deleting JSON path '{}': {}", path, ex.getMessage());
                                } else {
                                    log.debug("Successfully deleted JSON path '{}': result={}", path, result);
                                }
                            });
                })
                .collect(Collectors.toList());

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        CompletableFuture<String> result = allFutures.thenApply(v -> {
            log.debug("All JSON path deletions completed for key: {}", key);
            return "OK";
        });
        return new RedisFutureAdapter<>(result);
    }

    private void logPath(String path) {
        if (isPathSet()) {
            log.info("Path is set to: {}", path);
        } else {
            log.info("Path is not set, using default: {}", ROOT_PATH);
        }
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<String> performJsonMerge(BaseRedisAsyncCommands<K, V> commands, K key, String path, V value) throws JsonProcessingException {
        String emptyJson = mapper.writeValueAsString(new Object());
        byte[] emptyJsonBytes = emptyJson.getBytes(StandardCharsets.UTF_8);
        log.info("Performing JSON merge - key: {}  path: {}  value: {}", key, path, value);
        ((RedisJSONAsyncCommands<K, V>) commands).jsonMerge(key, ROOT_PATH, (V) emptyJsonBytes);
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonMerge(key, path, value);
    }

    private RedisFuture<String> performJsonSet(BaseRedisAsyncCommands<K, V> commands, K key, V value) throws JsonProcessingException {
        log.info("Performing JSON set - key: {}  value: {}", key, value);
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, ROOT_PATH, value);
    }

    private boolean isPathSet() {
        boolean pathSet = this.pathFunction != DEFAULT_PATH_FUNCTION;
        log.debug("isPathSet: {}", pathSet);
        return pathSet;
    }
}
