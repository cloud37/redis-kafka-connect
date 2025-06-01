/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.redis.kafka.connect.operation;

import com.redis.kafka.connect.adapter.RedisFutureAdapter;
import com.redis.kafka.connect.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.kafka.connect.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.kafka.connect.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import com.redis.kafka.connect.shaded.com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.kafka.connect.shaded.com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import com.redis.kafka.connect.shaded.io.lettuce.core.RedisFuture;
import com.redis.kafka.connect.shaded.io.lettuce.core.api.async.BaseRedisAsyncCommands;
import com.redis.kafka.connect.shaded.io.lettuce.core.api.async.RedisKeyAsyncCommands;
import com.redis.kafka.connect.shaded.org.slf4j.Logger;
import com.redis.kafka.connect.shaded.org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JsonMerge<K, V, T>
extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(JsonMerge.class);
    public static final String ROOT_PATH = "$";
    private static final Function<Object, String> DEFAULT_PATH_FUNCTION = t -> "$";
    private Function<T, String> pathFunction = DEFAULT_PATH_FUNCTION;
    private Function<T, String> subPathFunction = t -> "";
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonMerge() {
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public void setPath(String path) {
        this.pathFunction = t -> path;
    }

    public void setPathFunction(Function<T, String> pathFunction) {
        this.pathFunction = pathFunction;
    }

    public void setSubPathFunction(Function<T, String> subPathFunction) {
        this.subPathFunction = subPathFunction;
    }

    public void setValueFunction(Function<T, V> valueFunction) {
        this.valueFunction = valueFunction;
    }

    public void setConditionFunction(Function<T, Boolean> conditionFunction) {
        this.conditionFunction = conditionFunction;
    }

    private String determineMergePath(T item) {
        String mainPath = this.pathFunction.apply(item);
        log.debug("Determined merge path: {}", (Object)mainPath);
        return mainPath;
    }

    private String determineDeletePath(T item) {
        String mainPath = this.pathFunction.apply(item);
        String subPath = this.subPathFunction.apply(item);
        if (subPath != null && !subPath.trim().isEmpty()) {
            if (!subPath.trim().startsWith(".")) {
                subPath = "." + subPath.trim();
            }
            String fullPath = mainPath + subPath;
            log.debug("Determined delete path: mainPath={} subPath={} -> fullPath={}", mainPath, subPath, fullPath);
            return fullPath;
        }
        log.debug("No subpath configured; delete path equals merge path: {}", (Object)mainPath);
        return mainPath;
    }

    @Override
    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        try {
            String mergePath = this.determineMergePath(item);
            this.logPath(mergePath);
            V value = this.valueFunction.apply(item);
            log.info("Value: {}", (Object)value);
            if (this.conditionFunction.apply(item).booleanValue()) {
                if (this.isPathSet()) {
                    String deletePath = this.determineDeletePath(item);
                    if (deletePath.contains(",")) {
                        return this.deleteMultipleJsonPathsParallel(commands, key, deletePath);
                    }
                    log.debug("Deleting single JSON path: {}", (Object)deletePath);
                    return this.deleteJsonPath(commands, key, deletePath);
                }
                log.debug("Condition met and no specific path set, deleting entire key.");
                return this.deleteKey(commands, key);
            }
            log.info("isPathSet: {}  mergePath: {}", (Object)this.isPathSet(), (Object)mergePath);
            if (this.isPathSet()) {
                return this.performJsonMerge(commands, key, mergePath, value);
            }
            return this.performJsonSet(commands, key, value);
        } catch (JsonProcessingException e) {
            log.error("Error processing JSON", e);
            return null;
        } catch (Exception e) {
            log.error("Error executing Redis command", e);
            return null;
        }
    }

    private RedisFuture<String> deleteKey(BaseRedisAsyncCommands<K, V> commands, K key) {
        log.debug("Deleting key: {}", (Object)key);
        return ((RedisKeyAsyncCommands)((Object)commands)).del(key);
    }

    private RedisFuture<String> deleteJsonPath(BaseRedisAsyncCommands<K, V> commands, K key, String path) {
        log.debug("Deleting JSON path: {}", (Object)path);
        return ((RedisJSONAsyncCommands)((Object)commands)).jsonDel(key, path);
    }

    private RedisFuture<String> deleteMultipleJsonPathsParallel(BaseRedisAsyncCommands<K, V> commands, K key, String pathsStr) {
        List pathsToDelete = Arrays.stream(pathsStr.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
        log.debug("Parsed paths to delete: {}", (Object)pathsToDelete);
        if (pathsToDelete.isEmpty()) {
            log.debug("No paths found; deleting entire key: {}", (Object)key);
            return this.deleteKey(commands, key);
        }
        List<CompletableFuture> futures = pathsToDelete.stream().map(path -> {
            log.debug("Starting deletion for JSON path: {}", path);
            return this.deleteJsonPath(commands, key, (String)path).toCompletableFuture().whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Error deleting JSON path '{}': {}", path, (Object)ex.getMessage());
                } else {
                    log.debug("Successfully deleted JSON path '{}': result={}", path, result);
                }
            });
        }).collect(Collectors.toList());
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        CompletionStage result = allFutures.thenApply(v -> {
            log.debug("All JSON path deletions completed for key: {}", key);
            return "OK";
        });
        return new RedisFutureAdapter<String>((CompletableFuture<String>)result);
    }

    private void logPath(String path) {
        if (this.isPathSet()) {
            log.info("Path is set to: {}", (Object)path);
        } else {
            log.info("Path is not set, using default: {}", (Object)ROOT_PATH);
        }
    }

    private RedisFuture<String> performJsonMerge(BaseRedisAsyncCommands<K, V> commands, K key, String path, V value) throws JsonProcessingException {
        String emptyJson = this.mapper.writeValueAsString(new Object());
        byte[] emptyJsonBytes = emptyJson.getBytes(StandardCharsets.UTF_8);
        log.info("Performing JSON merge - key: {}  path: {}  value: {}", key, path, value);
        ((RedisJSONAsyncCommands)((Object)commands)).jsonMerge(key, ROOT_PATH, emptyJsonBytes);
        return ((RedisJSONAsyncCommands)((Object)commands)).jsonMerge(key, path, value);
    }

    private RedisFuture<String> performJsonSet(BaseRedisAsyncCommands<K, V> commands, K key, V value) throws JsonProcessingException {
        log.info("Performing JSON set - key: {}  value: {}", (Object)key, (Object)value);
        return ((RedisJSONAsyncCommands)((Object)commands)).jsonSet(key, ROOT_PATH, value);
    }

    private boolean isPathSet() {
        boolean pathSet = this.pathFunction != DEFAULT_PATH_FUNCTION;
        log.debug("isPathSet: {}", (Object)pathSet);
        return pathSet;
    }
}

