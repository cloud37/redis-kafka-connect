package com.redis.kafka.connect.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

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
        @SuppressWarnings("unchecked")
        Function<T, String> defaultPathFunc = (Function<T, String>) DEFAULT_PATH_FUNCTION;
        this.pathFunction = defaultPathFunc;
        this.subPathFunction = (t -> "");
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        log.info("JsonMerge operation initialized.");
    }

    public void setPathFunction(Function<T, String> pathFunction) {
        this.pathFunction = pathFunction;
        log.info("Path function set.");
    }

    public void setSubPathFunction(Function<T, String> subPathFunction) {
        this.subPathFunction = subPathFunction;
        log.info("Subpath function set.");
    }

    public void setValueFunction(Function<T, V> valueFunction) {
        this.valueFunction = valueFunction;
        log.info("Value function set.");
    }

    public void setConditionFunction(Function<T, Boolean> conditionFunction) {
        this.conditionFunction = conditionFunction;
        log.info("Condition function set.");
    }

    private String determineMergePath(T item) {
        String mainPath = this.pathFunction.apply(item);
        log.debug("Determined merge path: {}", mainPath);
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
        log.debug("No subpath configured; delete path equals main path: {}", mainPath);
        return mainPath;
    }

    private void logPath(String path) {
        if (isPathSet()) {
            log.info("Path is set to: {}", path);
        } else {
            log.info("Path is not set, using default: {}", ROOT_PATH);
        }
    }

    private boolean isPathSet() {
        boolean pathSet = this.pathFunction != DEFAULT_PATH_FUNCTION;
        log.debug("isPathSet: {}", pathSet);
        return pathSet;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        log.info("Executing JsonMerge operation.");
        log.debug("Key: {}", key);
        log.debug("Item: {}", item);

        try {
            // Start transaction
            RedisAsyncCommands<K, V> asyncCommands = (RedisAsyncCommands<K, V>) commands;
            RedisJSONAsyncCommands<K, V> jsonCommands = (RedisJSONAsyncCommands<K, V>) commands;
            asyncCommands.multi();

            if (conditionFunction != null && conditionFunction.apply(item)) {
                log.debug("Condition met, proceeding with JSON deletion.");
                if (isPathSet()) {
                    String deletePath = determineDeletePath(item);
                    if (deletePath.contains(",")) {
                        String[] paths = deletePath.split(",");
                        for (String p : paths) {
                            String trimmedPath = p.trim();
                            if (!trimmedPath.isEmpty()) {
                                log.info("Deleting JSON at path: {}", trimmedPath);
                                jsonCommands.jsonDel(key, trimmedPath);
                            }
                        }
                    } else {
                        log.info("Deleting JSON at path: {}", deletePath);
                        jsonCommands.jsonDel(key, deletePath);
                    }
                } else {
                    log.debug("Condition met and no specific path set, deleting entire key.");
                    ((RedisKeyAsyncCommands<K, V>) commands).del(key);
                }
            } else {
                log.info("Condition not met or condition function not set, proceeding with JSON merge operation.");
                V value = valueFunction.apply(item);
                if (value == null) {
                    log.error("Value is null. Skipping JSON merge operation.");
                    asyncCommands.discard();
                    return null;
                }

                String mergePath = determineMergePath(item);
                logPath(mergePath);
                log.info("isPathSet: {}  mergePath: {}", isPathSet(), mergePath);

                if (isPathSet()) {
                    String emptyJson = mapper.writeValueAsString(new Object());
                    byte[] emptyJsonBytes = emptyJson.getBytes(StandardCharsets.UTF_8);
                    jsonCommands.jsonMerge(key, ROOT_PATH, (V) emptyJsonBytes);
                    jsonCommands.jsonMerge(key, mergePath, value);
                } else {
                    jsonCommands.jsonSet(key, ROOT_PATH, value);
                }
            }

            // Execute transaction and return the result
            return (RedisFuture<String>) asyncCommands.exec().thenApply(results -> {
                if (results == null || results.isEmpty()) {
                    return "OK";
                }
                Object result = results.get(0);
                if (result instanceof String) {
                    return (String) result;
                }
                return "OK";
            });

        } catch (JsonProcessingException e) {
            log.error("Error processing JSON: {}", e.getMessage(), e);
            return null;
        } catch (Exception e) {
            log.error("Error executing Redis command: {}", e.getMessage(), e);
            return null;
        }
    }
}
