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
import java.util.concurrent.CompletableFuture;

public class JsonSet<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(JsonSet.class);

    public static final String ROOT_PATH = "$";
    private Function<T, String> pathFunction;
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;

    private static final Function<Object, String> DEFAULT_PATH_FUNCTION = t -> ROOT_PATH;

    private final ObjectMapper mapper;

    public JsonSet() {
        this.pathFunction = (Function<T, String>) DEFAULT_PATH_FUNCTION;
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public void setPathFunction(Function<T, String> path) {
        this.pathFunction = path;
    }

    public void setValueFunction(Function<T, V> value) {
        this.valueFunction = value;
    }

    public void setConditionFunction(Function<T, Boolean> function) {
        this.conditionFunction = function;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        if (!(commands instanceof RedisJSONAsyncCommands)) {
            throw new IllegalArgumentException("Commands must be an instance of RedisJSONAsyncCommands");
        }
        return execute((RedisJSONAsyncCommands<K, V>) commands, item, key);
    }

    protected RedisFuture<String> execute(RedisJSONAsyncCommands<K, V> commands, T item, K key) {
        log.info("Executing JsonSet operation.");
        log.debug("Key: {}", key);
        log.debug("Item: {}", item);

        try {
            // Start transaction
            RedisJSONAsyncCommands<K, V> asyncCommands = (RedisJSONAsyncCommands<K, V>) commands;
            ((RedisAsyncCommands<K, V>) asyncCommands).multi();

            String path = pathFunction.apply(item);
            V value = valueFunction.apply(item);

            if (value == null) {
                log.error("Value is null. Skipping operation.");
                ((RedisAsyncCommands<K, V>) asyncCommands).discard();
                return null;
            }

            if (conditionFunction != null && conditionFunction.apply(item)) {
                log.debug("Condition met, proceeding with deletion.");
                commands.jsonDel(key, path);
            } else {
                log.debug("Setting JSON value at path: {}", path);
                commands.jsonSet(key, path, value);
            }

            // Execute transaction and return the result
            return (RedisFuture<String>) ((RedisAsyncCommands<K, V>) asyncCommands).exec().thenApply(results -> {
                if (results == null || results.isEmpty()) {
                    return "OK";
                }
                Object result = results.get(0);
                if (result instanceof String) {
                    return (String) result;
                }
                return "OK";
            });

        } catch (Exception e) {
            log.error("Error during JsonSet operation: {}", e.getMessage(), e);
            return null;
        }
    }

    // Diese Methode ist nur für Tests gedacht
    public RedisFuture<String> executeForTest(RedisJSONAsyncCommands<K, V> commands, T item, K key) {
        return execute(commands, item, key);
    }

    private String determinePath(T item) {
        return this.pathFunction.apply(item);
    }

    private void logPath(String path) {
        if (isPathSet()) {
            log.info("Path is set to: {}", path);
            System.out.println("Path is set to: " + path);
        } else {
            log.info("Path is not set, using default: {}", ROOT_PATH);
            System.out.println("Path is not set, using default: " + ROOT_PATH);
        }
    }

    private boolean isPathSet() {
        return this.pathFunction != DEFAULT_PATH_FUNCTION;
    }

    private CompletableFuture<String> deleteKey(RedisJSONAsyncCommands<K, V> commands, K key) {
        RedisFuture<Long> future = ((RedisKeyAsyncCommands<K, V>) commands).del(key);
        CompletableFuture<String> mapped = new CompletableFuture<>();
        future.whenComplete((res, ex) -> {
            if (ex != null) mapped.completeExceptionally(ex);
            else mapped.complete("OK");
        });
        return mapped;
    }

    private CompletableFuture<String> deleteJsonPath(RedisJSONAsyncCommands<K, V> commands, K key, String path) {
        RedisFuture<Long> future = commands.jsonDel(key, path);
        CompletableFuture<String> mapped = new CompletableFuture<>();
        future.whenComplete((res, ex) -> {
            if (ex != null) mapped.completeExceptionally(ex);
            else mapped.complete("OK");
        });
        return mapped;
    }
}
