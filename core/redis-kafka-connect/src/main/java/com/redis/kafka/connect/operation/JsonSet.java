/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.redis.kafka.connect.operation;

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
import java.util.function.Function;

public class JsonSet<K, V, T>
extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(JsonSet.class);
    public static final String ROOT_PATH = "$";
    private Function<T, String> pathFunction = DEFAULT_PATH_FUNCTION;
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;
    private static final Function<Object, String> DEFAULT_PATH_FUNCTION = t -> "$";
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonSet() {
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public void setPath(String path) {
        this.pathFunction = t -> path;
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
    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        String path = this.determinePath(item);
        this.logPath(path);
        if (this.conditionFunction.apply(item).booleanValue()) {
            if (this.isPathSet()) {
                return this.deleteJsonPath(commands, key, path);
            }
            return this.deleteKey(commands, key);
        }
        try {
            V value = this.valueFunction.apply(item);
            String emptyJson = this.mapper.writeValueAsString(new Object());
            byte[] emptyJsonBytes = emptyJson.getBytes(StandardCharsets.UTF_8);
            ((RedisJSONAsyncCommands)((Object)commands)).jsonSet(key, ROOT_PATH, emptyJsonBytes);
            return ((RedisJSONAsyncCommands)((Object)commands)).jsonSet(key, path, value);
        } catch (JsonProcessingException e) {
            log.error("Error processing JSON", e);
            return null;
        } catch (Exception e) {
            log.error("Error executing Redis command", e);
            return null;
        }
    }

    private String determinePath(T item) {
        return this.pathFunction.apply(item);
    }

    private void logPath(String path) {
        if (this.isPathSet()) {
            log.info("Path is set to: {}", (Object)path);
            System.out.println("Path is set to: " + path);
        } else {
            log.info("Path is not set, using default: {}", (Object)ROOT_PATH);
            System.out.println("Path is not set, using default: $");
        }
    }

    private boolean isPathSet() {
        return this.pathFunction != DEFAULT_PATH_FUNCTION;
    }

    private RedisFuture<String> deleteKey(BaseRedisAsyncCommands<K, V> commands, K key) {
        return ((RedisKeyAsyncCommands)((Object)commands)).del(key);
    }

    private RedisFuture<String> deleteJsonPath(BaseRedisAsyncCommands<K, V> commands, K key, String path) {
        return ((RedisJSONAsyncCommands)((Object)commands)).jsonDel(key, path);
    }
}

