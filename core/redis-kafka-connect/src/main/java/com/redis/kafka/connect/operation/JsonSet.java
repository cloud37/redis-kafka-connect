package com.redis.kafka.connect.operation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

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

    protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        String path = determinePath(item);
        logPath(path);

        if (conditionFunction.apply(item)) {
            if (isPathSet()) {
                return deleteJsonPath(commands, key, path);
            } else {
                return deleteKey(commands, key);
            }
        } else {
            try {
                V value = this.valueFunction.apply(item);

                // Falls ein Fehler auftritt (z.B. das Dokument existiert nicht), setze ein leeres JSON und versuche es erneut
                String emptyJson = mapper.writeValueAsString(new Object());
                byte[] emptyJsonBytes = emptyJson.getBytes(StandardCharsets.UTF_8);
                ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, ROOT_PATH, (V) emptyJsonBytes);

                // Erneuter Versuch, das JSON zu setzen
                return ((RedisJSONAsyncCommands) commands).jsonSet(key, path, value);
            } catch (JsonProcessingException e) {
                log.error("Error processing JSON", e);
                return null;
            } catch (Exception e) {
                log.error("Error executing Redis command", e);
                return null;
            }
        }
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

    private RedisFuture<String> deleteKey(BaseRedisAsyncCommands<K, V> commands, K key) {
        return ((RedisKeyAsyncCommands) commands).del(key);
    }

    private RedisFuture<String> deleteJsonPath(BaseRedisAsyncCommands<K, V> commands, K key, String path) {
        return ((RedisJSONAsyncCommands) commands).jsonDel(key, path);
    }
}
