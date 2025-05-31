package com.redis.kafka.connect.operation;

import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class Sadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(Sadd.class);
    
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;

    public Sadd() {
        log.info("Sadd operation initialized.");
    }

    public void setValueFunction(Function<T, V> function) {
        this.valueFunction = function;
        log.info("Value function set.");
    }

    public void setConditionFunction(Function<T, Boolean> function) {
        this.conditionFunction = function;
        log.info("Condition function set.");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        log.info("Executing Sadd operation.");
        log.debug("Key: {}", key);
        log.debug("Item: {}", item);

        try {
            // Start transaction
            RedisAsyncCommands<K, V> asyncCommands = (RedisAsyncCommands<K, V>) commands;
            asyncCommands.multi();
            
            V value = valueFunction.apply(item);
            if (conditionFunction != null && conditionFunction.apply(item)) {
                log.debug("Condition met, proceeding with member removal.");
                log.info("Removing member from set.");
                asyncCommands.srem(key, value);
            } else {
                log.info("Condition not met or condition function not set, proceeding with add operation.");
                if (value == null) {
                    log.error("Value is null. Skipping addition to set.");
                    asyncCommands.discard();
                    return null;
                }
                log.info("Adding value to set.");
                asyncCommands.sadd(key, value);
            }

            // Execute transaction and return the result
            return (RedisFuture<Long>) asyncCommands.exec().thenApply(results -> {
                if (results == null || results.isEmpty()) {
                    return 0L;
                }
                Object result = results.get(0);
                if (result instanceof Long) {
                    return (Long) result;
                }
                return 0L;
            });

        } catch (Exception e) {
            log.error("Error during Sadd operation: {}", e.getMessage(), e);
            return null;
        }
    }
}
