package com.redis.kafka.connect.operation;

import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class Zadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(Zadd.class);

    private Function<T, ScoredValue<V>> valueFunction;
    private Function<T, ZAddArgs> argsFunction = (t) -> null;
    private Function<T, Boolean> conditionFunction;
    private Function<T, V> memberFunction;

    public Zadd() {
        log.info("Zadd operation initialized.");
    }

    public void setArgsFunction(Function<T, ZAddArgs> function) {
        this.argsFunction = function;
        log.info("Args function set.");
    }

    public void setMemberFunction(Function<T, V> function) {
        this.memberFunction = function;
        log.info("Member function set.");
    }

    public void setValueFunction(Function<T, ScoredValue<V>> function) {
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
        log.info("Executing Zadd operation.");
        log.debug("Key: {}", key);
        log.debug("Item: {}", item);

        try {
            // Start transaction
            RedisAsyncCommands<K, V> asyncCommands = (RedisAsyncCommands<K, V>) commands;
            asyncCommands.multi();

            // Apply condition function and handle condition met case in a single block
            if (conditionFunction != null && conditionFunction.apply(item)) {
                log.debug("Condition met, proceeding with member removal.");

                V member = memberFunction.apply(item);
                log.debug("Member: {}", member);

                log.info("Removing member from sorted set.");
                asyncCommands.zrem(key, member);
            } else {
                log.info("Condition not met or condition function not set, proceeding with add operation.");

                ScoredValue<V> value = valueFunction.apply(item);
                if (value == null) {
                    log.error("Value is null. Skipping addition to sorted set.");
                    asyncCommands.discard();
                    return null;
                }

                ZAddArgs args = argsFunction.apply(item);
                log.debug("ZAddArgs: {}", args);

                log.info("Adding value to sorted set.");
                asyncCommands.zadd(key, args, value);
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
            log.error("Error during Zadd operation: {}", e.getMessage(), e);
            return null;
        }
    }
}
