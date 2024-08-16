package com.redis.kafka.connect.operation;

import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
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
        System.out.println("Zadd operation initialized.");
    }

    public void setArgsFunction(Function<T, ZAddArgs> function) {
        this.argsFunction = function;
        log.info("Args function set.");
        System.out.println("Args function set.");
    }

    public void setMemberFunction(Function<T, V> function) {
        this.memberFunction = function;
        log.info("Member function set.");
        System.out.println("Member function set.");
    }

    public void setValueFunction(Function<T, ScoredValue<V>> function) {
        this.valueFunction = function;
        log.info("Value function set.");
        System.out.println("Value function set.");
    }

    public void setConditionFunction(Function<T, Boolean> function) {
        this.conditionFunction = function;
        log.info("Condition function set.");
        System.out.println("Condition function set.");
    }

    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        log.info("Executing Zadd operation.");
        System.out.println("Executing Zadd operation.");
        log.debug("Key: {}", key);
        System.out.println("Key: " + key.toString());
        log.debug("Item: {}", item);
        System.out.println("Item: " + item);

        try {
            // Apply condition function and handle condition met case in a single block
            if (conditionFunction != null && conditionFunction.apply(item)) {
                log.debug("Condition met, proceeding with member removal.");
                System.out.println("Condition met, proceeding with member removal.");

                V member = memberFunction.apply(item);
                log.debug("Member: {}", member);
                System.out.println("Member: " + member);

                log.info("Removing member from sorted set.");
                System.out.println("Removing member from sorted set.");
                return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, member);
            }

            log.info("Condition not met or condition function not set, proceeding with add operation.");
            System.out.println("Condition not met or condition function not set, proceeding with add operation.");

            ScoredValue<V> value = valueFunction.apply(item);
            if (value == null) {
                log.error("Value is null. Skipping addition to sorted set.");
                System.out.println("Value is null. Skipping addition to sorted set.");
                return null;
            }

            ZAddArgs args = argsFunction.apply(item);
            log.debug("ZAddArgs: {}", args);
            System.out.println("ZAddArgs: " + args);

            log.info("Adding value to sorted set.");
            System.out.println("Adding value to sorted set.");
            return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, value);

        } catch (Exception e) {
            log.error("Error during Zadd operation: {}", e.getMessage(), e);
            System.out.println("Error during Zadd operation: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
