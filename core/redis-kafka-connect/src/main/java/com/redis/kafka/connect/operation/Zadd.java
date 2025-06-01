/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.redis.kafka.connect.operation;

import com.redis.kafka.connect.shaded.com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import com.redis.kafka.connect.shaded.io.lettuce.core.RedisFuture;
import com.redis.kafka.connect.shaded.io.lettuce.core.ScoredValue;
import com.redis.kafka.connect.shaded.io.lettuce.core.ZAddArgs;
import com.redis.kafka.connect.shaded.io.lettuce.core.api.async.BaseRedisAsyncCommands;
import com.redis.kafka.connect.shaded.io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import com.redis.kafka.connect.shaded.org.slf4j.Logger;
import com.redis.kafka.connect.shaded.org.slf4j.LoggerFactory;
import java.util.function.Function;

public class Zadd<K, V, T>
extends AbstractKeyWriteOperation<K, V, T> {
    private static final Logger log = LoggerFactory.getLogger(Zadd.class);
    private Function<T, ScoredValue<V>> valueFunction;
    private Function<T, ZAddArgs> argsFunction = t -> null;
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
        log.debug("Key: {}", (Object)key);
        System.out.println("Key: " + key.toString());
        log.debug("Item: {}", (Object)item);
        System.out.println("Item: " + item);
        try {
            if (this.conditionFunction != null && this.conditionFunction.apply(item).booleanValue()) {
                log.debug("Condition met, proceeding with member removal.");
                System.out.println("Condition met, proceeding with member removal.");
                V member = this.memberFunction.apply(item);
                log.debug("Member: {}", (Object)member);
                System.out.println("Member: " + member);
                log.info("Removing member from sorted set.");
                System.out.println("Removing member from sorted set.");
                return ((RedisSortedSetAsyncCommands)((Object)commands)).zrem(key, member);
            }
            log.info("Condition not met or condition function not set, proceeding with add operation.");
            System.out.println("Condition not met or condition function not set, proceeding with add operation.");
            ScoredValue<V> value = this.valueFunction.apply(item);
            if (value == null) {
                log.error("Value is null. Skipping addition to sorted set.");
                System.out.println("Value is null. Skipping addition to sorted set.");
                return null;
            }
            ZAddArgs args = this.argsFunction.apply(item);
            log.debug("ZAddArgs: {}", (Object)args);
            System.out.println("ZAddArgs: " + args);
            log.info("Adding value to sorted set.");
            System.out.println("Adding value to sorted set.");
            return ((RedisSortedSetAsyncCommands)((Object)commands)).zadd(key, args, value);
        } catch (Exception e) {
            log.error("Error during Zadd operation: {}", (Object)e.getMessage(), (Object)e);
            System.out.println("Error during Zadd operation: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}

