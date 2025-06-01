/*
 * Decompiled with CFR 0.153-SNAPSHOT (d6f6758-dirty).
 */
package com.redis.kafka.connect.operation;

import com.redis.kafka.connect.shaded.com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import com.redis.kafka.connect.shaded.io.lettuce.core.RedisFuture;
import com.redis.kafka.connect.shaded.io.lettuce.core.api.async.BaseRedisAsyncCommands;
import com.redis.kafka.connect.shaded.io.lettuce.core.api.async.RedisSetAsyncCommands;
import java.util.function.Function;

public class Sadd<K, V, T>
extends AbstractKeyWriteOperation<K, V, T> {
    private Function<T, V> valueFunction;
    private Function<T, Boolean> conditionFunction;

    public void setValueFunction(Function<T, V> function) {
        this.valueFunction = function;
    }

    public void setConditionFunction(Function<T, Boolean> function) {
        this.conditionFunction = function;
    }

    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        V value = this.valueFunction.apply(item);
        if (this.conditionFunction.apply(item).booleanValue()) {
            return ((RedisSetAsyncCommands)((Object)commands)).srem(key, value);
        }
        return ((RedisSetAsyncCommands)((Object)commands)).sadd(key, value);
    }
}

