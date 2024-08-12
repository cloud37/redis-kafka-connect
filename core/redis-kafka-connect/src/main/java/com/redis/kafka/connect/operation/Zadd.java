package com.redis.kafka.connect.operation;

import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import java.util.function.Function;

public class Zadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {
    private Function<T, ScoredValue<V>> valueFunction;
    private Function<T, ZAddArgs> argsFunction = (t) -> null;
    private Function<T, Boolean> conditionFunction;

    public Zadd() {
    }

    public void setArgsFunction(Function<T, ZAddArgs> function) {
        this.argsFunction = function;
    }

    public void setValueFunction(Function<T, ScoredValue<V>> function) {
        this.valueFunction = function;
    }

    public void setConditionFunction(Function<T, Boolean> function) {
        this.conditionFunction = function;
    }

    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        ScoredValue<V> value = valueFunction.apply(item);
        ZAddArgs args = argsFunction.apply(item);

        if (conditionFunction != null && conditionFunction.apply(item)) {
            // If the condition is fulfilled, remove the value
            return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, value.getValue());
        } else {
            // Otherwise, add the value
            return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, value);
        }
    }
}
