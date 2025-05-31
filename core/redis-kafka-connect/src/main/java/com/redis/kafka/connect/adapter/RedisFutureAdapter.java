package com.redis.kafka.connect.adapter;

import io.lettuce.core.RedisFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RedisFutureAdapter<T> extends CompletableFuture<T> implements RedisFuture<T> {

    private volatile String error;

    public RedisFutureAdapter(CompletableFuture<T> delegate) {
        delegate.whenComplete((result, throwable) -> {
            if (throwable != null) {
                error = throwable.getMessage();
                this.completeExceptionally(throwable);
            } else {
                this.complete(result);
            }
        });
    }

    /**
     * Liefert den Fehlertext, falls ein Fehler aufgetreten ist, ansonsten null.
     */
    @Override
    public String getError() {
        return error;
    }

    /**
     * Wartet bis zu der angegebenen Zeit auf den Abschluss dieses Futures.
     * Gibt {@code true} zurück, wenn das Future innerhalb des Zeitlimits abgeschlossen wurde, ansonsten {@code false}.
     */
    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            // Versucht, das Future innerhalb des Zeitlimits abzuschließen.
            this.get(timeout, unit);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (ExecutionException e) {
            // Auch wenn ein Fehler auftrat, ist das Future abgeschlossen.
            return true;
        }
    }
}

