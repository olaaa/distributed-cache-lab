package io.github.olaaa.distributedcachelab;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.Ticker;

/* Часы, которые идут только по команде: TTL и refreshAfterWrite проверяются без sleep. */
public final class ManualTicker implements Ticker {

    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long read() {
        return nanos.get();
    }

    public void advance(Duration duration) {
        nanos.addAndGet(duration.toNanos());
    }
}
