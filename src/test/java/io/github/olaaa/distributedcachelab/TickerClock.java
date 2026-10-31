package io.github.olaaa.distributedcachelab;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/*
 * Часы приложения, которые идут вместе с ManualTicker: сдвиг тикера двигает и сроки Caffeine,
 * и время, по которому считается возраст курса. Одна команда — одно время.
 */
public final class TickerClock extends Clock {

    private static final Instant START = Instant.parse("2026-11-01T10:00:00Z");

    private final ManualTicker ticker;

    public TickerClock(ManualTicker ticker) {
        this.ticker = ticker;
    }

    @Override
    public Instant instant() {
        return START.plusNanos(ticker.read());
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("часы лаборатории — только UTC");
    }
}
