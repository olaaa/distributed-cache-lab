package io.github.olaaa.distributedcachelab.rates;

import java.time.Duration;

/* Последний известный курс старше допустимого: отдавать его нельзя. */
public class StaleRateException extends RuntimeException {

    public StaleRateException(String currency, Duration age) {
        super("Курс " + currency + " устарел: ему " + age);
    }
}
