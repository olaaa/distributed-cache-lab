package io.github.olaaa.distributedcachelab.rates;

import java.math.BigDecimal;
import java.time.Instant;

/* Курс валюты к рублю и момент, на который его выдал поставщик. */
public record ExchangeRate(String currency, BigDecimal rate, Instant asOf) {
}
