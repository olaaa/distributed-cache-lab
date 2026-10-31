package io.github.olaaa.distributedcachelab.rates;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 7. Внешний поставщик курсов валют — имитация: отвечает с задержкой, считает обращения
 * и умеет быть недоступным. Курс помечается моментом выдачи (asOf) по часам приложения.
 */
@Component
public class RateProvider {

    private static final Duration DELAY = Duration.ofMillis(50);
    private static final Map<String, BigDecimal> INITIAL_RATES = Map.of(
            "USD", new BigDecimal("92.50"),
            "EUR", new BigDecimal("99.80"),
            "CNY", new BigDecimal("12.70"));

    private final Clock clock;
    private final Map<String, BigDecimal> rates = new ConcurrentHashMap<>(INITIAL_RATES);
    private final AtomicInteger callCount = new AtomicInteger();
    private volatile boolean failing;

    /* rates.provider.failing=true — запустить реплику с уже недоступным поставщиком (ручная проверка). */
    public RateProvider(Clock clock, @Value("${rates.provider.failing:false}") boolean failing) {
        this.clock = clock;
        this.failing = failing;
    }

    public Mono<ExchangeRate> fetch(String currency) {
        return Mono.defer(() -> {
            callCount.incrementAndGet();
            if (failing) {
                /* Недоступный поставщик отвечает не сразу: ошибка приходит после сетевой задержки. */
                return Mono.delay(DELAY).then(Mono.error(new RateProviderUnavailableException(currency)));
            }
            BigDecimal rate = rates.get(currency);
            if (rate == null) {
                return Mono.empty();
            }
            return Mono.delay(DELAY).thenReturn(new ExchangeRate(currency, rate, clock.instant()));
        });
    }

    /* Поставщик опубликовал новый курс. */
    public void publish(String currency, BigDecimal rate) {
        rates.put(currency, rate);
    }

    public int callCount() {
        return callCount.get();
    }

    public boolean isFailing() {
        return failing;
    }

    public void setFailing(boolean failing) {
        this.failing = failing;
    }

    public void reset() {
        rates.clear();
        rates.putAll(INITIAL_RATES);
        callCount.set(0);
        failing = false;
    }
}
