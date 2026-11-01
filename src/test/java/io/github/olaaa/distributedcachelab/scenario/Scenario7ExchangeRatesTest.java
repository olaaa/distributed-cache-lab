package io.github.olaaa.distributedcachelab.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

import io.github.olaaa.distributedcachelab.rates.ExchangeRate;
import io.github.olaaa.distributedcachelab.rates.ExchangeRateService;
import io.github.olaaa.distributedcachelab.rates.RateProviderUnavailableException;
import io.github.olaaa.distributedcachelab.rates.StaleRateException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;

/* Сценарий 7. Курсы валют: прогрев, refreshAfterWrite и последний известный курс, когда поставщик недоступен. */
class Scenario7ExchangeRatesTest extends ScenarioTestSupport {

    private static final List<String> CURRENCIES = List.of("USD", "EUR", "CNY");
    private static final BigDecimal USD_RATE = new BigDecimal("92.50");
    private static final BigDecimal NEW_USD_RATE = new BigDecimal("93.10");

    /* Бин сервиса — «реплика A». */
    @Autowired
    ExchangeRateService replicaA;

    @Autowired
    ReactiveRedisTemplate<String, ExchangeRate> exchangeRateRedisTemplate;

    @Autowired
    Clock clock;

    @Test
    void warmUpLoadsAllRatesSoRequestsDoNotWaitForProvider() {
        assertThat(replicaA.warmUp().block()).isEqualTo(3);
        assertThat(rateProvider.callCount()).isEqualTo(3);

        CURRENCIES.forEach(currency -> replicaA.findRate(currency).block());

        assertThat(rateProvider.callCount())
                .as("после прогрева запросы отвечают из кэша, поставщика не ждут")
                .isEqualTo(3);
        assertThat(snapshot("USD").rate())
                .as("каждый полученный курс сохранён в Redis как последний известный")
                .isEqualByComparingTo(USD_RATE);
    }

    @Test
    void afterRefreshPeriodOldRateIsServedWhileNewOneLoadsInBackground() {
        replicaA.findRate("USD").block();
        awaitEntryWritten(() -> replicaA.localAgeOf("USD"));
        rateProvider.publish("USD", NEW_USD_RATE);
        ticker.advance(ExchangeRateService.REFRESH_AFTER.plusSeconds(1));

        assertThat(replicaA.findRate("USD").block().rate())
                .as("обращение после REFRESH_AFTER сразу получает прежний курс")
                .isEqualByComparingTo(USD_RATE);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> replicaA.findRate("USD").block().rate().compareTo(NEW_USD_RATE) == 0);

        assertThat(rateProvider.callCount()).as("фоновая перезагрузка — одно обращение").isEqualTo(2);
        assertThat(snapshot("USD").rate()).isEqualByComparingTo(NEW_USD_RATE);
    }

    @Test
    void providerDownDuringRefreshKeepsLastKnownRate() {
        replicaA.findRate("USD").block();
        awaitEntryWritten(() -> replicaA.localAgeOf("USD"));
        rateProvider.setFailing(true);
        ticker.advance(ExchangeRateService.REFRESH_AFTER.plusSeconds(1));

        ExchangeRate served = replicaA.findRate("USD").block();
        await().atMost(Duration.ofSeconds(5)).until(() -> rateProvider.callCount() == 2);

        assertThat(served.rate()).isEqualByComparingTo(USD_RATE);
        assertThat(replicaA.findRate("USD").block().rate())
                .as("обновление не удалось — отдаётся последний известный курс, без ошибки")
                .isEqualByComparingTo(USD_RATE);

        /* Обновление не засчитано: поставщик вернулся — следующее обращение сразу обновляет курс. */
        rateProvider.publish("USD", NEW_USD_RATE);
        rateProvider.setFailing(false);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> replicaA.findRate("USD").block().rate().compareTo(NEW_USD_RATE) == 0);
    }

    @Test
    void rateOlderThanMaxAgeIsRefused() {
        replicaA.findRate("USD").block();
        awaitEntryWritten(() -> replicaA.localAgeOf("USD"));
        rateProvider.setFailing(true);
        ticker.advance(ExchangeRateService.MAX_AGE.plusHours(1));

        assertThatThrownBy(() -> replicaA.findRate("USD").block())
                .as("поставщик недоступен больше суток: такой курс отдавать нельзя")
                .isInstanceOf(StaleRateException.class);

        /* Поставщик вернулся: первое обращение запускает обновление, следующее получает свежий курс. */
        rateProvider.setFailing(false);
        await().atMost(Duration.ofSeconds(5)).ignoreExceptions().untilAsserted(() ->
                assertThat(replicaA.findRate("USD").block().asOf()).isEqualTo(clock.instant()));
    }

    @Test
    void firstRequestAfterQuietPeriodLongerThanMaxAgeWaitsForFreshRate() {
        replicaA.findRate("USD").block();
        awaitEntryWritten(() -> replicaA.localAgeOf("USD"));

        /* Больше суток к курсу никто не обращался — обновлять его было некому. Поставщик доступен. */
        ticker.advance(ExchangeRateService.MAX_AGE.plusHours(1));

        assertThat(replicaA.findRate("USD").block().asOf())
                .as("суточный курс не отдан и не отвергнут: запрос дождался обновления")
                .isEqualTo(clock.instant());
        assertThat(rateProvider.callCount())
                .as("get запустил обновление, refresh вернул его же — к поставщику один запрос")
                .isEqualTo(2);
    }

    @Test
    void newReplicaStartsFromLastKnownRatesWhenProviderIsDown() {
        replicaA.warmUp().block();
        rateProvider.setFailing(true);

        /* Новая реплика: пустой локальный кэш, поставщик недоступен. */
        ExchangeRateService replicaB = startReplica();
        assertThat(replicaB.warmUp().block())
                .as("все три курса взяты из Redis — последние известные")
                .isEqualTo(3);

        assertThat(replicaB.findRate("USD").block().rate()).isEqualByComparingTo(USD_RATE);
    }

    @Test
    void replicaPicksUpNewerRateFromRedisWhenProviderIsDown() {
        replicaA.findRate("USD").block();
        awaitEntryWritten(() -> replicaA.localAgeOf("USD"));
        ExchangeRateService replicaB = startReplica();
        replicaB.findRate("USD").block();
        awaitEntryWritten(() -> replicaB.localAgeOf("USD"));

        /* Поставщик опубликовал новый курс; B успела его получить, потом поставщик упал. */
        rateProvider.publish("USD", NEW_USD_RATE);
        ticker.advance(ExchangeRateService.REFRESH_AFTER.plusSeconds(1));
        replicaB.findRate("USD").block();
        await().atMost(Duration.ofSeconds(5)).until(() -> snapshot("USD").rate().compareTo(NEW_USD_RATE) == 0);
        rateProvider.setFailing(true);

        replicaA.findRate("USD").block();
        await().atMost(Duration.ofSeconds(5))
                .alias("A не достучалась до поставщика, но взяла из Redis курс новее своего")
                .until(() -> replicaA.findRate("USD").block().rate().compareTo(NEW_USD_RATE) == 0);
    }

    @Test
    void withoutLastKnownRateAndProviderDownRateIsUnavailable() {
        rateProvider.setFailing(true);

        assertThat(replicaA.warmUp().block()).as("прогрев не уронил запуск, но ничего не загрузил").isZero();
        assertThatThrownBy(() -> replicaA.findRate("USD").block())
                .isInstanceOf(RateProviderUnavailableException.class);

        /*
         * Ошибка не закэширована: упавшую загрузку Caffeine убирает сам. Но делает это колбэком future,
         * а колбэки выполняются в обратном порядке регистрации — block() вернул ошибку раньше, чем
         * Caffeine убрал упавший future. Поэтому ждём, а не проверяем сразу.
         */
        rateProvider.setFailing(false);
        await().atMost(Duration.ofSeconds(5)).ignoreExceptions().untilAsserted(() ->
                assertThat(replicaA.findRate("USD").block().rate()).isEqualByComparingTo(USD_RATE));
    }

    private ExchangeRateService startReplica() {
        return new ExchangeRateService(rateProvider, exchangeRateRedisTemplate, clock, ticker, CURRENCIES);
    }

    private ExchangeRate snapshot(String currency) {
        return exchangeRateRedisTemplate.opsForValue().get(ExchangeRateService.SNAPSHOT_KEY_PREFIX + currency).block();
    }
}
