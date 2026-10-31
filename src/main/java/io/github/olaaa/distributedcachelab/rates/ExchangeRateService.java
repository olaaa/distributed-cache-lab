package io.github.olaaa.distributedcachelab.rates;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import io.github.olaaa.distributedcachelab.scenario.LabCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
 * Сценарий 7. Курсы валют: данные, которые обновляются по расписанию поставщика и допускают устаревание.
 *
 * - Локальный AsyncLoadingCache с refreshAfterWrite: через REFRESH_AFTER курс перезагружается в фоне
 *   при следующем обращении, а само обращение сразу получает прежний курс.
 * - Каждый полученный курс сохраняется в Redis как последний известный (rates:last::<валюта>, без срока
 *   жизни). Поставщик недоступен — загрузчик берёт курс оттуда. Это выручает реплику, у которой
 *   локального кэша ещё нет: новую или перезапущенную.
 * - Курс старше MAX_AGE не отдаётся: устаревание допустимо, но не бесконечно.
 * - Прогрев (warmUp) загружает все валюты до того, как приложение начнёт принимать запросы.
 */
@Service
public class ExchangeRateService implements LabCache {

    public static final Duration REFRESH_AFTER = Duration.ofMinutes(5);
    public static final Duration MAX_AGE = Duration.ofDays(1);
    public static final String SNAPSHOT_KEY_PREFIX = "rates:last::";

    private final RateProvider provider;
    private final ReactiveRedisTemplate<String, ExchangeRate> snapshots;
    private final Clock clock;
    private final List<String> currencies;
    private final AsyncLoadingCache<String, ExchangeRate> rates;

    public ExchangeRateService(RateProvider provider,
                               ReactiveRedisTemplate<String, ExchangeRate> exchangeRateRedisTemplate,
                               Clock clock, Ticker cacheTicker,
                               @Value("${rates.currencies}") List<String> currencies) {
        this.provider = provider;
        this.snapshots = exchangeRateRedisTemplate;
        this.clock = clock;
        this.currencies = currencies;
        this.rates = Caffeine.newBuilder()
                .ticker(cacheTicker)
                .refreshAfterWrite(REFRESH_AFTER)
                .buildAsync(new AsyncCacheLoader<String, ExchangeRate>() {
                    @Override
                    public CompletableFuture<ExchangeRate> asyncLoad(String currency, Executor executor) {
                        return load(currency).toFuture();
                    }

                    @Override
                    public CompletableFuture<ExchangeRate> asyncReload(String currency, ExchangeRate oldRate,
                                                                       Executor executor) {
                        return reload(currency, oldRate).toFuture();
                    }
                });
    }

    /*
     * Курс для ответа. Если он старше MAX_AGE — ошибка: поставщик давно недоступен,
     * и считать по такому курсу нельзя.
     */
    public Mono<ExchangeRate> findRate(String currency) {
        return Mono.fromFuture(() -> rates.get(currency), true)
                .flatMap(rate -> {
                    Duration age = Duration.between(rate.asOf(), clock.instant());
                    return age.compareTo(MAX_AGE) > 0
                            ? Mono.error(new StaleRateException(currency, age))
                            : Mono.just(rate);
                });
    }

    /* Прогрев: загрузить все валюты. Недоступные — пропустить: приложение стартует и без них. */
    public Mono<Long> warmUp() {
        return Flux.fromIterable(currencies)
                .flatMap(currency -> Mono.fromFuture(() -> rates.get(currency), true)
                        .onErrorResume(error -> Mono.empty()))
                .count();
    }

    /*
     * Первая загрузка: локального курса ещё нет. Поставщик недоступен — берём последний известный
     * из Redis. Нет и его — ошибка, ответа нет.
     */
    private Mono<ExchangeRate> load(String currency) {
        return fetchAndRemember(currency)
                .onErrorResume(RateProviderUnavailableException.class, unavailable ->
                        lastKnown(currency).switchIfEmpty(Mono.error(unavailable)));
    }

    /*
     * Фоновое обновление: локальный курс есть. Поставщик недоступен — последний известный из Redis
     * подходит, только если он новее нашего: его могла получить другая реплика. Иначе — ошибка,
     * и Caffeine оставит прежнее значение. Вернуть тот же старый курс как «успех» нельзя: Caffeine
     * засчитал бы обновление и не пробовал бы снова ещё REFRESH_AFTER.
     */
    private Mono<ExchangeRate> reload(String currency, ExchangeRate oldRate) {
        return fetchAndRemember(currency)
                .onErrorResume(RateProviderUnavailableException.class, unavailable ->
                        lastKnown(currency)
                                .filter(lastKnown -> lastKnown.asOf().isAfter(oldRate.asOf()))
                                .switchIfEmpty(Mono.error(unavailable)));
    }

    /* Свежий курс от поставщика — и сразу в Redis как последний известный. */
    private Mono<ExchangeRate> fetchAndRemember(String currency) {
        return provider.fetch(currency)
                .flatMap(rate -> snapshots.opsForValue().set(SNAPSHOT_KEY_PREFIX + currency, rate).thenReturn(rate));
    }

    private Mono<ExchangeRate> lastKnown(String currency) {
        return snapshots.opsForValue().get(SNAPSHOT_KEY_PREFIX + currency);
    }

    /* Сколько прошло с последней записи курса в локальный кэш; пока загрузка не завершилась, возраста нет. */
    public Optional<Duration> localAgeOf(String currency) {
        return rates.synchronous().policy().refreshAfterWrite()
                .flatMap(refresh -> refresh.ageOf(currency));
    }

    /*
     * Сброс лаборатории: отменить фоновые обновления, которые ещё идут, и очистить кэш.
     * Без отмены обновление, начатое в прошлом тесте, могло бы дописать курс уже в следующем.
     */
    @Override
    public void invalidateAll() {
        rates.synchronous().policy().refreshes().values().forEach(refresh -> refresh.cancel(true));
        rates.synchronous().invalidateAll();
    }
}
