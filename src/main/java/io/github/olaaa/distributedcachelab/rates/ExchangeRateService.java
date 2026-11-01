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
 * - Курс старше MAX_AGE не отдаётся: устаревание допустимо, но не бесконечно. Такой курс сначала
 *   пробуем обновить, и только если не вышло — ошибка.
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
                    /*
                     * Первая загрузка: курса этой валюты в кэше нет. Caffeine вызывает его из get(currency)
                     * при промахе и из refresh(currency), если значения в кэше нет.
                     * Пока future не завершился, одновременные get той же валюты ждут его же, второй
                     * загрузки нет. Future завершился ошибкой — Caffeine убирает его из кэша, и следующий
                     * get загрузит заново. toFuture() подписывается сразу: загрузка начинается при вызове.
                     */
                    @Override
                    public CompletableFuture<ExchangeRate> asyncLoad(String currency, Executor executor) {
                        return load(currency).toFuture();
                    }

                    /*
                     * Фоновое обновление: курс в кэше есть, oldRate — он. Caffeine вызывает его:
                     * - из get(currency), когда с записи курса прошло больше REFRESH_AFTER: get сразу
                     *   отдаёт oldRate, а обновление идёт в фоне;
                     * - из refresh(currency), если обновление этой валюты ещё не идёт; если идёт —
                     *   refresh возвращает его future и сюда не заходит.
                     * Новый курс заменяет oldRate в кэше и запускает отсчёт REFRESH_AFTER заново. Ошибка —
                     * Caffeine оставляет oldRate и пишет ошибку в журнал; время записи не сдвигается,
                     * поэтому следующий же get снова запустит обновление.
                     */
                    @Override
                    public CompletableFuture<ExchangeRate> asyncReload(String currency, ExchangeRate oldRate,
                                                                       Executor executor) {
                        return reload(currency, oldRate).toFuture();
                    }
                });
    }

    /*
     * Курс для ответа. Обычно — из кэша сразу, даже если пора обновлять: обновление идёт в фоне.
     * Но курс старше MAX_AGE отдавать нельзя — тогда ждём обновления, как при промахе.
     */
    public Mono<ExchangeRate> findRate(String currency) {
        return Mono.fromFuture(() -> rates.get(currency), true)
                .flatMap(rate -> isFresh(rate) ? Mono.just(rate) : awaitRefresh(currency, rate));
    }

    /*
     * Курс в кэше старше MAX_AGE. Это не значит, что поставщик недоступен: может быть, к курсу просто
     * давно не обращались — ночью, в выходные, — и обновлять его было некому. Поэтому сначала ждём
     * обновления. refresh возвращает уже идущее обновление, если get его запустил, — второго запроса
     * к поставщику нет. Обновить не удалось или и новый курс слишком старый — StaleRateException, 503.
     */
    private Mono<ExchangeRate> awaitRefresh(String currency, ExchangeRate tooOld) {
        return Mono.fromFuture(() -> rates.synchronous().refresh(currency), true)
                .onErrorResume(refreshFailed -> Mono.empty())
                .defaultIfEmpty(tooOld)
                .flatMap(rate -> isFresh(rate)
                        ? Mono.just(rate)
                        : Mono.error(new StaleRateException(currency, Duration.between(rate.asOf(), clock.instant()))));
    }

    private boolean isFresh(ExchangeRate rate) {
        return Duration.between(rate.asOf(), clock.instant()).compareTo(MAX_AGE) <= 0;
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
