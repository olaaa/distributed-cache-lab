package io.github.olaaa.distributedcachelab.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.function.LongFunction;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.config.CacheConfiguration;
import io.github.olaaa.distributedcachelab.config.JitteredTtlCacheConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.cache.RedisCacheWriter.TtlFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/* Сценарий 2. TTL с разбросом: записи, загруженные вместе, не должны вместе и истечь. */
class Scenario2TtlJitterTest extends ScenarioTestSupport {

    /* Прогрев: 100 товаров загружаются в кэш одновременно, как при старте реплики или после очистки Redis. */
    private static final int WARMED_UP_PRODUCTS = 100;

    @Autowired
    RedisCacheableProductService fixedTtlProductService;

    @Autowired
    JitteredTtlProductService jitteredTtlProductService;

    @BeforeEach
    void addProducts() {
        Flux.range(2, WARMED_UP_PRODUCTS - 1)
                .concatMap(id -> repository.save(new Product(id, "Товар " + id, new BigDecimal("1000.00"))))
                .then()
                .block();
    }

    @Test
    void fixedTtlEntriesLoadedTogetherExpireTogether() {
        warmUp(fixedTtlProductService::findById);

        List<Duration> remaining = remainingTimeToLive(RedisCacheableProductService.CACHE_NAME);
        assertThat(source.callCount()).isEqualTo(WARMED_UP_PRODUCTS);
        assertThat(spread(remaining))
                .as("одинаковый срок: все 100 записей истекут в пределах одной секунды")
                .isLessThan(Duration.ofSeconds(1));
    }

    @Test
    void jitteredTtlSpreadsExpirationOverJitterWindow() {
        warmUp(jitteredTtlProductService::findById);

        List<Duration> remaining = remainingTimeToLive(JitteredTtlProductService.CACHE_NAME);
        assertThat(remaining).allSatisfy(timeToLive -> assertThat(timeToLive)
                .isGreaterThan(JitteredTtlCacheConfiguration.BASE_TTL.minusSeconds(5))
                .isLessThanOrEqualTo(JitteredTtlCacheConfiguration.BASE_TTL.plus(JitteredTtlCacheConfiguration.MAX_JITTER)));
        assertThat(spread(remaining))
                .as("сроки разошлись больше чем на половину окна разброса")
                .isGreaterThan(JitteredTtlCacheConfiguration.MAX_JITTER.dividedBy(2));
        assertThat(maxExpiringWithinOneSecond(remaining))
                .as("в любую секунду истекает лишь несколько записей, а не все 100 разом")
                .isLessThanOrEqualTo(10);
    }

    @Test
    void jitteredTtlStaysWithinBounds() {
        TtlFunction jitteredTtl = JitteredTtlCacheConfiguration.jitteredTtl();
        for (int sampleNumber = 0; sampleNumber < 10_000; sampleNumber++) {
            assertThat(jitteredTtl.getTimeToLive(KETTLE_ID, null))
                    .isGreaterThanOrEqualTo(CacheConfiguration.PRODUCTS_TTL)
                    .isLessThanOrEqualTo(CacheConfiguration.PRODUCTS_TTL.plus(JitteredTtlCacheConfiguration.MAX_JITTER));
        }
    }

    private static void warmUp(LongFunction<Mono<Product>> reader) {
        /* flatMap подписывается на все чтения сразу: загрузки идут одновременно. */
        Flux.range(1, WARMED_UP_PRODUCTS)
                .flatMap(id -> reader.apply(id), WARMED_UP_PRODUCTS)
                .then()
                .block();
    }

    /* Сколько осталось жить каждому ключу — прямо из Redis, командой PTTL. */
    private List<Duration> remainingTimeToLive(String cacheName) {
        return Flux.range(1, WARMED_UP_PRODUCTS)
                .concatMap(id -> redis.getExpire(cacheName + "::" + id))
                .collectList()
                .block();
    }

    private static Duration spread(List<Duration> remaining) {
        List<Duration> sorted = remaining.stream().sorted().toList();
        return sorted.getLast().minus(sorted.getFirst());
    }

    /* Наибольшее число ключей, сроки которых укладываются в одно окно длиной в секунду. */
    private static int maxExpiringWithinOneSecond(List<Duration> remaining) {
        List<Duration> sorted = remaining.stream().sorted().toList();
        int maxInWindow = 0;
        int windowStart = 0;
        for (int windowEnd = 0; windowEnd < sorted.size(); windowEnd++) {
            while (sorted.get(windowEnd).minus(sorted.get(windowStart)).compareTo(Duration.ofSeconds(1)) >= 0) {
                windowStart++;
            }
            maxInWindow = Math.max(maxInWindow, windowEnd - windowStart + 1);
        }
        return maxInWindow;
    }
}
