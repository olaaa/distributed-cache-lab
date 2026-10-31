package io.github.olaaa.distributedcachelab.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.function.LongFunction;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.SourceUnavailableException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/* Сценарий 6. Лавина промахов: @Cacheable(sync = true) с Redis и аренда с ожиданием. */
class Scenario6StampedeTest extends ScenarioTestSupport {

    private static final int CONCURRENT_READS = 50;
    private static final int REPLICAS = 3;
    private static final String SYNC_KEY = SyncCacheableProductService.CACHE_NAME + "::" + KETTLE_ID;
    private static final String STAMPEDE_VALUE_KEY = StampedeLeaseProductService.KEY_PREFIX + KETTLE_ID;
    private static final String STAMPEDE_LEASE_KEY = StampedeLeaseProductService.LEASE_KEY_PREFIX + KETTLE_ID;

    @Autowired
    SyncCacheableProductService syncCacheable;

    /* Бин сервиса — «реплика A». */
    @Autowired
    StampedeLeaseProductService replicaA;

    @Autowired
    ReactiveRedisTemplate<String, Product> productRedisTemplate;

    @Test
    void syncCacheableWithRedisLetsEveryConcurrentMissThrough() {
        List<Product> products = readConcurrently(syncCacheable::findById);

        assertThat(products).hasSize(CONCURRENT_READS);
        assertThat(source.callCount())
                .as("RedisCache.retrieve не объединяет промахи: каждый одновременный промах идёт в источник")
                .isEqualTo(CONCURRENT_READS);
        assertThat(hasKey(SYNC_KEY)).isTrue();
    }

    @Test
    void leaseWithWaitingLoadsOnceForConcurrentMissesOnSeveralReplicas() {
        List<StampedeLeaseProductService> replicas = List.of(replicaA, startReplica(), startReplica());

        /* Три реплики, на каждой — 50 одновременных чтений одного товара. */
        List<Product> products = Flux.fromIterable(replicas)
                .flatMap(replica -> Flux.range(0, CONCURRENT_READS)
                        .flatMap(readNumber -> replica.findById(KETTLE_ID), CONCURRENT_READS), REPLICAS)
                .collectList()
                .block();

        assertThat(products).hasSize(REPLICAS * CONCURRENT_READS)
                .allSatisfy(product -> assertThat(product.price()).isEqualByComparingTo(KETTLE_PRICE));
        assertThat(source.callCount())
                .as("аренду взяло одно чтение на одной реплике, остальные 149 дождались значения в Redis")
                .isEqualTo(1);
        assertThat(replicas.stream().mapToInt(StampedeLeaseProductService::waitSteps).sum())
                .as("цена ожидания — повторные обращения к Redis")
                .isPositive();
        assertThat(replicas.stream().mapToInt(StampedeLeaseProductService::fallbackLoads).sum()).isZero();
        assertThat(hasKey(STAMPEDE_VALUE_KEY)).isTrue();
        assertThat(hasKey(STAMPEDE_LEASE_KEY)).as("после записи аренда снята").isFalse();
    }

    @Test
    void waitersLoadOnceAfterOrphanedLeaseExpires() {
        /* Аренду держит реплика, которая упала, не записав значение: ключ аренды есть, значения нет. */
        Duration orphanedLeaseTtl = Duration.ofMillis(500);
        long startNanos = System.nanoTime();
        redis.opsForValue().set(STAMPEDE_LEASE_KEY, "упавшая реплика", orphanedLeaseTtl).block();

        List<Product> products = readConcurrently(replicaA::findById);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

        assertThat(products).hasSize(CONCURRENT_READS);
        assertThat(elapsed)
                .as("чтения ждали, пока истечёт чужая аренда")
                .isGreaterThanOrEqualTo(orphanedLeaseTtl);
        assertThat(source.callCount())
                .as("после истечения аренду взяло одно чтение, остальные дождались его значения")
                .isEqualTo(1);
        assertThat(replicaA.fallbackLoads()).isZero();
    }

    @Test
    void failedLoadReleasesLeaseAtOnce() {
        source.setFailing(true);

        assertThatThrownBy(() -> replicaA.findById(KETTLE_ID).block())
                .isInstanceOf(SourceUnavailableException.class);
        assertThat(hasKey(STAMPEDE_LEASE_KEY))
                .as("аренда снята сразу: ждущим не нужно стоять до её истечения")
                .isFalse();
    }

    /* Ещё одна реплика: свой экземпляр сервиса, тот же Redis и та же база. */
    private StampedeLeaseProductService startReplica() {
        return new StampedeLeaseProductService(source, productRedisTemplate, redis);
    }

    /* flatMap подписывается на все чтения сразу: промахи случаются одновременно. */
    private static List<Product> readConcurrently(LongFunction<Mono<Product>> reader) {
        return Flux.range(0, CONCURRENT_READS)
                .flatMap(readNumber -> reader.apply(KETTLE_ID), CONCURRENT_READS)
                .collectList()
                .block();
    }

    private boolean hasKey(String key) {
        return Boolean.TRUE.equals(redis.hasKey(key).block());
    }
}
