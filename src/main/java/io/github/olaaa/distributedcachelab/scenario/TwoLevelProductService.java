package io.github.olaaa.distributedcachelab.scenario;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import io.github.olaaa.distributedcachelab.config.CacheConfiguration;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 3. Двухуровневый кэш: сначала локальный Caffeine (L1), при промахе — Redis (L2),
 * при промахе и там — источник, после чего значение кладётся в оба уровня.
 *
 * L1 убирает сетевой запрос в Redis на повторных чтениях. Цена — локальная копия может
 * отставать от Redis: об изменении, сделанном другой репликой, эта реплика не узнает,
 * пока не истечёт LOCAL_TTL. Поэтому срок первого уровня короткий. Как сообщить репликам
 * об изменении — сценарий 4.
 */
@Service
public class TwoLevelProductService implements LabCache {

    public static final String REDIS_KEY_PREFIX = "productsTwoLevel::";
    public static final Duration LOCAL_TTL = Duration.ofSeconds(30);
    public static final Duration REDIS_TTL = CacheConfiguration.PRODUCTS_TTL;

    private final ProductSource source;
    private final ReactiveRedisTemplate<String, Product> redis;
    private final AsyncCache<Long, Product> localCache;
    private final AtomicInteger redisReads = new AtomicInteger();

    public TwoLevelProductService(ProductSource source,
                                  ReactiveRedisTemplate<String, Product> productRedisTemplate,
                                  Ticker cacheTicker) {
        this.source = source;
        this.redis = productRedisTemplate;
        this.localCache = Caffeine.newBuilder()
                .ticker(cacheTicker)
                .maximumSize(10_000)
                .expireAfterWrite(LOCAL_TTL)
                .buildAsync();
    }

    public Mono<Product> findById(long id) {
        /*
         * Как в caching-lab: лямбда в fromFuture — обращение к кэшу при подписке, а не при сборке;
         * AsyncCache кладёт future загрузки сразу, и одновременные промахи на этой реплике
         * ждут одну загрузку — в Redis и в источник идёт один запрос.
         */
        return Mono.fromFuture(() -> localCache.get(id,
                        (key, executor) -> loadFromRedisOrSource(key).toFuture()),
                true);
    }

    private Mono<Product> loadFromRedisOrSource(long id) {
        String redisKey = REDIS_KEY_PREFIX + id;
        /*
         * Mono.defer внутри switchIfEmpty: без него source.load(id) вызывался бы при сборке цепочки,
         * даже когда товар нашёлся в Redis (ловушка switchIfEmpty из разбора про Reactor).
         */
        return Mono.defer(() -> {
                    redisReads.incrementAndGet();
                    return redis.opsForValue().get(redisKey);
                })
                .switchIfEmpty(Mono.defer(() -> source.load(id)
                        .flatMap(product -> redis.opsForValue().set(redisKey, product, REDIS_TTL)
                                .thenReturn(product))));
    }

    /* Сколько раз эта реплика ходила в Redis: каждое обращение — сетевой запрос. */
    public int redisReadCount() {
        return redisReads.get();
    }

    /* Возраст записи первого уровня; пока загрузка не завершилась, возраста нет. */
    public Optional<Duration> localAgeOf(long id) {
        return localCache.synchronous().policy().expireAfterWrite()
                .flatMap(expiration -> expiration.ageOf(id));
    }

    /* Сброс лаборатории: очистить первый уровень и счётчик обращений к Redis. */
    @Override
    public void invalidateAll() {
        localCache.synchronous().invalidateAll();
        redisReads.set(0);
    }
}
