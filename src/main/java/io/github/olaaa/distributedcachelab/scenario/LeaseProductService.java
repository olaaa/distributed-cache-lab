package io.github.olaaa.distributedcachelab.scenario;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductRepository;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import io.github.olaaa.distributedcachelab.config.CacheConfiguration;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 5. Аренда (lease) против устаревшей записи — по образцу memcache в Facebook.
 *
 * При промахе чтение сначала берёт в Redis аренду — ключ с маркером, — и только потом читает базу.
 * Изменение после коммита удаляет и значение, и аренду. Записать значение в Redis можно, только
 * если аренда с твоим маркером ещё на месте; проверка и запись идут одним Lua-скриптом, атомарно.
 * Чтение, которое прочитало старую цену до коммита, к моменту записи аренду уже потеряло,
 * и его запись отклоняется.
 */
@Service
public class LeaseProductService implements LabCache {

    public static final String KEY_PREFIX = "productsLease::";
    public static final String LEASE_KEY_PREFIX = "productsLease:lease::";
    public static final Duration REDIS_TTL = CacheConfiguration.PRODUCTS_TTL;
    /* Срок аренды — с запасом больше обычной загрузки: если загрузка дольше, запись просто не состоится. */
    public static final Duration LEASE_TTL = Duration.ofSeconds(10);

    /*
     * KEYS[1] — ключ товара, KEYS[2] — ключ аренды; ARGV[1] — маркер аренды, ARGV[2] — товар в JSON,
     * ARGV[3] — срок жизни значения в миллисекундах. Записывает, только если аренда — своя.
     * Тот же скрипт использует сценарий 6.
     */
    static final RedisScript<Long> WRITE_IF_LEASE_HELD = RedisScript.of("""
            if redis.call('GET', KEYS[2]) == ARGV[1] then
                redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
                redis.call('DEL', KEYS[2])
                return 1
            end
            return 0
            """, Long.class);

    private final ProductSource source;
    private final ProductRepository repository;
    private final ReactiveRedisTemplate<String, Product> products;
    private final ReactiveStringRedisTemplate strings;
    private final CacheWritePause cacheWritePause;
    private final JacksonJsonRedisSerializer<Product> productSerializer = new JacksonJsonRedisSerializer<>(Product.class);
    private final AtomicInteger staleWritesRejected = new AtomicInteger();

    public LeaseProductService(ProductSource source, ProductRepository repository,
                               ReactiveRedisTemplate<String, Product> productRedisTemplate,
                               ReactiveStringRedisTemplate strings, CacheWritePause cacheWritePause) {
        this.source = source;
        this.repository = repository;
        this.products = productRedisTemplate;
        this.strings = strings;
        this.cacheWritePause = cacheWritePause;
    }

    public Mono<Product> findById(long id) {
        return products.opsForValue().get(KEY_PREFIX + id)
                .switchIfEmpty(Mono.defer(() -> loadWithLease(id)));
    }

    /*
     * Аренда берётся ДО чтения базы. Только так удаление после коммита гарантированно её снимет:
     * если чтение увидело старую цену, оно читало до коммита, а значит, и аренду взяло до удаления.
     * Кто не получил аренду (её держит другое чтение), загружает товар, но в Redis не пишет.
     */
    private Mono<Product> loadWithLease(long id) {
        String leaseKey = LEASE_KEY_PREFIX + id;
        String token = UUID.randomUUID().toString();
        return strings.opsForValue().setIfAbsent(leaseKey, token, LEASE_TTL)
                .flatMap(leased -> source.load(id)
                        .flatMap(cacheWritePause::before)
                        .flatMap(product -> leased
                                ? writeIfLeaseHeld(id, token, product)
                                : Mono.just(product)));
    }

    private Mono<Product> writeIfLeaseHeld(long id, String token, Product product) {
        String json = new String(productSerializer.serialize(product), StandardCharsets.UTF_8);
        return strings.execute(WRITE_IF_LEASE_HELD,
                        List.of(KEY_PREFIX + id, LEASE_KEY_PREFIX + id),
                        List.of(token, json, String.valueOf(REDIS_TTL.toMillis())))
                .next()
                .doOnNext(written -> {
                    if (written == 0) {
                        staleWritesRejected.incrementAndGet();
                    }
                })
                .thenReturn(product);
    }

    /* После коммита — удалить и значение, и аренду: одна команда DEL на два ключа. */
    public Mono<Boolean> changePrice(long id, BigDecimal price) {
        return repository.updatePrice(id, price)
                .flatMap(updatedRows -> updatedRows == 0
                        ? Mono.just(false)
                        : strings.delete(KEY_PREFIX + id, LEASE_KEY_PREFIX + id).thenReturn(true));
    }

    /* Сколько записей в Redis отклонено: аренду к моменту записи уже сняло изменение. */
    public int staleWritesRejected() {
        return staleWritesRejected.get();
    }

    @Override
    public void invalidateAll() {
        staleWritesRejected.set(0);
    }
}
