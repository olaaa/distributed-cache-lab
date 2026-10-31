package io.github.olaaa.distributedcachelab.scenario;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import io.github.olaaa.distributedcachelab.config.CacheConfiguration;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 6. Одна загрузка на ключ для всех реплик: аренда из сценария 5 плюс ожидание.
 *
 * При промахе чтение пытается взять аренду. Взяло — загружает товар и пишет его в Redis, если аренда
 * своя. Не взяло — аренду держит другое чтение, может быть, на другой реплике: тогда ждёт WAIT_STEP
 * и снова смотрит в Redis. Так у Facebook: клиенты без аренды «wait a short amount of time and try again».
 */
@Service
public class StampedeLeaseProductService implements LabCache {

    public static final String KEY_PREFIX = "productsStampede::";
    public static final String LEASE_KEY_PREFIX = "productsStampede:lease::";
    public static final Duration REDIS_TTL = CacheConfiguration.PRODUCTS_TTL;
    public static final Duration LEASE_TTL = Duration.ofSeconds(10);
    /* Сколько ждёт чтение без аренды, прежде чем снова заглянуть в Redis. */
    public static final Duration WAIT_STEP = Duration.ofMillis(20);
    /*
     * Сколько ждать в сумме. Дольше — загрузить самому, без записи в Redis: держатель аренды, видимо,
     * завис, а ждать, пока истечёт его аренда, покупатель не должен.
     */
    public static final Duration MAX_WAIT = Duration.ofSeconds(5);

    /* Снять аренду, только если она своя: чужую, взятую после истечения нашей, не трогать. */
    private static final RedisScript<Long> RELEASE_LEASE = RedisScript.of("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final ProductSource source;
    private final ReactiveRedisTemplate<String, Product> products;
    private final ReactiveStringRedisTemplate strings;
    private final JacksonJsonRedisSerializer<Product> productSerializer = new JacksonJsonRedisSerializer<>(Product.class);
    private final AtomicInteger waitSteps = new AtomicInteger();
    private final AtomicInteger fallbackLoads = new AtomicInteger();

    public StampedeLeaseProductService(ProductSource source,
                                       ReactiveRedisTemplate<String, Product> productRedisTemplate,
                                       ReactiveStringRedisTemplate strings) {
        this.source = source;
        this.products = productRedisTemplate;
        this.strings = strings;
    }

    public Mono<Product> findById(long id) {
        return Mono.defer(() -> readOrLoad(id, System.nanoTime() + MAX_WAIT.toNanos()));
    }

    private Mono<Product> readOrLoad(long id, long waitDeadline) {
        return products.opsForValue().get(KEY_PREFIX + id)
                .switchIfEmpty(Mono.defer(() -> leaseOrWait(id, waitDeadline)));
    }

    private Mono<Product> leaseOrWait(long id, long waitDeadline) {
        String token = UUID.randomUUID().toString();
        return strings.opsForValue().setIfAbsent(LEASE_KEY_PREFIX + id, token, LEASE_TTL)
                .flatMap(leased -> {
                    if (leased) {
                        return recheckOrLoad(id, token);
                    }
                    if (System.nanoTime() >= waitDeadline) {
                        fallbackLoads.incrementAndGet();
                        return source.load(id);
                    }
                    waitSteps.incrementAndGet();
                    return Mono.delay(WAIT_STEP).then(Mono.defer(() -> readOrLoad(id, waitDeadline)));
                });
    }

    /*
     * Аренда взята — но между промахом и арендой другое чтение могло успеть загрузить товар, записать
     * его и снять свою аренду. Без перепроверки это чтение загрузило бы товар второй раз: тест на трёх
     * репликах так и ловил две загрузки вместо одной. Поэтому сначала ещё раз смотрим в Redis.
     */
    private Mono<Product> recheckOrLoad(long id, String token) {
        return products.opsForValue().get(KEY_PREFIX + id)
                .flatMap(product -> releaseLease(id, token).thenReturn(product))
                .switchIfEmpty(Mono.defer(() -> loadAndWrite(id, token)));
    }

    /*
     * Держатель аренды загружает товар и пишет его скриптом сценария 5: только если аренда своя.
     * Если загрузка упала или товара нет, аренда снимается сразу — иначе ждущие простояли бы до её истечения.
     */
    private Mono<Product> loadAndWrite(long id, String token) {
        return source.load(id)
                .flatMap(product -> writeIfLeaseHeld(id, token, product))
                .switchIfEmpty(Mono.defer(() -> releaseLease(id, token).then(Mono.empty())))
                .onErrorResume(error -> releaseLease(id, token).then(Mono.error(error)));
    }

    private Mono<Product> writeIfLeaseHeld(long id, String token, Product product) {
        String json = new String(productSerializer.serialize(product), StandardCharsets.UTF_8);
        return strings.execute(LeaseProductService.WRITE_IF_LEASE_HELD,
                        List.of(KEY_PREFIX + id, LEASE_KEY_PREFIX + id),
                        List.of(token, json, String.valueOf(REDIS_TTL.toMillis())))
                .then(Mono.just(product));
    }

    private Mono<Void> releaseLease(long id, String token) {
        return strings.execute(RELEASE_LEASE, List.of(LEASE_KEY_PREFIX + id), List.of(token)).then();
    }

    /* Сколько раз чтения без аренды ждали WAIT_STEP: цена ожидания — запросы в Redis. */
    public int waitSteps() {
        return waitSteps.get();
    }

    /* Сколько чтений не дождались значения за MAX_WAIT и загрузили товар сами. */
    public int fallbackLoads() {
        return fallbackLoads.get();
    }

    @Override
    public void invalidateAll() {
        waitSteps.set(0);
        fallbackLoads.set(0);
    }
}
