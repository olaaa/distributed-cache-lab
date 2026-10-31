package io.github.olaaa.distributedcachelab.scenario;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductRepository;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import io.github.olaaa.distributedcachelab.config.CacheConfiguration;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/*
 * Сценарий 4. Двухуровневый кэш из сценария 3 плюс рассылка инвалидации между репликами.
 *
 * Реплика, которая меняет цену, после коммита удаляет значение из Redis и из своего Caffeine
 * и публикует id товара в канал Redis Pub/Sub. Каждая реплика подписана на этот канал и,
 * получив сообщение, удаляет товар из своего Caffeine. Следующее чтение на любой реплике
 * пройдёт мимо обоих уровней в источник и получит новую цену — без ожидания LOCAL_TTL.
 *
 * Pub/Sub доставляет сообщение не больше одного раза: реплика, которая в момент рассылки
 * была отключена от Redis, сообщение потеряет навсегда. Поэтому короткий LOCAL_TTL остаётся
 * страховкой: потерянная инвалидация исправится сама, когда истечёт локальная копия.
 */
@Service
public class BroadcastInvalidationProductService implements LabCache, DisposableBean {

    public static final String REDIS_KEY_PREFIX = "productsBroadcast::";
    public static final ChannelTopic INVALIDATION_TOPIC = ChannelTopic.of("products:invalidated");
    public static final Duration LOCAL_TTL = Duration.ofSeconds(30);
    public static final Duration REDIS_TTL = CacheConfiguration.PRODUCTS_TTL;

    /* Итог изменения цены: нашёлся ли товар и сколько реплик получили инвалидацию (ответ PUBLISH). */
    public record PriceChangeResult(boolean updated, long notifiedReplicas) {
    }

    private final ProductSource source;
    private final ProductRepository repository;
    private final ReactiveRedisTemplate<String, Product> redis;
    private final ReactiveStringRedisTemplate stringRedis;
    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private final AsyncCache<Long, Product> localCache;
    private final Disposable subscription;
    private final AtomicInteger invalidationsReceived = new AtomicInteger();
    private volatile boolean subscribed;

    public BroadcastInvalidationProductService(ProductSource source,
                                               ProductRepository repository,
                                               ReactiveRedisTemplate<String, Product> productRedisTemplate,
                                               ReactiveStringRedisTemplate stringRedisTemplate,
                                               ReactiveRedisConnectionFactory connectionFactory,
                                               Ticker cacheTicker) {
        this.source = source;
        this.repository = repository;
        this.redis = productRedisTemplate;
        this.stringRedis = stringRedisTemplate;
        this.localCache = Caffeine.newBuilder()
                .ticker(cacheTicker)
                .maximumSize(10_000)
                .expireAfterWrite(LOCAL_TTL)
                .buildAsync();
        /*
         * Свой контейнер — своё соединение для подписки, как у отдельного процесса реплики:
         * контейнер держит одно соединение на всё время жизни.
         * receiveLater отдаёт поток сообщений, когда подписка на канал уже действует, — с этого
         * момента реплика гарантированно получит следующие рассылки.
         */
        this.listenerContainer = new ReactiveRedisMessageListenerContainer(connectionFactory);
        this.subscription = listenerContainer.receiveLater(INVALIDATION_TOPIC)
                .doOnNext(messages -> subscribed = true)
                .flatMapMany(messages -> messages)
                .subscribe(message -> onInvalidation(Long.parseLong(message.getMessage())));
    }

    public Mono<Product> findById(long id) {
        return Mono.fromFuture(() -> localCache.get(id,
                        (key, executor) -> loadFromRedisOrSource(key).toFuture()),
                true);
    }

    private Mono<Product> loadFromRedisOrSource(long id) {
        String redisKey = REDIS_KEY_PREFIX + id;
        return redis.opsForValue().get(redisKey)
                .switchIfEmpty(Mono.defer(() -> source.load(id)
                        .flatMap(product -> redis.opsForValue().set(redisKey, product, REDIS_TTL)
                                .thenReturn(product))));
    }

    /*
     * Порядок: сначала база (UPDATE без явной транзакции коммитится сразу), потом удалить из Redis,
     * потом из своего Caffeine, потом разослать остальным. Когда реплика получит сообщение,
     * в Redis старого значения уже нет, и её следующее чтение дойдёт до базы.
     * Команды ReactiveRedisTemplate ленивые: delete и convertAndSend уйдут в Redis при подписке,
     * по очереди, поэтому вызывать их в аргументах then безопасно.
     */
    public Mono<PriceChangeResult> changePrice(long id, BigDecimal price) {
        return repository.updatePrice(id, price)
                .flatMap(updatedRows -> {
                    if (updatedRows == 0) {
                        return Mono.just(new PriceChangeResult(false, 0));
                    }
                    return redis.delete(REDIS_KEY_PREFIX + id)
                            .then(Mono.fromRunnable(() -> localCache.synchronous().invalidate(id)))
                            .then(stringRedis.convertAndSend(INVALIDATION_TOPIC.getTopic(), String.valueOf(id)))
                            .map(notifiedReplicas -> new PriceChangeResult(true, notifiedReplicas));
                });
    }

    private void onInvalidation(long id) {
        localCache.synchronous().invalidate(id);
        invalidationsReceived.incrementAndGet();
    }

    /* Подписка на канал инвалидации уже действует. */
    public boolean isSubscribed() {
        return subscribed;
    }

    /* Сколько сообщений об инвалидации получила эта реплика, включая свои собственные. */
    public int invalidationsReceived() {
        return invalidationsReceived.get();
    }

    /* Возраст записи первого уровня; пока загрузка не завершилась, возраста нет. */
    public Optional<Duration> localAgeOf(long id) {
        return localCache.synchronous().policy().expireAfterWrite()
                .flatMap(expiration -> expiration.ageOf(id));
    }

    /* Отключить реплику от канала: отписаться и закрыть соединение подписки (в тесте — обрыв связи). */
    public void disconnect() {
        subscribed = false;
        subscription.dispose();
        listenerContainer.destroy();
    }

    @Override
    public void destroy() {
        disconnect();
    }

    /* Сброс лаборатории: очистить первый уровень и счётчик полученных инвалидаций. */
    @Override
    public void invalidateAll() {
        localCache.synchronous().invalidateAll();
        invalidationsReceived.set(0);
    }
}
