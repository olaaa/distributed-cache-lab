package io.github.olaaa.distributedcachelab.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.config.CacheConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import reactor.test.StepVerifier;

/* Сценарий 1. RedisCacheManager: @Cacheable на Mono с кэшем в Redis, сериализация и срок жизни. */
class Scenario1RedisCacheManagerTest extends ScenarioTestSupport {

    /* Ключ в Redis: имя кэша, два двоеточия и ключ кэша — так по умолчанию строит CacheKeyPrefix.simple(). */
    private static final String KETTLE_KEY = "products::1";

    @Autowired
    RedisCacheableProductService redisCacheableProductService;

    @Autowired
    DefaultConfigProductService defaultConfigProductService;

    @Autowired
    RedisConnectionFactory connectionFactory;

    @Test
    void cacheableOnMonoStoresValueInRedis() {
        for (int readNumber = 0; readNumber < 5; readNumber++) {
            assertThat(redisCacheableProductService.findById(KETTLE_ID).block()).isNotNull();
        }

        assertThat(source.callCount()).isEqualTo(1);
        assertThat(redis.hasKey(KETTLE_KEY).block())
                .as("значение лежит в Redis, а не в памяти приложения")
                .isTrue();
    }

    @Test
    void valueIsStoredAsReadableJson() {
        redisCacheableProductService.findById(KETTLE_ID).block();

        assertThat(redis.opsForValue().get(KETTLE_KEY).block())
                .as("JacksonJsonRedisSerializer: обычный JSON, без имени класса")
                .isEqualTo("{\"id\":1,\"name\":\"Чайник\",\"price\":2490.00}");
    }

    @Test
    void entryExpiresAfterConfiguredTimeToLive() {
        redisCacheableProductService.findById(KETTLE_ID).block();

        Duration remaining = redis.getExpire(KETTLE_KEY).block();
        assertThat(remaining)
                .as("срок жизни задан в CacheConfiguration; без entryTtl запись жила бы вечно")
                .isPositive()
                .isLessThanOrEqualTo(CacheConfiguration.PRODUCTS_TTL)
                .isGreaterThan(CacheConfiguration.PRODUCTS_TTL.minusMinutes(1));
    }

    @Test
    void anotherReplicaReadsCachedValueWithoutSource() {
        redisCacheableProductService.findById(KETTLE_ID).block();
        assertThat(source.callCount()).isEqualTo(1);

        /* Другая реплика сервиса: свой RedisCacheManager с той же настройкой, но тот же Redis. */
        RedisCacheManager anotherReplica = RedisCacheManager.builder(connectionFactory)
                .withCacheConfiguration(RedisCacheableProductService.CACHE_NAME,
                        CacheConfiguration.productsCacheConfiguration())
                .build();
        anotherReplica.afterPropertiesSet();
        Cache productsOnAnotherReplica = anotherReplica.getCache(RedisCacheableProductService.CACHE_NAME);

        Product cached = productsOnAnotherReplica.get(KETTLE_ID, Product.class);
        assertThat(cached).isNotNull();
        assertThat(cached.name()).isEqualTo("Чайник");
        assertThat(cached.price()).isEqualByComparingTo(KETTLE_PRICE);
        assertThat(source.callCount())
                .as("вторая реплика получила товар из Redis, не обращаясь к источнику")
                .isEqualTo(1);
    }

    @Test
    void defaultJdkSerializationBreaksReadOfNonSerializableRecord() {
        /*
         * Кэш productsDefault не настроен: Spring Boot даёт ему JdkSerializationRedisSerializer.
         * Product — record без Serializable, сериализатор от него отказывается, и запись в кэш
         * падает. Ошибка доходит до клиента, хотя товар из БД уже прочитан.
         */
        StepVerifier.create(defaultConfigProductService.findById(KETTLE_ID))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Cannot serialize value of type")
                        .hasMessageContaining("Product"))
                .verify(Duration.ofSeconds(5));

        assertThat(source.callCount()).isEqualTo(1);
        assertThat(redis.hasKey("productsDefault::1").block()).isFalse();
    }
}
