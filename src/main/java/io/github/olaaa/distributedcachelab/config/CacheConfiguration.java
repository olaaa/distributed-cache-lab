package io.github.olaaa.distributedcachelab.config;

import java.time.Duration;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.scenario.RedisCacheableProductService;
import org.springframework.boot.cache.autoconfigure.RedisCacheManagerBuilderCustomizer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;

@Configuration(proxyBeanMethods = false)
@EnableCaching // включаем перехват
public class CacheConfiguration {

    public static final Duration PRODUCTS_TTL = Duration.ofMinutes(10);

    /*
     * Настройка кэша products (сценарий 1). Остальные кэши получают настройки Spring Boot
     * по умолчанию: JDK-сериализация, вечный срок жизни, null кэшируется.
     *
     * - JacksonJsonRedisSerializer с конкретным типом Product: значение в Redis — читаемый JSON
     *   без служебного поля с именем класса, а при чтении Jackson создаёт только Product.
     * - entryTtl: без него запись в Redis живёт вечно.
     * - disableCachingNullValues: пустой результат не кэшируется (отрицательный кэш — отдельная тема).
     */
    public static RedisCacheConfiguration productsCacheConfiguration() {
        return RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(PRODUCTS_TTL)
                .disableCachingNullValues()
                .serializeValuesWith(SerializationPair.fromSerializer(new JacksonJsonRedisSerializer<>(Product.class)));
    }

    /* Spring Boot сам создаёт RedisCacheManager; customizer добавляет к нему настройку кэша products. */
    @Bean
    RedisCacheManagerBuilderCustomizer productsCacheCustomizer() {
        return builder -> builder.withCacheConfiguration(RedisCacheableProductService.CACHE_NAME,
                productsCacheConfiguration());
    }
}
