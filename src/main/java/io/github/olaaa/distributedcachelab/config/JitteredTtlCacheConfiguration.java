package io.github.olaaa.distributedcachelab.config;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import io.github.olaaa.distributedcachelab.scenario.JitteredTtlProductService;
import org.springframework.boot.cache.autoconfigure.RedisCacheManagerBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheWriter.TtlFunction;

/* Сценарий 2. Кэш productsJittered: тот же JSON, что у products, но срок жизни с разбросом. */
@Configuration(proxyBeanMethods = false)
public class JitteredTtlCacheConfiguration {

    public static final Duration BASE_TTL = CacheConfiguration.PRODUCTS_TTL;
    public static final Duration MAX_JITTER = Duration.ofMinutes(2);

    /*
     * Срок жизни = базовый + случайная добавка от 0 до MAX_JITTER включительно.
     * Spring Data Redis вычисляет TtlFunction при каждой записи в кэш, поэтому записи,
     * загруженные в одну и ту же секунду, получают разные сроки и истекают в разное время.
     */
    public static TtlFunction jitteredTtl() {
        return (key, value) -> BASE_TTL.plusMillis(ThreadLocalRandom.current().nextLong(MAX_JITTER.toMillis() + 1));
    }

    public static RedisCacheConfiguration jitteredCacheConfiguration() {
        return CacheConfiguration.productsCacheConfiguration().entryTtl(jitteredTtl());
    }

    @Bean
    RedisCacheManagerBuilderCustomizer jitteredTtlCacheCustomizer() {
        return builder -> builder.withCacheConfiguration(JitteredTtlProductService.CACHE_NAME,
                jitteredCacheConfiguration());
    }
}
