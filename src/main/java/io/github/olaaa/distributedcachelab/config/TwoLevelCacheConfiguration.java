package io.github.olaaa.distributedcachelab.config;

import com.github.benmanes.caffeine.cache.Ticker;
import io.github.olaaa.distributedcachelab.catalog.Product;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/* Сценарий 3. Двухуровневый кэш собран вручную: Caffeine — первый уровень, Redis — второй. */
@Configuration(proxyBeanMethods = false)
public class TwoLevelCacheConfiguration {

    /* Часы локального кэша. В тестах их подменяет ManualTicker: срок первого уровня двигается без sleep. */
    @Bean
    Ticker cacheTicker() {
        return Ticker.systemTicker();
    }

    /*
     * Доступ ко второму уровню мимо Spring Cache: ключ — строка, значение — Product в JSON,
     * тот же формат, что у кэша products в сценарии 1.
     */
    @Bean
    ReactiveRedisTemplate<String, Product> productRedisTemplate(ReactiveRedisConnectionFactory connectionFactory) {
        RedisSerializationContext<String, Product> serializationContext = RedisSerializationContext
                .<String, Product>newSerializationContext(new StringRedisSerializer())
                .value(new JacksonJsonRedisSerializer<>(Product.class))
                .build();
        return new ReactiveRedisTemplate<>(connectionFactory, serializationContext);
    }
}
