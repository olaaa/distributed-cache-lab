package io.github.olaaa.distributedcachelab.config;

import java.time.Clock;

import io.github.olaaa.distributedcachelab.rates.ExchangeRate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/* Сценарий 7. Часы приложения и доступ к последним известным курсам в Redis. */
@Configuration(proxyBeanMethods = false)
public class RatesConfiguration {

    /* Часы, по которым поставщик ставит asOf и сервис считает возраст курса. В тестах — от ManualTicker. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /* Курс в Redis — JSON: {"currency":"USD","rate":92.50,"asOf":"…"}. */
    @Bean
    ReactiveRedisTemplate<String, ExchangeRate> exchangeRateRedisTemplate(ReactiveRedisConnectionFactory connectionFactory) {
        RedisSerializationContext<String, ExchangeRate> serializationContext = RedisSerializationContext
                .<String, ExchangeRate>newSerializationContext(new StringRedisSerializer())
                .value(new JacksonJsonRedisSerializer<>(ExchangeRate.class))
                .build();
        return new ReactiveRedisTemplate<>(connectionFactory, serializationContext);
    }
}
