package io.github.olaaa.distributedcachelab.scenario;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 1. @Cacheable на Mono, кэш — Redis. Кэш products настроен в CacheConfiguration:
 * JSON вместо JDK-сериализации и срок жизни 10 минут.
 */
@Service
public class RedisCacheableProductService {

    public static final String CACHE_NAME = "products";

    private final ProductSource source;

    public RedisCacheableProductService(ProductSource source) {
        this.source = source;
    }

    @Cacheable(CACHE_NAME)
    public Mono<Product> findById(long id) {
        return source.load(id);
    }
}
