package io.github.olaaa.distributedcachelab.scenario;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 2. Тот же @Cacheable, что в сценарии 1, но кэш productsJittered: срок жизни
 * каждой записи — базовый плюс случайная добавка (JitteredTtlCacheConfiguration).
 */
@Service
public class JitteredTtlProductService {

    public static final String CACHE_NAME = "productsJittered";

    private final ProductSource source;

    public JitteredTtlProductService(ProductSource source) {
        this.source = source;
    }

    @Cacheable(CACHE_NAME)
    public Mono<Product> findById(long id) {
        return source.load(id);
    }
}
