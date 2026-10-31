package io.github.olaaa.distributedcachelab.scenario;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 6. @Cacheable(sync = true), кэш — Redis.
 *
 * С Caffeine sync = true объединяет одновременные промахи (сценарий 9 caching-lab): AsyncCache держит
 * одну future загрузки на ключ. RedisCache.retrieve(key, valueLoader) такого не делает: читает ключ
 * и при промахе сразу вызывает загрузчик, без блокировки, — и каждый одновременный промах идёт в источник.
 */
@Service
public class SyncCacheableProductService {

    public static final String CACHE_NAME = "productsSync";

    private final ProductSource source;

    public SyncCacheableProductService(ProductSource source) {
        this.source = source;
    }

    @Cacheable(cacheNames = CACHE_NAME, sync = true)
    public Mono<Product> findById(long id) {
        return source.load(id);
    }
}
