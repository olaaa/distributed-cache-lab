package io.github.olaaa.distributedcachelab.scenario;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 1, ловушка: тот же @Cacheable, но кэш productsDefault не настроен, и Spring Boot
 * создаёт его с настройками по умолчанию — JDK-сериализацией. Product — record без Serializable,
 * поэтому запись в кэш падает, и ошибка доходит до клиента, хотя товар из БД уже прочитан.
 */
@Service
public class DefaultConfigProductService {

    public static final String CACHE_NAME = "productsDefault";

    private final ProductSource source;

    public DefaultConfigProductService(ProductSource source) {
        this.source = source;
    }

    @Cacheable(CACHE_NAME)
    public Mono<Product> findById(long id) {
        return source.load(id);
    }
}
