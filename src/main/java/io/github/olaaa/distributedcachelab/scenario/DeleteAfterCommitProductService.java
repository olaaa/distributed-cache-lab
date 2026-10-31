package io.github.olaaa.distributedcachelab.scenario;

import java.math.BigDecimal;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductRepository;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 5. Обычная связка Spring Cache: @Cacheable при чтении и @CacheEvict при изменении.
 *
 * UPDATE выполняется без явной транзакции и коммитится сразу, а @CacheEvict удаляет ключ из Redis
 * после того, как Mono изменения завершился, — то есть после коммита. Это лучший порядок из простых,
 * но гонку он не закрывает: чтение, которое прочитало старую цену до коммита, а записало её в Redis
 * после удаления, вернёт старую цену в кэш до конца срока жизни.
 */
@Service
public class DeleteAfterCommitProductService {

    public static final String CACHE_NAME = "productsDeleteAfterCommit";

    private final ProductSource source;
    private final ProductRepository repository;
    private final CacheWritePause cacheWritePause;

    public DeleteAfterCommitProductService(ProductSource source, ProductRepository repository,
                                           CacheWritePause cacheWritePause) {
        this.source = source;
        this.repository = repository;
        this.cacheWritePause = cacheWritePause;
    }

    /*
     * Значение попадает в Redis, когда Mono этого метода выдаст товар. Пауза стоит после чтения
     * из базы, поэтому запись в Redis откладывается на её длину.
     */
    @Cacheable(CACHE_NAME)
    public Mono<Product> findById(long id) {
        return source.load(id).flatMap(cacheWritePause::before);
    }

    @CacheEvict(cacheNames = CACHE_NAME, key = "#id")
    public Mono<Boolean> changePrice(long id, BigDecimal price) {
        return repository.updatePrice(id, price).map(updatedRows -> updatedRows > 0);
    }
}
