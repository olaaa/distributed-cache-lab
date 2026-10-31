package io.github.olaaa.distributedcachelab.web;

import java.math.BigDecimal;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.scenario.BroadcastInvalidationProductService;
import io.github.olaaa.distributedcachelab.scenario.DefaultConfigProductService;
import io.github.olaaa.distributedcachelab.scenario.DeleteAfterCommitProductService;
import io.github.olaaa.distributedcachelab.scenario.JitteredTtlProductService;
import io.github.olaaa.distributedcachelab.scenario.LeaseProductService;
import io.github.olaaa.distributedcachelab.scenario.RedisCacheableProductService;
import io.github.olaaa.distributedcachelab.scenario.StampedeLeaseProductService;
import io.github.olaaa.distributedcachelab.scenario.SyncCacheableProductService;
import io.github.olaaa.distributedcachelab.scenario.TwoLevelProductService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/products")
public class ProductController {

    public enum ReadVia {
        REDIS, REDIS_DEFAULT_CONFIG, REDIS_JITTERED_TTL, TWO_LEVEL, BROADCAST, DELETE_AFTER_COMMIT, LEASE,
        REDIS_SYNC, STAMPEDE_LEASE
    }

    public enum ChangeVia {
        BROADCAST, DELETE_AFTER_COMMIT, LEASE
    }

    public record PriceChange(BigDecimal price) {
    }

    private final RedisCacheableProductService redisCacheableProductService;
    private final DefaultConfigProductService defaultConfigProductService;
    private final JitteredTtlProductService jitteredTtlProductService;
    private final TwoLevelProductService twoLevelProductService;
    private final BroadcastInvalidationProductService broadcastInvalidationProductService;
    private final DeleteAfterCommitProductService deleteAfterCommitProductService;
    private final LeaseProductService leaseProductService;
    private final SyncCacheableProductService syncCacheableProductService;
    private final StampedeLeaseProductService stampedeLeaseProductService;

    public ProductController(RedisCacheableProductService redisCacheableProductService,
                             DefaultConfigProductService defaultConfigProductService,
                             JitteredTtlProductService jitteredTtlProductService,
                             TwoLevelProductService twoLevelProductService,
                             BroadcastInvalidationProductService broadcastInvalidationProductService,
                             DeleteAfterCommitProductService deleteAfterCommitProductService,
                             LeaseProductService leaseProductService,
                             SyncCacheableProductService syncCacheableProductService,
                             StampedeLeaseProductService stampedeLeaseProductService) {
        this.redisCacheableProductService = redisCacheableProductService;
        this.defaultConfigProductService = defaultConfigProductService;
        this.jitteredTtlProductService = jitteredTtlProductService;
        this.twoLevelProductService = twoLevelProductService;
        this.broadcastInvalidationProductService = broadcastInvalidationProductService;
        this.deleteAfterCommitProductService = deleteAfterCommitProductService;
        this.leaseProductService = leaseProductService;
        this.syncCacheableProductService = syncCacheableProductService;
        this.stampedeLeaseProductService = stampedeLeaseProductService;
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Product>> findById(@PathVariable long id,
                                                  @RequestParam(defaultValue = "REDIS") ReadVia via) {
        Mono<Product> product = switch (via) {
            case REDIS -> redisCacheableProductService.findById(id);
            case REDIS_DEFAULT_CONFIG -> defaultConfigProductService.findById(id);
            case REDIS_JITTERED_TTL -> jitteredTtlProductService.findById(id);
            case TWO_LEVEL -> twoLevelProductService.findById(id);
            case BROADCAST -> broadcastInvalidationProductService.findById(id);
            case DELETE_AFTER_COMMIT -> deleteAfterCommitProductService.findById(id);
            case LEASE -> leaseProductService.findById(id);
            case REDIS_SYNC -> syncCacheableProductService.findById(id);
            case STAMPEDE_LEASE -> stampedeLeaseProductService.findById(id);
        };
        return product.map(ResponseEntity::ok).defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /*
     * Изменить цену. BROADCAST — сценарий 4: удалить из Redis и разослать инвалидацию остальным репликам.
     * DELETE_AFTER_COMMIT и LEASE — сценарий 5: удалить из Redis после коммита, без аренды и с ней.
     */
    @PutMapping("/{id}/price")
    public Mono<ResponseEntity<?>> changePrice(@PathVariable long id, @RequestBody PriceChange change,
                                               @RequestParam(defaultValue = "BROADCAST") ChangeVia via) {
        return switch (via) {
            case BROADCAST -> broadcastInvalidationProductService.changePrice(id, change.price())
                    .map(result -> result.updated() ? ResponseEntity.ok(result) : ResponseEntity.notFound().build());
            case DELETE_AFTER_COMMIT -> deleteAfterCommitProductService.changePrice(id, change.price())
                    .map(ProductController::updatedOrNotFound);
            case LEASE -> leaseProductService.changePrice(id, change.price())
                    .map(ProductController::updatedOrNotFound);
        };
    }

    private static ResponseEntity<?> updatedOrNotFound(boolean updated) {
        return updated ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }
}
