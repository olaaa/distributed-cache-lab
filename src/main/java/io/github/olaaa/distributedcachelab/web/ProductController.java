package io.github.olaaa.distributedcachelab.web;

import java.math.BigDecimal;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.scenario.BroadcastInvalidationProductService;
import io.github.olaaa.distributedcachelab.scenario.BroadcastInvalidationProductService.PriceChangeResult;
import io.github.olaaa.distributedcachelab.scenario.DefaultConfigProductService;
import io.github.olaaa.distributedcachelab.scenario.JitteredTtlProductService;
import io.github.olaaa.distributedcachelab.scenario.RedisCacheableProductService;
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
        REDIS, REDIS_DEFAULT_CONFIG, REDIS_JITTERED_TTL, TWO_LEVEL, BROADCAST
    }

    public record PriceChange(BigDecimal price) {
    }

    private final RedisCacheableProductService redisCacheableProductService;
    private final DefaultConfigProductService defaultConfigProductService;
    private final JitteredTtlProductService jitteredTtlProductService;
    private final TwoLevelProductService twoLevelProductService;
    private final BroadcastInvalidationProductService broadcastInvalidationProductService;

    public ProductController(RedisCacheableProductService redisCacheableProductService,
                             DefaultConfigProductService defaultConfigProductService,
                             JitteredTtlProductService jitteredTtlProductService,
                             TwoLevelProductService twoLevelProductService,
                             BroadcastInvalidationProductService broadcastInvalidationProductService) {
        this.redisCacheableProductService = redisCacheableProductService;
        this.defaultConfigProductService = defaultConfigProductService;
        this.jitteredTtlProductService = jitteredTtlProductService;
        this.twoLevelProductService = twoLevelProductService;
        this.broadcastInvalidationProductService = broadcastInvalidationProductService;
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
        };
        return product.map(ResponseEntity::ok).defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /* Сценарий 4: изменить цену и разослать инвалидацию остальным репликам. */
    @PutMapping("/{id}/price")
    public Mono<ResponseEntity<PriceChangeResult>> changePrice(@PathVariable long id, @RequestBody PriceChange change) {
        return broadcastInvalidationProductService.changePrice(id, change.price())
                .map(result -> result.updated() ? ResponseEntity.ok(result) : ResponseEntity.notFound().build());
    }
}
