package io.github.olaaa.distributedcachelab.web;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.scenario.DefaultConfigProductService;
import io.github.olaaa.distributedcachelab.scenario.JitteredTtlProductService;
import io.github.olaaa.distributedcachelab.scenario.RedisCacheableProductService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/products")
public class ProductController {

    public enum ReadVia {
        REDIS, REDIS_DEFAULT_CONFIG, REDIS_JITTERED_TTL
    }

    private final RedisCacheableProductService redisCacheableProductService;
    private final DefaultConfigProductService defaultConfigProductService;
    private final JitteredTtlProductService jitteredTtlProductService;

    public ProductController(RedisCacheableProductService redisCacheableProductService,
                             DefaultConfigProductService defaultConfigProductService,
                             JitteredTtlProductService jitteredTtlProductService) {
        this.redisCacheableProductService = redisCacheableProductService;
        this.defaultConfigProductService = defaultConfigProductService;
        this.jitteredTtlProductService = jitteredTtlProductService;
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Product>> findById(@PathVariable long id,
                                                  @RequestParam(defaultValue = "REDIS") ReadVia via) {
        Mono<Product> product = switch (via) {
            case REDIS -> redisCacheableProductService.findById(id);
            case REDIS_DEFAULT_CONFIG -> defaultConfigProductService.findById(id);
            case REDIS_JITTERED_TTL -> jitteredTtlProductService.findById(id);
        };
        return product.map(ResponseEntity::ok).defaultIfEmpty(ResponseEntity.notFound().build());
    }
}
