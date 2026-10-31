package io.github.olaaa.distributedcachelab.web;

import java.time.Duration;
import java.util.List;

import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import io.github.olaaa.distributedcachelab.rates.RateProvider;
import io.github.olaaa.distributedcachelab.scenario.BroadcastInvalidationProductService;
import io.github.olaaa.distributedcachelab.scenario.CacheWritePause;
import io.github.olaaa.distributedcachelab.scenario.LabCache;
import io.github.olaaa.distributedcachelab.scenario.TwoLevelProductService;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/* Служебные ручки для ручных экспериментов: сколько раз дошли до источника и до Redis, сброс. */
@RestController
@RequestMapping("/lab")
public class LabController {

    public record LabStats(int sourceCalls, int twoLevelRedisReads, int broadcastInvalidationsReceived) {
    }

    private final ProductSource source;
    private final TwoLevelProductService twoLevelProductService;
    private final BroadcastInvalidationProductService broadcastInvalidationProductService;
    private final List<LabCache> labCaches;
    private final ReactiveStringRedisTemplate redisTemplate;
    private final CacheWritePause cacheWritePause;
    private final RateProvider rateProvider;

    public LabController(ProductSource source,
                         TwoLevelProductService twoLevelProductService,
                         BroadcastInvalidationProductService broadcastInvalidationProductService,
                         List<LabCache> labCaches,
                         ReactiveStringRedisTemplate redisTemplate,
                         CacheWritePause cacheWritePause,
                         RateProvider rateProvider) {
        this.source = source;
        this.twoLevelProductService = twoLevelProductService;
        this.broadcastInvalidationProductService = broadcastInvalidationProductService;
        this.labCaches = labCaches;
        this.redisTemplate = redisTemplate;
        this.cacheWritePause = cacheWritePause;
        this.rateProvider = rateProvider;
    }

    public record RateProviderState(int calls, boolean failing) {
    }

    /* Сценарий 7: сколько раз обращались к поставщику курсов и доступен ли он. */
    @GetMapping("/rates/provider")
    public RateProviderState rateProvider() {
        return new RateProviderState(rateProvider.callCount(), rateProvider.isFailing());
    }

    @PostMapping("/rates/provider/fail")
    public void failRateProvider() {
        rateProvider.setFailing(true);
    }

    @PostMapping("/rates/provider/recover")
    public void recoverRateProvider() {
        rateProvider.setFailing(false);
    }

    @GetMapping("/stats")
    public LabStats stats() {
        return new LabStats(source.callCount(), twoLevelProductService.redisReadCount(),
                broadcastInvalidationProductService.invalidationsReceived());
    }

    @PostMapping("/source/fail")
    public void failSource() {
        source.setFailing(true);
    }

    @PostMapping("/source/recover")
    public void recoverSource() {
        source.setFailing(false);
    }

    /* Сценарий 5: пауза между чтением из базы и записью в Redis, чтобы успеть изменить цену. 0 — без паузы. */
    @PostMapping("/cache-write-pause")
    public void cacheWritePause(@RequestParam long seconds) {
        cacheWritePause.set(Duration.ofSeconds(seconds));
    }

    /*
     * Сбрасывает счётчик источника, локальные кэши и очищает текущую базу Redis целиком:
     * все кэши лаборатории живут в ней.
     */
    @PostMapping("/reset")
    public Mono<Void> reset() {
        source.reset();
        rateProvider.reset();
        labCaches.forEach(LabCache::invalidateAll);
        return redisTemplate.execute(connection -> connection.serverCommands().flushDb()).then();
    }
}
