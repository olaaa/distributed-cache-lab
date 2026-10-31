package io.github.olaaa.distributedcachelab.web;

import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/* Служебные ручки для ручных экспериментов: сколько раз дошли до источника, сброс источника и Redis. */
@RestController
@RequestMapping("/lab")
public class LabController {

    public record LabStats(int sourceCalls) {
    }

    private final ProductSource source;
    private final ReactiveStringRedisTemplate redisTemplate;

    public LabController(ProductSource source, ReactiveStringRedisTemplate redisTemplate) {
        this.source = source;
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/stats")
    public LabStats stats() {
        return new LabStats(source.callCount());
    }

    @PostMapping("/source/fail")
    public void failSource() {
        source.setFailing(true);
    }

    @PostMapping("/source/recover")
    public void recoverSource() {
        source.setFailing(false);
    }

    /* Сбрасывает счётчик источника и очищает текущую базу Redis целиком: все кэши лаборатории живут в ней. */
    @PostMapping("/reset")
    public Mono<Void> reset() {
        source.reset();
        return redisTemplate.execute(connection -> connection.serverCommands().flushDb()).then();
    }
}
