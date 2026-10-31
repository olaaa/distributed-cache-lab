package io.github.olaaa.distributedcachelab.scenario;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 5. Пауза между чтением из базы и записью в Redis.
 *
 * В жизни такая пауза случается сама: сборка мусора, медленная сеть до Redis, перегруженный поток.
 * Лаборатория вставляет её явно, чтобы гонку чтения и изменения можно было повторить каждый раз:
 * в тесте и вручную через POST /lab/cache-write-pause. По умолчанию паузы нет.
 */
@Component
public class CacheWritePause implements LabCache {

    private volatile Duration pause = Duration.ZERO;
    private final AtomicInteger pausedWrites = new AtomicInteger();

    /* Вызывается, когда значение уже прочитано из базы, но ещё не записано в Redis. */
    public <T> Mono<T> before(T value) {
        Duration current = pause;
        if (current.isZero()) {
            return Mono.just(value);
        }
        pausedWrites.incrementAndGet();
        return Mono.delay(current).thenReturn(value);
    }

    public void set(Duration pause) {
        this.pause = pause;
    }

    /* Сколько записей в Redis сейчас ждут или уже переждали паузу. */
    public int pausedWrites() {
        return pausedWrites.get();
    }

    /* Сброс лаборатории: без паузы. */
    @Override
    public void invalidateAll() {
        pause = Duration.ZERO;
        pausedWrites.set(0);
    }
}
