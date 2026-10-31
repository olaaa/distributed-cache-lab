package io.github.olaaa.distributedcachelab.catalog;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Источник истины для всех сценариев — «дорогая» загрузка товара.
 * Считает обращения (главная метрика лаборатории: сколько раз мы дошли до БД),
 * добавляет задержку, чтобы промах был заметен, и умеет притворяться упавшим.
 */
@Component
public class ProductSource {

    private final ProductRepository repository;
    private final Duration delay;
    /**
     * Число обращений к источнику.
     * <p>
     * {@code ProductSource} — один бин на всё приложение. Счётчик увеличивается в момент подписки
     * на {@link #load(long)}, то есть на том потоке, где подписались, а таких потоков много:
     * потоки event loop HTTP-сервера ({@code webflux-http-nio-*}, их немного, каждый обслуживает
     * много соединений), потоки драйвера R2DBC ({@code reactor-tcp-nio-*}), если загрузка стоит
     * в цепочке после запроса к БД, потоки клиента Redis ({@code lettuce-*}) и поток теста.
     * Одновременные запросы приходят на разные потоки event loop и увеличивают счётчик
     * одновременно. А читает счётчик ещё один поток — тест или {@code /lab/stats}.
     * <p>
     * Обычный {@code int} с {@code callCount++} здесь не годится: {@code ++} — это три шага
     * (прочитать, прибавить, записать), и два потока могут прочитать одно и то же значение — одно
     * увеличение потеряется, и тест увидит меньше обращений, чем было. {@code volatile int} не
     * спасает: он гарантирует видимость, но не делает {@code ++} одной операцией.
     * {@link AtomicInteger#incrementAndGet()} выполняет увеличение атомарно (через compare-and-set)
     * и заодно гарантирует, что поток теста увидит все увеличения.
     */
    private final AtomicInteger callCount = new AtomicInteger();

    /**
     * Притворяется ли источник упавшим.
     * <p>
     * Флаг меняет один поток (тест или запрос {@code POST /lab/source/fail}), а читают другие —
     * те, что выполняют {@link #load(long)}. Без {@code volatile} модель памяти Java не обещает,
     * что читающий поток когда-нибудь увидит новое значение: оно может остаться в кэше процессора,
     * а JIT вправе вынести чтение поля из цикла. {@code volatile} даёт видимость: запись
     * происходит-до (happens-before) любого последующего чтения этого поля в другом потоке.
     * <p>
     * Атомарность здесь не нужна: с флагом делают только простую запись и простое чтение, без
     * «прочитать и изменить», поэтому {@code AtomicBoolean} с его compare-and-set был бы лишним.
     */
    private volatile boolean failing;

    public ProductSource(ProductRepository repository, @Value("${catalog.source.delay}") Duration delay) {
        this.repository = repository;
        this.delay = delay;
    }

    public Mono<Product> load(long id) {
        /* Mono.defer: обращение считается в момент подписки, а не в момент сборки цепочки. */
        return Mono.defer(() -> {
            callCount.incrementAndGet();
            if (failing) {
                return Mono.error(new SourceUnavailableException(id));
            }
            return Mono.delay(delay).then(repository.findById(id));
        });
    }

    public int callCount() {
        return callCount.get();
    }

    public void setFailing(boolean failing) {
        this.failing = failing;
    }

    public void reset() {
        callCount.set(0);
        failing = false;
    }
}
