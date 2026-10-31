package io.github.olaaa.distributedcachelab.scenario;

import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.github.olaaa.distributedcachelab.ManualTicker;
import io.github.olaaa.distributedcachelab.ManualTickerConfiguration;
import io.github.olaaa.distributedcachelab.TestcontainersConfiguration;
import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductRepository;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import io.github.olaaa.distributedcachelab.rates.RateProvider;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/*
 * Общая основа сценариев: один контекст Spring, один контейнер PostgreSQL и один Redis на все тесты.
 * Перед каждым тестом — чистая база Redis, пустые локальные кэши, обнулённые счётчики и одинаковые данные.
 */
@SpringBootTest(properties = "catalog.source.delay=100ms")
@Import({TestcontainersConfiguration.class, ManualTickerConfiguration.class})
abstract class ScenarioTestSupport {

    static final long KETTLE_ID = 1;
    static final BigDecimal KETTLE_PRICE = new BigDecimal("2490.00");

    @Autowired
    ProductRepository repository;

    @Autowired
    ProductSource source;

    /* Поставщик курсов валют (сценарий 7). */
    @Autowired
    RateProvider rateProvider;

    /* Прямой доступ к Redis в обход кэша: посмотреть ключи, значения и срок жизни. */
    @Autowired
    ReactiveStringRedisTemplate redis;

    /* Часы локальных кэшей Caffeine; срок жизни в Redis они не двигают — его считает сам Redis. */
    @Autowired
    ManualTicker ticker;

    @Autowired
    List<LabCache> labCaches;

    @BeforeEach
    void resetLab() {
        repository.deleteAll()
                .then(repository.save(new Product(KETTLE_ID, "Чайник", KETTLE_PRICE)))
                .block();
        redis.execute(connection -> connection.serverCommands().flushDb()).blockLast();
        labCaches.forEach(LabCache::invalidateAll);
        source.reset();
        rateProvider.reset();
    }

    /*
     * Ждём, пока Caffeine отметит время записи. AsyncCache делает это в колбэке future,
     * а колбэки CompletableFuture выполняются в обратном порядке регистрации: block()
     * может вернуть товар раньше, чем Caffeine запишет время. Если в этот промежуток
     * сдвинуть ManualTicker, запись получит время уже после сдвига и «помолодеет».
     */
    static void awaitEntryWritten(Supplier<Optional<Duration>> ageOfEntry) {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> ageOfEntry.get().filter(age -> !age.isNegative()).isPresent());
    }
}
