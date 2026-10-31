package io.github.olaaa.distributedcachelab.scenario;

import java.math.BigDecimal;

import io.github.olaaa.distributedcachelab.TestcontainersConfiguration;
import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.catalog.ProductRepository;
import io.github.olaaa.distributedcachelab.catalog.ProductSource;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/*
 * Общая основа сценариев: один контекст Spring, один контейнер PostgreSQL и один Redis на все тесты.
 * Перед каждым тестом — чистая база Redis, обнулённый счётчик источника и одинаковые данные.
 */
@SpringBootTest(properties = "catalog.source.delay=100ms")
@Import(TestcontainersConfiguration.class)
abstract class ScenarioTestSupport {

    static final long KETTLE_ID = 1;
    static final BigDecimal KETTLE_PRICE = new BigDecimal("2490.00");

    @Autowired
    ProductRepository repository;

    @Autowired
    ProductSource source;

    /* Прямой доступ к Redis в обход кэша: посмотреть ключи, значения и срок жизни. */
    @Autowired
    ReactiveStringRedisTemplate redis;

    @BeforeEach
    void resetLab() {
        repository.deleteAll()
                .then(repository.save(new Product(KETTLE_ID, "Чайник", KETTLE_PRICE)))
                .block();
        redis.execute(connection -> connection.serverCommands().flushDb()).blockLast();
        source.reset();
    }
}
