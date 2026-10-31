package io.github.olaaa.distributedcachelab.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import io.github.olaaa.distributedcachelab.catalog.Product;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;

/* Сценарий 3. Двухуровневый кэш: Caffeine поверх Redis. */
class Scenario3TwoLevelCacheTest extends ScenarioTestSupport {

    private static final String KETTLE_REDIS_KEY = TwoLevelProductService.REDIS_KEY_PREFIX + KETTLE_ID;
    private static final BigDecimal NEW_PRICE = new BigDecimal("1990.00");

    /* Бин сервиса — «реплика A». */
    @Autowired
    TwoLevelProductService replicaA;

    @Autowired
    ReactiveRedisTemplate<String, Product> productRedisTemplate;

    @Test
    void firstReadGoesThroughRedisToSourceAndFillsBothLevels() {
        assertThat(replicaA.findById(KETTLE_ID).block()).isNotNull();

        assertThat(source.callCount()).isEqualTo(1);
        assertThat(replicaA.redisReadCount())
                .as("промах первого уровня — один запрос в Redis, там тоже промах")
                .isEqualTo(1);
        assertThat(redis.hasKey(KETTLE_REDIS_KEY).block())
                .as("после загрузки из источника значение положено и в Redis")
                .isTrue();
    }

    @Test
    void repeatedReadsAreServedLocallyWithoutRedis() {
        for (int readNumber = 0; readNumber < 10; readNumber++) {
            assertThat(replicaA.findById(KETTLE_ID).block()).isNotNull();
        }

        assertThat(source.callCount()).isEqualTo(1);
        assertThat(replicaA.redisReadCount())
                .as("девять повторных чтений отдал Caffeine, без сетевых запросов")
                .isEqualTo(1);
    }

    @Test
    void anotherReplicaGetsValueFromRedisNotFromSource() {
        /* Вторая реплика: свой локальный кэш, тот же Redis и тот же источник. */
        TwoLevelProductService replicaB = new TwoLevelProductService(source, productRedisTemplate, ticker);

        replicaA.findById(KETTLE_ID).block();
        for (int readNumber = 0; readNumber < 5; readNumber++) {
            assertThat(replicaB.findById(KETTLE_ID).block()).isNotNull();
        }

        assertThat(source.callCount())
                .as("реплика B нашла товар в Redis и в источник не ходила")
                .isEqualTo(1);
        assertThat(replicaB.redisReadCount())
                .as("реплика B сходила в Redis один раз, дальше — свой Caffeine")
                .isEqualTo(1);
    }

//    Локальная копия остаётся устаревшей, пока не истечёт локальный срок жизни
    @Test
    void localCopyStaysStaleUntilLocalTimeToLiveExpires() {
        replicaA.findById(KETTLE_ID).block();
        awaitEntryWritten(() -> replicaA.localAgeOf(KETTLE_ID));

        /* Другая реплика обновила цену и записала новое значение в Redis. */
        productRedisTemplate.opsForValue()
                .set(KETTLE_REDIS_KEY, new Product(KETTLE_ID, "Чайник", NEW_PRICE), TwoLevelProductService.REDIS_TTL)
                .block();

        assertThat(replicaA.findById(KETTLE_ID).block().price())
                .as("реплика A не знает об изменении: отвечает своя локальная копия")
                .isEqualByComparingTo(KETTLE_PRICE);

        ticker.advance(TwoLevelProductService.LOCAL_TTL.plusSeconds(1));

        assertThat(replicaA.findById(KETTLE_ID).block().price())
                .as("локальная копия истекла — новое значение пришло из Redis")
                .isEqualByComparingTo(NEW_PRICE);
        assertThat(source.callCount()).isEqualTo(1);
        assertThat(replicaA.redisReadCount()).isEqualTo(2);
    }
}
