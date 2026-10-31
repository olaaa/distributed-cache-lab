package io.github.olaaa.distributedcachelab.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.github.olaaa.distributedcachelab.catalog.Product;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveRedisTemplate;

/* Сценарий 5. Удаление из Redis после коммита и гонка с медленным чтением; аренда как защита. */
class Scenario5DeleteAfterCommitTest extends ScenarioTestSupport {

    private static final BigDecimal NEW_PRICE = new BigDecimal("1990.00");
    private static final String DELETE_AFTER_COMMIT_KEY = DeleteAfterCommitProductService.CACHE_NAME + "::" + KETTLE_ID;
    private static final String LEASE_VALUE_KEY = LeaseProductService.KEY_PREFIX + KETTLE_ID;
    private static final String LEASE_KEY = LeaseProductService.LEASE_KEY_PREFIX + KETTLE_ID;

    /* Пауза медленного чтения между базой и Redis: изменение цены успевает пройти целиком, пока чтение ждёт. */
    private static final Duration SLOW_READ_PAUSE = Duration.ofSeconds(1);

    @Autowired
    DeleteAfterCommitProductService deleteAfterCommit;

    @Autowired
    LeaseProductService lease;

    @Autowired
    CacheWritePause cacheWritePause;

    @Autowired
    ReactiveRedisTemplate<String, Product> productRedisTemplate;

    @Test
    void deleteAfterCommitMakesNextReadLoadNewPrice() {
        deleteAfterCommit.findById(KETTLE_ID).block();
        assertThat(hasKey(DELETE_AFTER_COMMIT_KEY)).isTrue();

        assertThat(deleteAfterCommit.changePrice(KETTLE_ID, NEW_PRICE).block()).isTrue();

        assertThat(hasKey(DELETE_AFTER_COMMIT_KEY))
                .as("@CacheEvict удалил ключ после того, как UPDATE закоммитился")
                .isFalse();
        assertThat(deleteAfterCommit.findById(KETTLE_ID).block().price()).isEqualByComparingTo(NEW_PRICE);
        assertThat(source.callCount()).isEqualTo(2);
    }

    @Test
    void slowReadPutsOldPriceBackAfterDelete() {
        cacheWritePause.set(SLOW_READ_PAUSE);

        /* Медленное чтение: промах, старая цена из базы, пауза перед записью в Redis. */
        CompletableFuture<Product> slowRead = deleteAfterCommit.findById(KETTLE_ID).toFuture();
        awaitSlowReadPaused();

        /* Пока чтение ждёт — изменение цены: UPDATE, коммит, удаление ключа. */
        assertThat(deleteAfterCommit.changePrice(KETTLE_ID, NEW_PRICE).block()).isTrue();
        assertThat(hasKey(DELETE_AFTER_COMMIT_KEY)).as("удаление прошло: ключа в Redis нет").isFalse();

        assertThat(awaitResult(slowRead).price())
                .as("медленное чтение прочитало базу до коммита")
                .isEqualByComparingTo(KETTLE_PRICE);
        await().atMost(Duration.ofSeconds(5)).until(() -> hasKey(DELETE_AFTER_COMMIT_KEY));
        cacheWritePause.set(Duration.ZERO);

        assertThat(repository.findById(KETTLE_ID).block().price())
                .as("в базе новая цена")
                .isEqualByComparingTo(NEW_PRICE);
        assertThat(deleteAfterCommit.findById(KETTLE_ID).block().price())
                .as("а кэш отдаёт старую: медленное чтение записало её в Redis уже после удаления")
                .isEqualByComparingTo(KETTLE_PRICE);
        assertThat(redis.getExpire(DELETE_AFTER_COMMIT_KEY).block())
                .as("и старая цена пролежит в Redis почти весь срок жизни — 10 минут")
                .isGreaterThan(Duration.ofMinutes(9));
        assertThat(source.callCount()).isEqualTo(1);
    }

    @Test
    void leaseRejectsStaleWriteAfterDelete() {
        cacheWritePause.set(SLOW_READ_PAUSE);

        CompletableFuture<Product> slowRead = lease.findById(KETTLE_ID).toFuture();
        awaitSlowReadPaused();
        assertThat(hasKey(LEASE_KEY)).as("медленное чтение взяло аренду до чтения базы").isTrue();

        assertThat(lease.changePrice(KETTLE_ID, NEW_PRICE).block()).isTrue();
        assertThat(hasKey(LEASE_KEY)).as("изменение после коммита сняло аренду вместе со значением").isFalse();

        assertThat(awaitResult(slowRead).price())
                .as("медленное чтение по-прежнему прочитало старую цену")
                .isEqualByComparingTo(KETTLE_PRICE);
        assertThat(lease.staleWritesRejected())
                .as("но записать её в Redis не смогло: своей аренды на месте нет")
                .isEqualTo(1);
        assertThat(hasKey(LEASE_VALUE_KEY)).as("старой цены в Redis нет").isFalse();
        cacheWritePause.set(Duration.ZERO);

        assertThat(lease.findById(KETTLE_ID).block().price())
                .as("следующее чтение загрузило новую цену")
                .isEqualByComparingTo(NEW_PRICE);
        assertThat(productRedisTemplate.opsForValue().get(LEASE_VALUE_KEY).block().price())
                .isEqualByComparingTo(NEW_PRICE);
        assertThat(source.callCount()).isEqualTo(2);
    }

    @Test
    void leaseLetsOrdinaryMissFillCache() {
        lease.findById(KETTLE_ID).block();

        assertThat(hasKey(LEASE_VALUE_KEY)).isTrue();
        assertThat(hasKey(LEASE_KEY)).as("после записи аренда снята").isFalse();
        assertThat(redis.getExpire(LEASE_VALUE_KEY).block()).isGreaterThan(Duration.ofMinutes(9));

        lease.findById(KETTLE_ID).block();
        assertThat(source.callCount()).as("второе чтение — из Redis").isEqualTo(1);
        assertThat(lease.staleWritesRejected()).isZero();
    }

    /* Медленное чтение прочитало базу и встало на паузу перед записью в Redis. */
    private void awaitSlowReadPaused() {
        await().atMost(Duration.ofSeconds(5)).until(() -> cacheWritePause.pausedWrites() == 1);
    }

    private static Product awaitResult(CompletableFuture<Product> read) {
        try {
            return read.get(5, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("медленное чтение не завершилось", exception);
        }
    }

    private boolean hasKey(String key) {
        return Boolean.TRUE.equals(redis.hasKey(key).block());
    }
}
