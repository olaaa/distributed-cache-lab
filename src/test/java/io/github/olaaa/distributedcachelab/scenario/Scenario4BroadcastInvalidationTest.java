package io.github.olaaa.distributedcachelab.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.github.olaaa.distributedcachelab.catalog.Product;
import io.github.olaaa.distributedcachelab.scenario.BroadcastInvalidationProductService.PriceChangeResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;

/* Сценарий 4. Инвалидация локальных кэшей между репликами через Redis Pub/Sub. */
class Scenario4BroadcastInvalidationTest extends ScenarioTestSupport {

    private static final BigDecimal NEW_PRICE = new BigDecimal("1990.00");

    /* Бин сервиса — «реплика A». */
    @Autowired
    BroadcastInvalidationProductService replicaA;

    @Autowired
    ReactiveRedisTemplate<String, Product> productRedisTemplate;

    @Autowired
    ReactiveRedisConnectionFactory connectionFactory;

    private final List<BroadcastInvalidationProductService> startedReplicas = new ArrayList<>();

    @BeforeEach
    void awaitReplicaASubscribed() {
        await().atMost(Duration.ofSeconds(5)).until(replicaA::isSubscribed);
    }

    @AfterEach
    void stopStartedReplicas() {
        startedReplicas.forEach(BroadcastInvalidationProductService::disconnect);
    }

    @Test
    void priceChangeOnOneReplicaInvalidatesLocalCopyOnAnother() {
        BroadcastInvalidationProductService replicaB = startReplica();
        replicaA.findById(KETTLE_ID).block();
        replicaB.findById(KETTLE_ID).block();
        assertThat(source.callCount()).isEqualTo(1);

        PriceChangeResult result = replicaA.changePrice(KETTLE_ID, NEW_PRICE).block();

        assertThat(result.notifiedReplicas())
                .as("PUBLISH вернул число получателей: обе реплики подписаны")
                .isEqualTo(2);
        await().atMost(Duration.ofSeconds(5)).until(() -> replicaB.invalidationsReceived() == 1);
        assertThat(replicaB.findById(KETTLE_ID).block().price())
                .as("реплика B удалила свою копию по сообщению и сразу видит новую цену — без ожидания LOCAL_TTL")
                .isEqualByComparingTo(NEW_PRICE);
        assertThat(replicaA.findById(KETTLE_ID).block().price()).isEqualByComparingTo(NEW_PRICE);
        assertThat(source.callCount())
                .as("после изменения — одна загрузка из базы: B загрузила и положила в Redis, A взяла из Redis")
                .isEqualTo(2);
    }

    @Test
    void replicaThatMissedMessageStaysStaleUntilLocalTimeToLive() {
        BroadcastInvalidationProductService replicaB = startReplica();
        BroadcastInvalidationProductService replicaC = startReplica();
        replicaA.findById(KETTLE_ID).block();
        replicaB.findById(KETTLE_ID).block();
        replicaC.findById(KETTLE_ID).block();
        awaitEntryWritten(() -> replicaB.localAgeOf(KETTLE_ID));

        /* Связь реплики B с Redis оборвалась на время рассылки. */
        replicaB.disconnect();
        PriceChangeResult result = replicaA.changePrice(KETTLE_ID, NEW_PRICE).block();

        assertThat(result.notifiedReplicas())
                .as("сообщение получили только подписанные реплики A и C")
                .isEqualTo(2);
        await().atMost(Duration.ofSeconds(5)).until(() -> replicaC.invalidationsReceived() == 1);
        assertThat(replicaC.findById(KETTLE_ID).block().price()).isEqualByComparingTo(NEW_PRICE);
        assertThat(replicaB.findById(KETTLE_ID).block().price())
                .as("Pub/Sub не повторяет доставку: реплика B сообщение потеряла и отдаёт старую цену")
                .isEqualByComparingTo(KETTLE_PRICE);

        ticker.advance(BroadcastInvalidationProductService.LOCAL_TTL.plusSeconds(1));

        assertThat(replicaB.findById(KETTLE_ID).block().price())
                .as("страховка — короткий LOCAL_TTL: локальная копия истекла, новая цена пришла из Redis")
                .isEqualByComparingTo(NEW_PRICE);
    }

    /* Ещё одна реплика: свой Caffeine и своё соединение подписки, общие Redis и база. */
    private BroadcastInvalidationProductService startReplica() {
        BroadcastInvalidationProductService replica = new BroadcastInvalidationProductService(
                source, repository, productRedisTemplate, redis, connectionFactory, ticker);
        startedReplicas.add(replica);
        await().atMost(Duration.ofSeconds(5)).until(replica::isSubscribed);
        return replica;
    }
}
