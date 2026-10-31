package io.github.olaaa.distributedcachelab;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    /* @ServiceConnection сам выставляет R2DBC-подключение к контейнеру. */
    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer("postgres:18-alpine");
    }

    /*
     * Отдельного модуля Testcontainers для Redis не нужно: Spring Boot узнаёт GenericContainer
     * по имени образа redis и сам выставляет адрес подключения.
     */
    @Bean
    @ServiceConnection(name = "redis")
    GenericContainer<?> redisContainer() {
        return new GenericContainer<>("redis:8-alpine").withExposedPorts(6379);
    }
}
