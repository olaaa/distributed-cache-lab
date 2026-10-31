package io.github.olaaa.distributedcachelab.rates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/*
 * Сценарий 7. Прогрев курсов при старте.
 *
 * ApplicationRunner, а не @PostConstruct: Spring Boot считает приложение готовым принимать трафик
 * (ReadinessState.ACCEPTING_TRAFFIC) только после того, как отработали все раннеры. Пока курсы
 * грузятся, балансировщик на реплику запросы не отправит.
 */
@Component
public class ExchangeRateWarmUp implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ExchangeRateWarmUp.class);

    private final ExchangeRateService exchangeRateService;

    public ExchangeRateWarmUp(ExchangeRateService exchangeRateService) {
        this.exchangeRateService = exchangeRateService;
    }

    @Override
    public void run(ApplicationArguments arguments) {
        Long loaded = exchangeRateService.warmUp().block();
        log.info("Прогрев курсов: загружено валют — {}", loaded);
    }
}
