package io.github.olaaa.distributedcachelab;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration(proxyBeanMethods = false)
public class ManualTickerConfiguration {

    /* @Primary: ручные кэши получают эти часы вместо Ticker.systemTicker(). */
    @Bean
    @Primary
    ManualTicker manualTicker() {
        return new ManualTicker();
    }
}
