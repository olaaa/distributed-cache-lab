package io.github.olaaa.distributedcachelab.web;

import io.github.olaaa.distributedcachelab.rates.ExchangeRate;
import io.github.olaaa.distributedcachelab.rates.ExchangeRateService;
import io.github.olaaa.distributedcachelab.rates.RateProviderUnavailableException;
import io.github.olaaa.distributedcachelab.rates.StaleRateException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/* Сценарий 7. Курсы валют. Курса нет или он устарел — 503: повторить позже. */
@RestController
@RequestMapping("/rates")
public class RateController {

    private final ExchangeRateService exchangeRateService;

    public RateController(ExchangeRateService exchangeRateService) {
        this.exchangeRateService = exchangeRateService;
    }

    @GetMapping("/{currency}")
    public Mono<ExchangeRate> findRate(@PathVariable String currency) {
        return exchangeRateService.findRate(currency);
    }

    @ExceptionHandler({RateProviderUnavailableException.class, StaleRateException.class})
    public ResponseEntity<String> rateUnavailable(RuntimeException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(exception.getMessage());
    }
}
