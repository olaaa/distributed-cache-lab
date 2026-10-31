package io.github.olaaa.distributedcachelab.rates;

/* Поставщик курсов не ответил, и последнего известного курса тоже нет. */
public class RateProviderUnavailableException extends RuntimeException {

    public RateProviderUnavailableException(String currency) {
        super("Поставщик курсов недоступен, курса " + currency + " нет");
    }
}
