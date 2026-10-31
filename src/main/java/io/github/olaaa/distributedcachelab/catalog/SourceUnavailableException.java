package io.github.olaaa.distributedcachelab.catalog;

public class SourceUnavailableException extends RuntimeException {

    public SourceUnavailableException(long productId) {
        super("Источник каталога недоступен, товар " + productId);
    }
}
