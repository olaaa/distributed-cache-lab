package io.github.olaaa.distributedcachelab.catalog;

import java.math.BigDecimal;

public record Product(long id, String name, BigDecimal price) {
}
