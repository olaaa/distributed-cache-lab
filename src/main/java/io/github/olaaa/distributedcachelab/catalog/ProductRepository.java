package io.github.olaaa.distributedcachelab.catalog;

import java.math.BigDecimal;

import io.r2dbc.spi.Readable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/* Чистый доступ к таблице product через DatabaseClient: без кэша, без задержек, без счётчиков. */
@Repository
public class ProductRepository {

    private final DatabaseClient databaseClient;

    public ProductRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Mono<Product> findById(long id) {
        return databaseClient.sql("SELECT id, name, price FROM product WHERE id = :id")
                .bind("id", id)
                .map(ProductRepository::toProduct)
                .one();
    }

    public Mono<Long> updatePrice(long id, BigDecimal price) {
        return databaseClient.sql("UPDATE product SET price = :price WHERE id = :id")
                .bind("id", id)
                .bind("price", price)
                .fetch()
                .rowsUpdated();
    }

    public Mono<Void> save(Product product) {
        return databaseClient.sql("""
                        INSERT INTO product (id, name, price)
                        VALUES (:id, :name, :price)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, price = EXCLUDED.price
                        """)
                .bind("id", product.id())
                .bind("name", product.name())
                .bind("price", product.price())
                .then();
    }

    public Mono<Void> deleteAll() {
        return databaseClient.sql("DELETE FROM product").then();
    }

    private static Product toProduct(Readable row) {
        return new Product(
                row.get("id", Long.class),
                row.get("name", String.class),
                row.get("price", BigDecimal.class));
    }
}
