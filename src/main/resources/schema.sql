/*
 * id без генератора: все id задаются явно (data.sql, тесты с KETTLE_ID и MISSING_ID).
 * Если бы товары создавало приложение, здесь был бы BIGINT GENERATED ALWAYS AS IDENTITY,
 * а не BIGSERIAL: стандарт SQL, последовательность принадлежит столбцу, свой id случайно
 * не вставить.
 */
CREATE TABLE IF NOT EXISTS product
(
    id    BIGINT PRIMARY KEY,
    name  TEXT           NOT NULL,
    price NUMERIC(12, 2) NOT NULL
);
