# distributed-cache-lab

Каталог товаров на WebFlux, R2DBC и Spring Data Redis: приёмы распределённого кэша на Redis, каждый — отдельный сервис и тест со счётчиком обращений к источнику.

Продолжение [caching-lab](https://github.com/olaaa/caching-lab). Там — локальный кэш на Caffeine: у каждой реплики сервиса своя копия. Здесь — Redis как общий кэш для всех реплик.

## Как запустить

Нужны Java 25 и Docker.

```bash
mvn test
```

Тесты поднимают PostgreSQL и Redis в контейнерах Testcontainers, по одному на все сценарии.

```bash
mvn spring-boot:run
```

Spring Boot сам поднимет PostgreSQL и Redis из `compose.yaml`. Запросы для IntelliJ HTTP Client — в папке `http`, по файлу на сценарий, счётчик обращений к источнику — `GET /lab/stats`, сброс счётчика и очистка Redis — `POST /lab/reset`. Посмотреть содержимое Redis: `docker compose exec redis redis-cli`.

## Устройство

- `catalog` — таблица `product`, `ProductRepository` на `DatabaseClient` и `ProductSource`: чтение товара с задержкой, счётчиком обращений и умением падать.
- `scenario` — по сервису на приём.
- `rates` — курсы валют (сценарий 7): поставщик, сервис с `refreshAfterWrite`, прогрев при старте.
- `config` — настройка кэшей Redis (`CacheConfiguration`, `JitteredTtlCacheConfiguration`) и двухуровневого кэша (`TwoLevelCacheConfiguration`).
- `web` — `GET /products/{id}?via=…` выбирает сервис сценария, `/lab` — служебные ручки.

Клиент Redis — Lettuce, его приносит `spring-boot-starter-data-redis-reactive`. Реактивный `@Cacheable` на `Mono` с `RedisCache` работает только с реактивной фабрикой соединений, а её даёт Lettuce.

## Сценарии

Подробное описание каждого — [docs/scenarios](docs/scenarios/README.md). Ниже — коротко.

### 1. `RedisCacheManager` и сериализация

Тест: `Scenario1RedisCacheManagerTest`. Сервисы: `RedisCacheableProductService`, `DefaultConfigProductService`. Подробно: [docs/scenarios/01-redis-cache-manager.md](docs/scenarios/01-redis-cache-manager.md), вручную: `http/01-redis-cache-manager.http`.

`@Cacheable` на `Mono` без изменений в коде сервиса: Spring Boot создаёт `RedisCacheManager`, и значение ложится в Redis под ключом `products::1`. Его видит любая реплика, и оно переживает перезапуск приложения.

Настройки по умолчанию опасны: JDK-сериализация и вечный срок жизни. Кэш `products` настроен на JSON (`JacksonJsonRedisSerializer<Product>`) и срок жизни 10 минут. Кэш с настройками по умолчанию на record `Product` без `Serializable` падает при записи, и клиент получает 500.

### 2. TTL с разбросом

Тест: `Scenario2TtlJitterTest`. Сервис: `JitteredTtlProductService`. Подробно: [docs/scenarios/02-ttl-jitter.md](docs/scenarios/02-ttl-jitter.md), вручную: `http/02-ttl-jitter.http`.

Сто товаров загружаются в кэш одновременно (прогрев). С фиксированным сроком все сто ключей истекают в пределах нескольких десятков миллисекунд — и база получит их разом. Срок с разбросом (`TtlFunction`: 10 минут плюс случайная добавка до 2 минут) растягивает истечение на две минуты, по четыре-пять ключей в секунду.

### 3. Двухуровневый кэш: Caffeine плюс Redis

Тест: `Scenario3TwoLevelCacheTest`. Сервис: `TwoLevelProductService`. Подробно: [docs/scenarios/03-two-level-cache.md](docs/scenarios/03-two-level-cache.md), вручную: `http/03-two-level-cache.http`.

Перед Redis — локальный Caffeine в каждой реплике: повторные чтения идут из памяти, без сети. В замере — 1–4 мкс на чтение против 450–660 мкс через Redis. Вторая реплика берёт значение из Redis, а не из базы. Цена — копии реплик расходятся: об изменении в Redis реплика узнаёт, только когда истечёт её локальная копия (`LOCAL_TTL` = 30 секунд).

### 4. Инвалидация локальных кэшей между репликами

Тест: `Scenario4BroadcastInvalidationTest`. Сервис: `BroadcastInvalidationProductService`. Подробно: [docs/scenarios/04-broadcast-invalidation.md](docs/scenarios/04-broadcast-invalidation.md), вручную: `http/04-broadcast-invalidation.http` на двух репликах.

Реплика, которая меняет цену, коммитит изменение, удаляет ключ из Redis и из своего Caffeine и публикует id товара в канал Redis Pub/Sub. Остальные реплики удаляют свою локальную копию и сразу видят новую цену, без ожидания `LOCAL_TTL`. Pub/Sub доставляет не больше одного раза: отключённая реплика сообщение теряет, поэтому короткий `LOCAL_TTL` остаётся страховкой.

### 5. Удаление после коммита и гонка в Redis

Тест: `Scenario5DeleteAfterCommitTest`. Сервисы: `DeleteAfterCommitProductService`, `LeaseProductService`. Подробно: [docs/scenarios/05-delete-after-commit.md](docs/scenarios/05-delete-after-commit.md), вручную: `http/05-delete-after-commit.http`.

Изменение цены удаляет ключ из Redis после коммита (`@CacheEvict`), но медленное чтение, которое прочитало базу до коммита, записывает старую цену в Redis уже после удаления — и она живёт весь срок жизни, 10 минут. Встроенной защиты в Redis нет. Защита — аренда, как в memcache у Facebook: при промахе чтение берёт ключ-аренду через `SET NX`, пишет значение Lua-скриптом, только если аренда своя, а изменение удаляет и значение, и аренду. Медленное чтение в тестах и вручную — пауза `CacheWritePause` между базой и Redis.

### 6. Лавина промахов и `@Cacheable(sync = true)` с Redis

Тест: `Scenario6StampedeTest`. Сервисы: `SyncCacheableProductService`, `StampedeLeaseProductService`. Подробно: [docs/scenarios/06-stampede.md](docs/scenarios/06-stampede.md), вручную: `http/06-stampede.http` и `curl` из описания.

`@Cacheable(sync = true)` с Caffeine объединяет одновременные промахи, а с Redis в реактивном коде — нет: `RedisCache.retrieve` при промахе сразу вызывает загрузчик, и 50 одновременных промахов дают 50 загрузок. Одна загрузка на все реплики — аренда с ожиданием, как в memcache у Facebook: кто взял аренду, перепроверяет значение, загружает и пишет; остальные каждые 20 мс снова смотрят в Redis. Три реплики по 50 одновременных чтений — одна загрузка.

### 7. Курсы валют: обновление по расписанию и допустимое устаревание

Тест: `Scenario7ExchangeRatesTest`. Сервис: `ExchangeRateService`, прогрев — `ExchangeRateWarmUp`, поставщик — `RateProvider`. Подробно: [docs/scenarios/07-exchange-rates.md](docs/scenarios/07-exchange-rates.md), вручную: `http/07-exchange-rates.http` на двух репликах.

Курсы прогреваются при старте в `ApplicationRunner` — Spring Boot объявит реплику готовой только после него. Caffeine с `refreshAfterWrite` обновляет курс в фоне, отдавая прежний. Каждый полученный курс хранится в Redis как последний известный: реплика с пустым кэшем берёт его оттуда, если поставщик недоступен; неудачное обновление оставляет прежний курс, но берёт из Redis более новый, если его получила другая реплика. К поставщику ходят не по часам, а при старте и при первом запросе после 5 минут; курс старше суток не отдаётся — запрос ждёт обновления, не вышло — 503.
