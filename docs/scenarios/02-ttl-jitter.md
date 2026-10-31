# Сценарий 2. TTL с разбросом

Тест: `src/test/java/io/github/olaaa/distributedcachelab/scenario/Scenario2TtlJitterTest.java`.
Сервисы: `JitteredTtlProductService` (кэш `productsJittered`), для сравнения — `RedisCacheableProductService` из сценария 1 (кэш `products`). Настройка: `config/JitteredTtlCacheConfiguration`.
Ручная проверка: `http/02-ttl-jitter.http` и команды `redis-cli` ниже.

Разброс по-английски — jitter, буквально «дрожание»: небольшое случайное отклонение от заданного значения.

## Ситуация

Каталог прогревает кэш: при старте реплики или после очистки Redis сотня популярных товаров загружается в кэш почти одновременно. У кэша `products` срок жизни фиксированный — 10 минут. Значит, через 10 минут все сто записей истекут почти одновременно, и следующие запросы к этим товарам разом пойдут в БД. Если в этот момент магазин под нагрузкой, база получает волну запросов, которой не было бы, если бы записи истекали вразнобой.

Тот же эффект бывает не только после прогрева: массовая загрузка, рассылка, после которой все открыли одни и те же товары, восстановление после падения Redis.

AWS в whitepaper «Database Caching Strategies Using Redis» описывает ровно этот случай с товарами: «If all your product data expires at the same time and your application is under heavy load, then your backend database has to fulfill all the product requests». И рекомендует: «add some time jitter to your TTLs ... (for example, TTL = your initial TTL value in seconds + jitter)».

## Как это работает

Срок жизни = базовый + случайная добавка. В Spring Data Redis срок можно задать не числом, а функцией `TtlFunction`:

```java
public static final Duration BASE_TTL = CacheConfiguration.PRODUCTS_TTL;   // 10 минут
public static final Duration MAX_JITTER = Duration.ofMinutes(2);

public static TtlFunction jitteredTtl() {
    return (key, value) -> BASE_TTL.plusMillis(ThreadLocalRandom.current().nextLong(MAX_JITTER.toMillis() + 1));
}

public static RedisCacheConfiguration jitteredCacheConfiguration() {
    return CacheConfiguration.productsCacheConfiguration().entryTtl(jitteredTtl());
}
```

Javadoc `TtlFunction.getTimeToLive`: «The time-to-live is computed on each write operation». Функция вызывается при каждой записи в кэш, поэтому записи, загруженные в одну и ту же миллисекунду, получают разные сроки — от 10 до 12 минут. `entryTtl(Duration)` из сценария 1 — частный случай: внутри он превращается в `TtlFunction.just(ttl)`, функцию, которая всегда возвращает одно и то же.

Функция получает ключ и значение, поэтому срок можно выбирать и по ним: например, короче для дешёвых товаров и длиннее для справочников. Здесь значение не используется.

`JitteredTtlCacheConfiguration` берёт настройку кэша `products` из сценария 1 (JSON, без `null`) и заменяет в ней только срок. `RedisCacheConfiguration` неизменяемый: `entryTtl` возвращает новый объект и не трогает исходный.

## Сколько брать разброса

Разброс должен быть заметно больше, чем время, за которое записи попали в кэш: прогрев за полсекунды и разброс в секунду почти ничего не дадут. И при этом не слишком большим: записи, которым досталась большая добавка, дольше остаются устаревшими. Здесь добавка до 2 минут при базовом сроке 10 минут, то есть до 20 % — вывод из этих двух условий, а не правило из источника: AWS говорит только о «slight jitter».

Разброс не защищает от лавины промахов на одном горячем ключе — там нужна одна загрузка на ключ (сценарий 6 по плану) или фоновое обновление. Он защищает от одновременного истечения многих разных ключей.

## Что проверяют тесты

Перед каждым тестом в таблицу добавляются товары 2–100, вместе с «Чайником» — сто товаров. Прогрев — сто одновременных чтений через `flatMap`, затем срок каждого ключа читается из Redis напрямую (`getExpire`, то есть команда `PTTL`).

`fixedTtlEntriesLoadedTogetherExpireTogether`: прогрев через кэш `products` с фиксированным сроком. Обращений к источнику 100, а сроки всех ста ключей отличаются меньше чем на секунду.

`jitteredTtlSpreadsExpirationOverJitterWindow`: прогрев через кэш `productsJittered`. Каждый срок — от 10 до 12 минут, сроки разошлись больше чем на половину окна разброса, и ни в какую секунду не истекает больше 10 ключей.

`jitteredTtlStaysWithinBounds`: 10 000 вызовов `TtlFunction` без Redis — все сроки в пределах от 10 до 12 минут.

Цифры трёх прогонов теста:

| Прогон | Фиксированный срок: разброс сроков | Фиксированный срок: ключей в одну секунду | С разбросом: разброс сроков | С разбросом: ключей в одну секунду |
|---|---|---|---|---|
| 1 | 18 мс | 100 | 1 мин 57 с | 4 |
| 2 | 36 мс | 100 | 1 мин 58 с | 4 |
| 3 | 24 мс | 100 | 2 мин 0 с | 5 |

Без разброса все сто ключей истекают в пределах нескольких десятков миллисекунд. С разбросом — по четыре-пять в секунду на протяжении двух минут.

Самого истечения тест не ждёт: срок считают часы Redis, подменить их нельзя, а ждать 10 минут в тесте — не вариант. Тест проверяет причину — распределение сроков в момент записи.

## Попробовать вручную

Запустить приложение: `mvn spring-boot:run`. Открыть `http/02-ttl-jitter.http`: сброс, затем товары 1, 2 и 3 через `REDIS` и через `REDIS_JITTERED_TTL`. Потом посмотреть сроки:

```bash
for id in 1 2 3; do docker compose exec redis redis-cli TTL products::$id; done
for id in 1 2 3; do docker compose exec redis redis-cli TTL productsJittered::$id; done
```

У `products` три почти одинаковых числа около 600 секунд, у `productsJittered` — три разных числа от 600 до 720.

## Что важно заметить

- Проблема не в одном ключе, а в совпадении сроков многих ключей. Срок совпадает, когда совпадает момент записи: прогрев, массовая загрузка, восстановление после очистки.
- Разброс — одна строка конфигурации, а не новый механизм. Поэтому его ставят почти всегда, когда записи могут попасть в кэш пачкой.
- Разброс работает и для Caffeine: там срок задаётся через `Expiry` с той же случайной добавкой. Это вывод по аналогии, в caching-lab не проверялся.

## Как сказать на собеседовании

«Если записи попадают в кэш пачкой — прогрев, массовая загрузка, — с одинаковым TTL они и истекут пачкой, и база получит волну запросов. Поэтому к сроку добавляю случайную добавку: TTL = базовый + jitter. В Spring Data Redis это `TtlFunction` в `RedisCacheConfiguration.entryTtl`, она вычисляется при каждой записи. Разброс беру заметно больше времени прогрева, но небольшой относительно базового срока, например до 20 %. От лавины на одном горячем ключе он не спасает — там нужна одна загрузка на ключ».

## Источники

- AWS whitepaper «Database Caching Strategies Using Redis» — [Cache Validity](https://docs.aws.amazon.com/whitepapers/latest/database-caching-strategies-using-redis/cache-validity.html).
- Spring Data Redis 4.1.2, исходники: `RedisCacheWriter.TtlFunction`, `RedisCacheConfiguration.entryTtl`, `RedisCache.put`.
