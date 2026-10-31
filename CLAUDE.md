# distributed-cache-lab — правила для ассистента

- Публичный репозиторий. Никаких корпоративных имён: сервисов, таблиц, пакетов, тикетов и названий компаний из рабочих проектов. Список запрещённых слов лежит в `.git/info/forbidden-words` и проверяется хуками `pre-commit` и `commit-msg`.
- Продолжение [caching-lab](https://github.com/olaaa/caching-lab): там локальный кэш на Caffeine (этап 1), здесь Redis как кэш (этап 2). Фундамент перенесён оттуда: `ProductSource` со счётчиком обращений, `ScenarioTestSupport`.
- Стек: Java 25, Spring Boot 4.1, WebFlux, R2DBC через `DatabaseClient`, Reactor, Spring Data Redis с клиентом Lettuce. Без JPA.
- Каждый приём — отдельный сервис в `scenario` и отдельный тест `ScenarioN…Test`. Главная проверка — `ProductSource.callCount()`, плюс прямой взгляд в Redis через `ReactiveStringRedisTemplate`. У каждого сценария — описание в `docs/scenarios/NN-….md` и файл ручной проверки `http/NN-….http`; при изменении сценария обновлять все три.
- Тесты поднимают PostgreSQL и Redis в Testcontainers; перед каждым тестом база Redis очищается. Срок жизни записей в Redis считают часы самого Redis, их не подменить: проверять остаток через `getExpire` или короткие сроки с `await`, без `sleep`.
- Стиль: многострочные комментарии одним блоком `/* */`, фигурные скобки у тел `if`/`for`/`while`, говорящие имена.
- Ветка `main`. Коммит и пуш — по окончании задачи, `git add` по именам файлов, перед пушем `git fetch`.
