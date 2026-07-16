# Архитектурные решения

## ADR-001 — PostgreSQL как durable source of truth

**Статус:** принято.

PostgreSQL хранит inbox, операции, попытки, leases, результаты, durable deliveries для
HTTP/webhook и прикладные ACK. Он обеспечивает восстановление Proxy после рестарта.
Клиент использует отдельное постоянное хранилище своего приложения; встроенную БД
клиентская библиотека не создаёт.

## ADR-002 — At-least-once и стабильные идентификаторы

**Статус:** принято.

Команды, результаты и webhook deliveries используют стабильные ID и идемпотентную
регистрацию. Exactly-once HTTP side effect не обещается.

## ADR-003 — Универсальный HTTP proxy без provider adapters

**Статус:** принято; заменяет прежнее решение о typed integration registry.

Вызывающий сервис передает method, URL, headers и body. Proxy не знает бизнес-схему и
не добавляет credentials. Авторизованный сервис может обратиться к любому URL.

## ADR-004 — Core NATS без JetStream

**Статус:** принято; заменяет прежнее решение использовать JetStream.

Core NATS остается быстрым транспортом. Durability обеспечивают PostgreSQL, повторная
публикация клиентом и Proxy, а также прикладные acceptance/result ACK.

## ADR-005 — Стандартный `net/http`

**Статус:** принято.

Гарантируется неизменность прикладных данных, но не побайтовая идентичность HTTP на
проводе. Redirect и прозрачная decompression отключаются; request/response body не
интерпретируются и не перекодируются.

## ADR-006 — Retry задает вызывающий сервис

**Статус:** принято.

По умолчанию запрос выполняется один раз. Caller может явно разрешить повторы и задать
их политику. Это исключает неявный повтор небезопасных write-операций, но не устраняет
unknown outcome при обрыве или падении во время HTTP-вызова.

## ADR-007 — Fan-out webhook через независимые deliveries

**Статус:** принято.

Webhook сначала сохраняется в PostgreSQL, после чего каждому подписчику создается
отдельная доставка с собственными retry, состоянием и ACK.

## ADR-008 — Асинхронный внутренний API

**Статус:** принято.

Команда, acceptance ACK, итоговый результат и result ACK являются отдельными
сообщениями. Acceptance ACK подтверждает durable-прием, но не завершение HTTP-вызова.
Webhook синхронно отвечает внешнему отправителю только после записи в PostgreSQL, а
внутренним подписчикам доставляется асинхронно.

## ADR-009 — Стандартный Go HTTP client без Proxy ordering

**Статус:** предложено после review; заменяет текущий черновой `client.Do`.

Клиент реализует `http.RoundTripper` и используется стандартным `http.Client`.
Одна goroutine естественно выполняет запросы последовательно; несколько goroutine
означают осознанную параллельность. Proxy не сортирует requests по timestamp, NATS
order или ACK order.

## ADR-010 — Result сначала клиенту, затем в БД Proxy

**Статус:** принято.

После HTTP-ответа Proxy сначала публикует result клиенту, затем сохраняет его локально.
Это дает две независимые durable boundary. Неудачная локальная запись не запускает
HTTP повторно; после потери in-memory result операция становится `unknown`.

## ADR-011 — Per-Proxy host throttling и connection pool

**Статус:** предложено после review; заменяет shared-replica limiter прототипа.

Каждый Proxy использует долгоживущий `net/http.Transport`. Его PostgreSQL координирует
RPS и concurrency limits только для этого Proxy.
Целевой limiter поддерживает также строгий `min_interval`; текущий fixed-window
limiter прототипа будет заменён при рефакторинге.

## ADR-012 — Два webhook response mode

**Статус:** принято.

Static mode отвечает после PostgreSQL commit заранее настроенным ответом. Delegated
mode ждет status/headers/body от одного назначенного NATS responder; остальные
subscribers получают независимые asynchronous deliveries.

## ADR-013 — Один Proxy, одна PostgreSQL

**Статус:** предложено после review.

Каждый Proxy имеет уникальный `proxy_id` и отдельную PostgreSQL. Несколько Proxy могут
работать в одном NATS, но не делят БД и subjects.

## ADR-014 — Callback через `http.Handler`

**Статус:** предложено после review.

Клиентский callback adapter вызывает стандартный `http.Handler`, чтобы существующую
HTTP-бизнес-логику можно было использовать без переписывания.

## ADR-015 — Автоматический `unknown`

**Статус:** предложено после review.

`unknown` доставляется клиенту как ошибка и затем очищается автоматически. Уже
сохранённый клиентом успешный результат не заменяется последующей ошибкой.

## ADR-016 — Разделение исходящего и callback Proxy

**Статус:** предложено после review.

Исходящий HTTP и callback реализуются в отдельных пакетах. Общими остаются только
небольшие protocol/security/repository primitives.
