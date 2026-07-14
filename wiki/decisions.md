# Архитектурные решения

## ADR-001 — PostgreSQL как durable source of truth

**Статус:** принято.

PostgreSQL хранит inbox, операции, попытки, leases, результаты, durable deliveries для
HTTP/webhook и прикладные ACK. Он обеспечивает восстановление после рестарта и
совместную работу нескольких реплик.

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

## ADR-009 — Синхронный Go client без Proxy ordering

**Статус:** принято.

`client.Do` блокируется до конечного результата. Одна goroutine естественно выполняет
запросы последовательно; несколько goroutine означают осознанную параллельность.
Proxy не сортирует requests по timestamp, NATS order или ACK order.

## ADR-010 — Result сначала клиенту, затем в БД Proxy

**Статус:** принято.

После HTTP-ответа Proxy сначала публикует result клиенту, затем сохраняет его локально.
Это дает две независимые durable boundary. Неудачная локальная запись не запускает
HTTP повторно; после потери in-memory result операция становится `unknown`.

## ADR-011 — Shared per-host throttling и connection pool

**Статус:** принято.

Каждый экземпляр использует долгоживущий `net/http.Transport`. PostgreSQL координирует
RPS и concurrency limits target host между всеми репликами логического Proxy.
Текущая реализация использует фиксированное секундное rate window; строгий
`min_interval` остается отдельным возможным расширением.

## ADR-012 — Два webhook response mode

**Статус:** принято.

Static mode отвечает после PostgreSQL commit заранее настроенным ответом. Delegated
mode ждет status/headers/body от одного назначенного NATS responder; остальные
subscribers получают независимые asynchronous deliveries.
