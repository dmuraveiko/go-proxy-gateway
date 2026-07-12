# Integration Gateway: надежный мост HTTP ↔ NATS

Integration Gateway — инфраструктурный Go-сервис между изолированным внутренним контуром NATS и внешним интернетом. Он не является открытым HTTP-прокси: внутренний сервис не может передать произвольный URL, HTTP-метод или набор credentials. Вместо этого он публикует типизированную команду, а gateway выбирает заранее зарегистрированный handler, который владеет endpoint, авторизацией, схемой, лимитами и правилами повторов.

Модуль решает два симметричных сценария:

1. Внутренний сервис отправляет команду через NATS JetStream. Gateway надежно сохраняет ее, вызывает внешний API и публикует типизированный результат.
2. Внешний provider вызывает webhook. Gateway проверяет подпись, валидирует и дедуплицирует событие, затем надежно публикует внутреннее NATS-событие.

Система рассчитана на перезапуски, временную недоступность интернета, повторную доставку сообщений, горизонтальное масштабирование и контролируемую деградацию внешних API.

## Содержание

- [Границы ответственности](#границы-ответственности)
- [Архитектура](#архитектура)
- [Гарантии доставки](#гарантии-доставки)
- [NATS-контракты](#nats-контракты)
- [Жизненный цикл команды](#жизненный-цикл-команды)
- [Webhook pipeline](#webhook-pipeline)
- [Повторы и классификация ошибок](#повторы-и-классификация-ошибок)
- [Идемпотентность](#идемпотентность)
- [JetStream](#jetstream)
- [PostgreSQL](#postgresql)
- [Безопасность](#безопасность)
- [Масштабирование и backpressure](#масштабирование-и-backpressure)
- [Конфигурация](#конфигурация)
- [Запуск](#запуск)
- [Добавление интеграции](#добавление-интеграции)
- [Наблюдаемость](#наблюдаемость)
- [Отказы и восстановление](#отказы-и-восстановление)
- [Ограничения](#ограничения)

## Границы ответственности

Gateway отвечает за:

- durable-прием команд и webhook-событий;
- аутентификацию и авторизацию NATS-команд;
- аутентификацию webhook provider-а;
- выбор версионированного integration handler-а;
- безопасный HTTP-доступ только к настроенным endpoint;
- timeout, rate limit, circuit breaker и retry policy;
- дедупликацию команд и событий;
- хранение состояния выполнения;
- публикацию результатов через transactional outbox;
- DLQ для окончательно неуспешных или невалидных сообщений;
- эксплуатационные health checks и метрики.

Gateway не отвечает за:

- бизнес-решение о достоверности blockchain-транзакции;
- сравнение результатов нескольких blockchain providers;
- хранение wallet private keys;
- подписание blockchain-транзакций;
- exactly-once side effects во внешних системах;
- автоматический повтор финансовой операции с неизвестным результатом;
- произвольный доступ внутренних сервисов в интернет.

## Архитектура

```text
┌─────────────┐       signed command        ┌────────────────────┐
│  Service A  │ ──────────────────────────> │ PROXY_COMMANDS     │
└─────────────┘          JetStream           └─────────┬──────────┘
                                                      │ durable pull
                                                      ▼
                                             ┌───────────────────┐
                                             │ Command consumer  │
                                             │ verify + persist  │
                                             └─────────┬─────────┘
                                                       │ PostgreSQL
                                                       ▼
                                             ┌───────────────────┐
                                             │ Operation workers │
                                             └─────────┬─────────┘
                                                       │ typed handler
                                                       ▼
                                             ┌───────────────────┐
                                             │ External provider │
                                             └─────────┬─────────┘
                                                       │ result/error
                                                       ▼
                                             ┌───────────────────┐
                                             │ DB transaction:   │
                                             │ state + outbox    │
                                             └─────────┬─────────┘
                                                       │ parallel publishers
                         ┌─────────────────────────────┴────────────────────┐
                         ▼                                                  ▼
               ┌──────────────────┐                              ┌──────────────────┐
               │ PROXY_RESULTS    │                              │ PROXY_DLQ        │
               └──────────────────┘                              └──────────────────┘

External provider ──HTTP webhook──> auth/parse/map ──DB dedup + outbox──> PROXY_EVENTS
```

Код разделен по ответственности:

```text
cmd/proxy/                         composition root и lifecycle
internal/app/                      HTTP server, probes, webhook ingress, metrics
internal/config/                   env-конфигурация и fail-fast validation
internal/contracts/                Command, Result, Event, Problem
internal/integration/              интерфейсы handlers и registry
internal/integrations/             provider-specific адаптеры
internal/message/                  signed envelope и ed25519
internal/repository/               PostgreSQL state/inbox/outbox/migrations
internal/security/                 URL allowlist и SSRF protection
internal/transport/                JetStream streams, consumer, publisher
internal/worker/                   execution pool, retries, state transitions
```

## Гарантии доставки

Система использует семантику **at least once**.

Что гарантируется:

- JetStream не удаляет команду до подтверждения consumer-а.
- Consumer подтверждает команду только после идемпотентной записи в PostgreSQL.
- `command.id` является primary key и защищает от повторной регистрации одной операции.
- Результат и outbox-запись создаются в одной PostgreSQL-транзакции.
- Webhook event и его outbox-запись создаются в одной транзакции.
- Если процесс упал после DB commit, но до NATS publish, outbox publisher продолжит работу после рестарта.
- Если процесс упал после publish, но до `published_at`, сообщение может быть опубликовано повторно. JetStream `Nats-Msg-Id` уменьшает вероятность дубля в duplicate window, но consumers все равно должны дедуплицировать сообщения.
- Processing lease позволяет другому экземпляру подобрать зависшую операцию.

Что не гарантируется:

- Exactly-once HTTP-вызов внешнего API.
- Отсутствие дублей за пределами JetStream duplicate window.
- Автоматическая безопасность повторов внешних write-операций без idempotency key provider-а.

## NATS-контракты

### Envelope

Каждое production-сообщение помещается в подписанный envelope:

```json
{
  "id": "unique-envelope-id",
  "type": "integration.command",
  "timestamp": "2026-07-12T12:00:00Z",
  "payload": {},
  "key_id": "derived-public-key-id",
  "signature": "base64-ed25519-signature"
}
```

Подписывается точная последовательность:

```text
envelope.id + "\n" +
envelope.type + "\n" +
timestamp in RFC3339Nano UTC + "\n" +
raw JSON payload
```

Gateway проверяет:

- наличие ID, типа и timestamp;
- допустимое временное окно;
- соответствие `key_id` фактическому публичному ключу;
- ed25519 signature;
- право ключа выполнять конкретный тип команды.

### Command

```json
{
  "id": "operation-unique-id",
  "correlation_id": "business-flow-id",
  "causation_id": "previous-message-id",
  "traceparent": "00-trace-span-flags",
  "tenant_id": "optional-tenant",
  "type": "blockchain.transaction_status.get",
  "version": 1,
  "payload": {
    "transaction_id": "abc123"
  },
  "created_at": "2026-07-12T12:00:00Z"
}
```

Обязательные поля:

- `id` — стабильный idempotency key операции;
- `correlation_id` — идентификатор бизнес-цепочки;
- `type` — логический тип команды;
- `version` — версия схемы;
- `payload` — provider-independent входные данные.

URL, API key, HTTP method и callback URL в команду не входят.

### Result

```json
{
  "command_id": "operation-unique-id",
  "correlation_id": "business-flow-id",
  "type": "blockchain.transaction_status.get.result",
  "status": "succeeded",
  "payload": {},
  "attempts": 1,
  "finished_at": "2026-07-12T12:00:01Z"
}
```

Ошибка:

```json
{
  "command_id": "operation-unique-id",
  "status": "failed",
  "error": {
    "code": "provider_rejected",
    "message": "provider returned 400 Bad Request",
    "retryable": false
  },
  "attempts": 1
}
```

### Subjects

```text
proxy.commands.<domain>
proxy.results.<command-type>
proxy.events.<internal-event-type>
proxy.dlq.<command-type>
proxy.dlq.rejected
```

## Жизненный цикл команды

1. Producer создает Command и подписанный Envelope.
2. Producer публикует envelope в `proxy.commands.*` через JetStream publish и получает server ACK.
3. Durable pull consumer получает команду.
4. Gateway проверяет envelope, signature, key ID и permissions.
5. Контракт команды валидируется на базовом уровне.
6. Команда вставляется в `proxy_operations` через `ON CONFLICT DO NOTHING`.
7. Только после успешной DB-операции выполняется `AckSync` исходного NATS-сообщения.
8. Worker атомарно захватывает due operation через `FOR UPDATE SKIP LOCKED` и устанавливает lease.
9. Registry выбирает handler по паре `(type, version)`.
10. Handler валидирует payload и вызывает внешний provider.
11. При transient error операция переводится в `retrying` с новым `next_attempt_at`.
12. При успехе или terminal error состояние и outbox создаются в одной транзакции.
13. Один из параллельных outbox publishers захватывает запись по lease.
14. Результат подписывается gateway-ключом и публикуется в JetStream.
15. После JetStream ACK outbox помечается опубликованным.

## Webhook pipeline

Endpoint:

```http
POST /v1/webhooks/{provider}
```

Базовый HMAC adapter ожидает:

```http
X-Webhook-Signature: sha256=<hex-hmac-sha256(raw-body)>
Content-Type: application/json
```

Payload примера:

```json
{
  "id": "provider-event-id",
  "type": "provider.transaction.confirmed",
  "payload": {}
}
```

Последовательность:

1. HTTP body ограничивается по размеру.
2. Provider выбирается только из зарегистрированного registry.
3. Подпись проверяется над raw body до JSON parsing.
4. Provider event type отображается в внутренний тип через explicit mapping.
5. Неизвестный тип отклоняется fail-closed.
6. `event.id` записывается в PostgreSQL как primary key.
7. При первом событии одновременно создается outbox-запись.
8. Повторный webhook с тем же ID считается успешно обработанным, но второй event не публикуется.
9. HTTP `202 Accepted` возвращается только после DB commit.
10. Outbox независимо публикует event в `proxy.events.*`.

Для реального provider-а generic adapter следует заменить отдельным handler-ом с его canonical signature, timestamp, nonce и правилами rotation.

## Повторы и классификация ошибок

Handler возвращает один из двух классов ошибок:

```go
integration.Permanent("provider_rejected", err)
integration.Retryable("provider_unavailable", err)
```

Повторяются:

- сетевые ошибки;
- DNS/TLS/connect failures;
- provider HTTP 429;
- provider HTTP 5xx;
- временно невалидный/неполный ответ;
- открытый circuit breaker;
- временная недоступность глобального limiter-а.

Не повторяются автоматически:

- ошибка схемы команды;
- HTTP 4xx, кроме 429;
- запрещенный command type;
- неизвестная версия handler-а;
- невалидная подпись;
- потенциально опасная write-операция с неизвестным результатом.

Backoff экспоненциальный, ограниченный `MaxBackoff`, с jitter. После `MaxAttempts` создаются два сообщения: обычный failure result для caller-а и копия в DLQ для operations team.

## Идемпотентность

Есть три независимых ключа:

- `command.id` — дедупликация входящих операций;
- `event.id` — дедупликация webhook events;
- `outbox.id` — дедупликация публикаций результата/события.

Для read-only integrations повтор HTTP-вызова обычно безопасен. Для write/payment integrations handler обязан передать `command.id` как idempotency key внешнему provider-у и сохранить provider operation ID.

Если внешний write-запрос завершился timeout-ом, это не означает, что операция не была выполнена. Такой случай должен переходить в `outcome_unknown`, затем проверяться reconciliation handler-ом. Автоматический повтор выплаты без проверки запрещен архитектурно.

## JetStream

Gateway управляет четырьмя stream-ами:

| Stream | Subjects | Назначение | Default retention |
|---|---|---|---|
| `PROXY_COMMANDS` | `proxy.commands.>` | Входящие команды | 7 дней |
| `PROXY_RESULTS` | `proxy.results.>` | Результаты выполнения | 30 дней |
| `PROXY_EVENTS` | `proxy.events.>` | Входящие внешние события | 30 дней |
| `PROXY_DLQ` | `proxy.dlq.>` | Ошибки и rejected messages | 90 дней |

Для всех stream-ов используются:

- file storage;
- ограничение общего размера;
- ограничение размера одного сообщения;
- `DiscardNew`, чтобы переполнение было видимым, а старые необработанные команды не удалялись молча;
- duplicate window 24 часа;
- три реплики по умолчанию.

При старте gateway создает отсутствующие streams и reconciles изменяемые параметры существующих. Несовместимое изменение subjects приводит к fail-fast. Durable consumer также создается или обновляется до желаемой конфигурации.

Локальный compose использует одну реплику и уменьшенный storage budget. Это dev-настройка, не production topology.

## PostgreSQL

### `proxy_operations`

Source of truth выполнения команды. Статусы:

```text
pending -> processing -> completed
                     \-> retrying -> processing
                     \-> failed
                     \-> outcome_unknown -> manual_review/reconciliation
```

Lease предотвращает вечный `processing`: после истечения запись может подобрать другой worker. Lease выбирается длиннее HTTP request timeout.

### `proxy_webhook_events`

Хранит ID, provider, внутренний тип и raw normalized event. Primary key обеспечивает дедупликацию.

### `proxy_outbox`

Хранит еще не опубликованные NATS messages. Параллельные publishers используют `SKIP LOCKED` и lease.

### `proxy_rate_limits`

Глобальный секундный rate limiter. В отличие от локального limiter-а он ограничивает суммарный RPS всех gateway replicas.

### Миграции

Миграции встроены в binary, но применяются отдельной командой:

```bash
proxy migrate
```

Gateway по умолчанию не меняет schema при обычном старте. Если schema отсутствует, процесс завершается с понятной ошибкой. `PROXY_AUTO_MIGRATE=true` допустим для локальной разработки, но не рекомендуется для production rollout.

## Безопасность

### NATS

- TLS CA verification;
- optional mTLS client certificate;
- NATS credentials/NKey/JWT;
- ed25519 application-level envelope signatures;
- key-specific command permissions;
- рекомендуемые subject ACL на стороне NATS account.

Пример permissions:

```text
<key-id>=blockchain.*|provider.health.get;<other-key-id>=payments.status.get
```

Wildcard `blockchain.*` разрешает только указанный namespace. Неизвестный key ID или command type отклоняется.

### HTTP/SSRF

- endpoint задается конфигурацией handler-а;
- разрешен только HTTPS;
- redirects запрещены;
- DNS-адреса private, loopback, link-local, multicast и unspecified запрещены;
- transport подключается к проверенному IP, снижая риск DNS rebinding;
- hop-by-hop/proxy поведение не управляется producer-ом;
- response body ограничен.

### Webhooks

- signature проверяется constant-time сравнением;
- подпись считается по raw body;
- provider выбирается из registry;
- event types отображаются explicit allowlist-ом;
- event ID дедуплицируется;
- неизвестные providers/events отклоняются.

### Secrets

В repository нельзя хранить:

- NATS seeds;
- ed25519 private keys;
- provider API keys;
- webhook secrets;
- wallet keys;
- production payloads.

В production secrets должны поступать из Vault/KMS/secret manager. Контейнер запускается non-root, с read-only filesystem и без Linux capabilities.

## Масштабирование и backpressure

Масштабирование происходит на четырех уровнях:

- количество gateway replicas;
- `PROXY_WORKERS` — параллельные integration executions на replica;
- `PROXY_OUTBOX_WORKERS` — параллельные publishers на replica;
- PostgreSQL pool и JetStream `MaxAckPending`.

`FOR UPDATE SKIP LOCKED` распределяет operations и outbox между процессами без центрального coordinator-а. Локальный token bucket сглаживает burst одной replica, PostgreSQL limiter ограничивает общий provider RPS.

Backpressure обеспечивают:

- JetStream stream limits и `DiscardNew`;
- `MaxAckPending` durable consumer-а;
- bounded worker count;
- bounded outbox publisher count;
- PostgreSQL connection pool;
- provider rate limiter;
- circuit breaker.

Увеличивать workers без увеличения provider quota и DB capacity нельзя: это уменьшит latency очереди, но увеличит 429, contention и количество retries.

## Конфигурация

Полный шаблон находится в [.env.example](.env.example).

### Сервис и PostgreSQL

| Переменная | Default | Описание |
|---|---:|---|
| `PROXY_HTTP_ADDR` | `:8080` | HTTP listen address |
| `PROXY_DATABASE_URL` | required | PostgreSQL DSN |
| `PROXY_DB_MAX_CONNS` | `32` | Максимум соединений одной replica |
| `PROXY_WORKERS` | `16` | Operation workers |
| `PROXY_OUTBOX_WORKERS` | `4` | Outbox publishers |
| `PROXY_REQUEST_TIMEOUT` | `30s` | Timeout provider request |
| `PROXY_SHUTDOWN_TIMEOUT` | `30s` | Максимальное graceful drain time |
| `PROXY_RETENTION` | `720h` | DB retention completed data |
| `PROXY_AUTO_MIGRATE` | `false` | Автомиграции при старте |

### NATS и JetStream

| Переменная | Default | Описание |
|---|---:|---|
| `PROXY_NATS_URL` | `nats://127.0.0.1:4222` | NATS servers URL |
| `PROXY_NATS_CREDS_FILE` | empty | NATS credentials file |
| `PROXY_NATS_CA_CERT` | empty | CA bundle |
| `PROXY_NATS_CLIENT_CERT` | empty | mTLS certificate |
| `PROXY_NATS_CLIENT_KEY` | empty | mTLS private key |
| `PROXY_STREAM_REPLICAS` | `3` | Stream/consumer replicas |
| `PROXY_STREAM_MAX_BYTES` | `10 GiB` | Лимит каждого stream |
| `PROXY_MAX_MESSAGE_BYTES` | `2 MiB` | Максимум одного сообщения |
| `PROXY_ACK_WAIT` | `1m` | Consumer ACK deadline |
| `PROXY_MAX_ACK_PENDING` | `1024` | Consumer backpressure |
| `PROXY_FETCH_BATCH` | `64` | Pull batch size |

### Подписи

| Переменная | Описание |
|---|---|
| `PROXY_REQUIRE_SIGNATURE` | Обязательность signatures; production — `true` |
| `PROXY_SIGNING_PRIVATE_KEY_FILE` | Gateway ed25519 private key |
| `PROXY_VERIFY_PUBLIC_KEYS` | Base64 public keys через запятую |
| `PROXY_KEY_PERMISSIONS` | key ID → allowed command patterns |

### Provider example

| Переменная | Описание |
|---|---|
| `PROXY_TRANSACTION_STATUS_ENDPOINT` | Fixed HTTPS endpoint |
| `PROXY_PROVIDER_API_KEY_HEADER` | Имя auth header |
| `PROXY_PROVIDER_API_KEY` | Provider credential |
| `PROXY_PROVIDER_RPS` | Глобальный и локальный RPS limit |
| `PROXY_WEBHOOK_PROVIDER` | Имя webhook provider-а |
| `PROXY_WEBHOOK_SECRET` | HMAC secret |
| `PROXY_WEBHOOK_EVENT_TYPES` | `external.type=internal.type;...` |

## Запуск

### Docker Compose

```bash
docker compose up -d --build
curl --fail http://localhost:8080/health/ready
curl --fail http://localhost:8080/metrics
```

Compose выполняет migrations отдельным one-shot контейнером, затем запускает gateway.

Остановка:

```bash
docker compose down
```

Удаление dev-данных:

```bash
docker compose down -v
```

### Локальный Go

```bash
cp .env.example .env
go run ./cmd/proxy migrate
go run ./cmd/proxy
```

### Проверки

```bash
make fmt
make test
make vet
make build
```

Расширенная проверка:

```bash
go test -race ./...
go test -run=Fuzz -fuzz=Fuzz ./internal/message
```

## Добавление интеграции

### Command handler

Новый handler реализует:

```go
type CommandHandler interface {
    Type() string
    Version() int
    Validate(json.RawMessage) error
    Execute(context.Context, json.RawMessage) (json.RawMessage, error)
    RetryPolicy() RetryPolicy
}
```

Правила:

1. Payload должен быть provider-independent.
2. Handler обязан декодировать JSON в строгую входную структуру.
3. Endpoint и credentials берутся только из configuration/secrets.
4. HTTP client обязан использовать timeout, SSRF policy и запрет redirects.
5. Ошибки должны быть классифицированы как permanent или retryable.
6. Write handler обязан использовать внешний idempotency key.
7. Логи не должны содержать secrets или полный чувствительный payload.
8. Изменение контракта требует новой `Version()`, а не ломающего изменения v1.

Регистрация выполняется в composition root `cmd/proxy/main.go`.

### Webhook handler

```go
type WebhookHandler interface {
    Provider() string
    Authenticate(*http.Request, []byte) error
    Parse([]byte) ([]contracts.Event, error)
}
```

Provider-specific handler должен:

- проверять documented canonical signature;
- проверять timestamp/nonce и replay window;
- поддерживать rotation двух активных secrets;
- использовать стабильный provider event ID;
- преобразовывать только явно разрешенные event types;
- не публиковать provider payload напрямую без normalization.

## Наблюдаемость

Endpoints:

```text
GET /health/live   — процесс и HTTP loop живы
GET /health/ready  — PostgreSQL доступен и JetStream consumer активен
GET /metrics       — Prometheus text format
```

Текущие метрики:

```text
proxy_operations{status="pending"}
proxy_operations{status="retrying"}
proxy_operations{status="processing"}
proxy_operations{status="failed"}
proxy_oldest_pending_seconds
proxy_outbox_pending
```

Критичные alerts:

- возраст oldest pending выше SLA;
- рост outbox backlog;
- появление failed operations;
- рост DLQ;
- заполнение JetStream storage;
- consumer lag;
- PostgreSQL pool saturation;
- provider 429/5xx/timeout rate;
- signature failures.

JSON-логи предназначены для централизованного сбора. `correlation_id`, `command_id`, provider и attempt следует передавать во все связанные записи без логирования секретов.

## Отказы и восстановление

### Gateway перезапустился

JetStream повторно доставит не подтвержденные команды. PostgreSQL primary key устранит повторную регистрацию. Processing operation вернется в работу после lease expiry. Outbox продолжит неопубликованные сообщения.

### NATS недоступен

Новые NATS-команды не принимаются. Уже сохраненные operations могут выполняться, а результаты накапливаются в outbox. Readiness становится отрицательной. После восстановления publishers выгружают backlog.

### PostgreSQL недоступен

Command consumer не подтверждает сообщения, webhook отвечает 503, workers не могут захватить операции. JetStream сохраняет команды до восстановления DB.

### Provider недоступен

Handler возвращает retryable error, включается backoff. После серии ошибок circuit breaker временно прекращает реальные запросы. Команды остаются durable.

### Процесс упал во время HTTP-запроса

Результат мог быть неизвестен. После lease expiry операция будет получена повторно. Для write integrations безопасность зависит от внешнего idempotency key/reconciliation.

### Stream заполнен

`DiscardNew` отклоняет новые публикации явной ошибкой. Старые данные не удаляются молча. Operations team должна увеличить capacity или устранить consumer lag, не выполняя необдуманный purge.

Подробные процедуры находятся в [docs/runbook.md](docs/runbook.md), production checklist — в [docs/production-checklist.md](docs/production-checklist.md), SLO — в [docs/slo.md](docs/slo.md).

## Ограничения

- Generic transaction-status integration является инфраструктурным примером, а не готовым TronGrid/Tatum contract.
- Generic webhook HMAC не заменяет provider-specific canonical signature.
- Exactly-once delivery не заявляется.
- Состояния `outcome_unknown` и `manual_review` заложены в schema, но конкретный reconciliation workflow появляется вместе с write/payment integration.
- Сравнение blockchain-ответов двух providers должно выполняться отдельной доменной интеграцией или сервисом.
- Локальный compose не моделирует quorum трехузлового JetStream и HA PostgreSQL.
- Метрики покрывают основные очереди; provider latency histograms и полноценный distributed tracing следует подключать вместе с production observability stack.

## Дополнительная документация

- [Архитектурные решения](wiki/decisions.md)
- [Требования](wiki/requirements.md)
- [Открытые вопросы](wiki/open-questions.md)
- [Production checklist](docs/production-checklist.md)
- [Operations runbook](docs/runbook.md)
- [SLO](docs/slo.md)
- [Security policy](SECURITY.md)
