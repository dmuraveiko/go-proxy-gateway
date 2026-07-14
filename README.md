# HTTP–NATS Proxy

Production-oriented Go-модуль, который дает внутренним сервисам без прямого доступа
в интернет возможность выполнять произвольные HTTP/HTTPS-запросы через Core NATS.
PostgreSQL отвечает за durability, состояния, дедупликацию и повторную доставку.

Proxy не знает бизнес-схему запроса, не выбирает provider и не добавляет credentials.
Авторизованный клиент сам передает URL, method, headers и body.

Описание всей логики простыми словами, включая аварийные сценарии и примеры для
согласования: [документ для технического директора](docs/logic-for-tech-director.md).

## Что реализовано

- Core NATS без JetStream;
- PostgreSQL inbox и durable deliveries;
- Ed25519-подпись сообщений и ACL `client_id → proxy_id`;
- стабильный `request_id` и дедупликация повторных команд;
- двухсторонний acceptance/result ACK-протокол;
- универсальный HTTP executor на стандартном `net/http`;
- один долгоживущий connection pool на экземпляр Proxy;
- общий для всех реплик rate/concurrency limit по целевому host;
- безопасная retry-policy и состояние `unknown`;
- синхронный Go-клиент поверх асинхронного NATS-протокола;
- динамическая регистрация webhook через NATS;
- статический и delegated webhook response;
- несколько независимых webhook subscribers;
- leases, восстановление после рестарта, retention cleanup;
- health endpoints, Prometheus-метрики, Docker Compose и Kubernetes-манифест.

## Основной сценарий

```text
Service A                     Proxy                         Internet
   │                            │                              │
   │ сохранить request          │                              │
   │ publish HTTP command       │                              │
   ├───────────────────────────►│                              │
   │                            │ сохранить request в PG       │
   │◄──────── acceptance ACK ───┤                              │
   │ сохранить acceptance       │                              │
   ├──── acceptance ACK ───────►│                              │
   │                            │ сохранить ACK + dispatching  │
   │                            ├──────── HTTP request ───────►│
   │                            │◄────── HTTP response ────────┤
   │◄──────── result ───────────┤  сначала шанс клиенту        │
   │ сохранить result           │  затем сохранить result в PG │
   ├──────── result ACK ───────►│                              │
   │                            │ сохранить ACK                │
   │◄──── ACK confirmed ────────┤                              │
   │ сохранить completion       │                              │
```

### Почему ACK несколько

Core NATS не хранит сообщения, когда получатель недоступен. Поэтому сообщение можно
считать надежно переданным только после того, как получатель:

1. сохранил его в своей БД;
2. отправил прикладной ACK;
3. получил подтверждение, что отправитель сохранил ACK.

Потерянные промежуточные сообщения отправляются снова с теми же ID. Все операции
идемпотентны. Это конечный handshake: если последнее подтверждение потерялось, клиент
повторяет предыдущий ACK, а Proxy снова отвечает подтверждением.

## Синхронность для вызывающего Go-кода

NATS-протокол внутри асинхронный, но пакет [`client`](client/client.go) предоставляет
обычный блокирующий метод:

```go
result, err := proxyClient.Do(ctx, client.Request{
    RequestID: "payment-status-123",
    Method:    http.MethodGet,
    URL:       "https://api.example.com/v1/payments/123",
})
```

`Do` возвращается только после получения HTTP-результата и завершения result ACK.
Следующий вызов в той же goroutine естественно начнется позже. Для параллельной работы
caller явно запускает несколько goroutine.

Proxy намеренно не сортирует запросы одного клиента. Порядок NATS и ACK не используется
как бизнес-порядок; параллельные запросы считаются независимыми.

Production-клиент обязан реализовать интерфейс `client.Store` на своей БД. Исходящий
request сохраняется до первой публикации. `MemoryStore` предназначен только для тестов.

## Состояния HTTP-операции

```text
awaiting_acceptance_ack
          │ client durably confirmed acceptance
          ▼
        ready ──► reserved ──► dispatching ──► http_completed
                     │              │                  │
                     │ crash        │ lost outcome     │ result ACK
                     ▼              ▼                  ▼
                   ready          unknown       result_delivered
                                    │ result ACK
                                    ▼
                             result_delivered
```

- `awaiting_acceptance_ack` — Proxy сохранил request, но клиент еще не подтвердил ACK;
- `ready` — клиент знает, что повторять исходную команду больше не нужно;
- `reserved` — worker зарезервировал запись, но HTTP еще не начат;
- `dispatching` — флаг записан **до** физической HTTP-отправки;
- `http_completed` — ответ сохранен в PostgreSQL;
- `result_delivered` — клиент сохранил результат и подтвердил это;
- `unknown` — HTTP мог выполниться, но durable-результат восстановить нельзя.

Истекший `reserved` безопасно возвращается в `ready`: HTTP еще не начинался. Истекший
`dispatching` становится `unknown` и автоматически повторно не выполняется.

## Неустранимая граница HTTP ↔ PostgreSQL

HTTP-вызов внешнего сервера и запись в нашей БД не являются одной транзакцией.
Exactly-once на этой границе невозможен без участия внешнего API.

Принята модель «лучше не выполнить повторно, чем создать дубль»:

1. До HTTP Proxy фиксирует `dispatching`.
2. После ответа сначала публикует результат клиенту через NATS.
3. Затем сохраняет результат у себя.
4. Ошибка сохранения результата никогда сама по себе не запускает второй HTTP-вызов.
5. Пока процесс жив, он продолжает сохранять уже полученный результат.
6. Если процесс потерял результат, операция становится `unknown`.

Таким образом, результат получает два независимых шанса сохраниться: в БД клиента и в
БД Proxy. Если недоступны оба пути и Proxy упал, восстановить ответ невозможно.

## Retry

По умолчанию выполняется одна HTTP-попытка. Retry возможен только когда caller явно
передал политику и запрос безопасен:

- `GET`, `HEAD`, `OPTIONS`; или
- caller выставил `idempotent=true`, потому что внешний API поддерживает idempotency
  key, уже включенный клиентом в headers/body.

Caller отдельно указывает retry сетевых ошибок и список HTTP status codes. Proxy не
добавляет idempotency key самостоятельно и не понимает бизнес-смысл `POST`.

## HTTP-контракт

Request содержит:

```json
{
  "request_id": "request-123",
  "client_id": "service-a",
  "proxy_id": "proxy-main",
  "method": "POST",
  "url": "https://api.example.com/v1/items?full=true",
  "headers": [
    {"name": "Content-Type", "value": "application/json"},
    {"name": "X-Custom", "value": "one"},
    {"name": "X-Custom", "value": "two"}
  ],
  "body": "base64-encoded-by-json",
  "timeout": 30000000000,
  "retry": {"max_attempts": 1}
}
```

Headers представлены списком, чтобы не потерять повторяющиеся значения. Body —
непрозрачные bytes; стандартный JSON encoder передает `[]byte` как base64.

Proxy сохраняет method, URL/query, значения end-to-end headers и body. В результате
сохраняются status code, значения headers и response body. Proxy не декодирует JSON,
не распаковывает gzip и не следует за redirect автоматически.

Стандартный `net/http` может изменить wire-представление: порядок и регистр headers,
`Content-Length`, chunked framing и управление соединением. Побайтовая идентичность
TCP-потока не является гарантией. Интеграция, подписывающая точный wire-пакет вместе с
порядком headers, требует отдельного raw transport.

## Connection pool

На экземпляр Proxy создается один долгоживущий `http.Client`/`http.Transport`.
Keep-alive включен, idle-соединения с тем же host используются повторно, HTTP/2 может
мультиплексировать запросы. Response body всегда полностью читается и закрывается,
чтобы соединение вернулось в pool. Если внешний сервер сам закрыл соединение, будет
создано новое; гарантировать одно физическое соединение навсегда невозможно.

## Ограничение нагрузки по host

Конфигурация задает default и overrides:

```env
PROXY_DEFAULT_HOST_RPS=20
PROXY_DEFAULT_HOST_CONCURRENCY=8
PROXY_HOST_LIMITS=api.example.com=5:2;legacy.example.org:8443=1:1
```

PostgreSQL координирует rate window и concurrent permits между всеми репликами одного
логического Proxy. Если permit пока недоступен, request остается в очереди. Это не
whitelist: неизвестный host использует default limit.

Текущий rate limit считает запросы в фиксированном секундном окне. Поэтому около
границы двух секунд возможен короткий burst. Строгая минимальная пауза между запросами
пока не реализована и должна быть отдельно согласована для provider-ов, которым она
нужна.

## Несколько Proxy и клиентов

Клиент явно выбирает логический `proxy_id`. NATS subjects изолированы по нему, а Proxy
проверяет Ed25519 identity и `PROXY_ALLOWED_CLIENTS`.

Несколько физических экземпляров одного `proxy_id` используют:

- одинаковую ACL и signing identity;
- общий PostgreSQL;
- одну NATS queue group;
- отдельные локальные HTTP connection pools;
- общий DB-backed host limiter.

Повтор одного request всегда идет с тем же `request_id` и к тому же `proxy_id`.
Автоматически переключать незавершенный небезопасный request на другой логический
Proxy нельзя: второй Proxy может повторить внешний side effect.

## NATS subjects

| Направление | Subject |
|---|---|
| client → Proxy | `proxy.<proxy_id>.requests` |
| Proxy → client | `client.<client_id>.proxy.<proxy_id>.accepted` |
| client → Proxy | `proxy.<proxy_id>.accepted_acks` |
| Proxy → client | `client.<client_id>.proxy.<proxy_id>.results` |
| client → Proxy | `proxy.<proxy_id>.result_acks` |
| Proxy → client | `client.<client_id>.proxy.<proxy_id>.ack_confirmed` |
| webhook control | `proxy.<proxy_id>.webhooks.commands` |
| webhook event | `client.<client_id>.proxy.<proxy_id>.webhooks.events` |
| webhook ACK | `proxy.<proxy_id>.webhooks.acks` |

NATS server ACL следует настроить дополнительно: клиент публикует только в subjects
выбранного Proxy и читает только собственный `client.<client_id>.>`.

## Ed25519

Каждое сообщение помещается в envelope:

```json
{
  "id": "random-envelope-id",
  "type": "http.request.v1",
  "timestamp": "2026-07-14T10:00:00Z",
  "payload": {},
  "key_id": "derived-key-id",
  "signature": "base64-signature"
}
```

Подписываются ID, type, UTC timestamp и точные bytes JSON payload. Proxy связывает
public key с `client_id`, проверяет freshness и разрешение клиента для `proxy_id`.
Private keys не должны храниться в репозитории.

## Webhook

### Регистрация

Owner отправляет подписанную `webhook.register.v1` в webhook control subject. Команда
содержит стабильный `command_id`, mode, subscribers, лимит body и настройки ответа.
Proxy идемпотентно создает route и возвращает capability URL:

```text
https://proxy.example.com/v1/webhooks/<webhook_id>/<secret-token>
```

Результат register сейчас публикуется через Core NATS без отдельной durable delivery.
Если он потерялся, owner повторяет ту же команду с тем же `command_id` и получает тот
же URL. Subscribe/delete выполняются идемпотентно, но отдельного success ACK для них
пока нет; полноценный control ACK-протокол остается production-доработкой.

Owner самостоятельно регистрирует этот URL у внешнего provider. Команды subscribe и
delete доступны только owner; подписчиками могут быть только клиенты ACL этого Proxy.
На текущем этапе control API поддерживает register, subscribe и delete. List/get,
update, unsubscribe и rotation capability token в текущий scope не входят.

Публичный webhook endpoint принимает `POST`. Поддержка других HTTP methods требует
отдельного расширения контракта.

### Static mode

```text
provider → Proxy → PostgreSQL commit → configured HTTP response
                         └──────────→ durable NATS fan-out
```

Proxy отвечает наружу только после DB commit. Каждому subscriber создается отдельная
delivery и отдельный ACK. Недоступность одного subscriber не влияет на остальных.

### Delegated mode

```text
provider → Proxy → PostgreSQL → designated responder over NATS
provider ← Proxy ← status + headers + body
```

Один клиент назначается responder. Внешнее HTTP-соединение остается открытым до его
ответа. Остальные subscribers получают событие асинхронно. При timeout Proxy отвечает
`504`, а provider может повторить callback. Повтор должен дедуплицироваться внутренним
сервисом по provider event ID или бизнес-ключу; универсальный Proxy этого ID не знает.

Provider-specific подпись проверяет сервис-получатель по переданным body и headers.
Capability token защищает сам route. Подключаемые provider-specific auth handlers в
текущую универсальную версию не входят.

## Retention

Фоновая очистка небольшими batches удаляет только завершенные HTTP-операции после
`PROXY_RETENTION`. Активные и недоставленные records не удаляются. `unknown` по
умолчанию хранится бессрочно; отдельный TTL включается явно. Webhook event удаляется
только когда все его deliveries подтверждены и истек retention.

При очень большом потоке следующим шагом будет date partitioning таблиц; текущая
batch-очистка не берет длительные table locks.

## Запуск локально

```bash
docker compose up -d --build
curl -i http://localhost:8080/health/ready
curl http://localhost:8080/metrics
```

Compose запускает PostgreSQL, обычный NATS без JetStream, Proxy и тестовый HTTP echo.
Production migrations выполняются отдельно:

```bash
proxy migrate
```

В репозитории оставлена одна актуальная миграция `000001_initial`, которая сразу
создает финальную схему. Она рассчитана на чистую БД. Если в окружении уже применялась
старая версия миграции `000001`, нельзя просто заменить файл: migration runner сочтет
ее выполненной. Для такой среды нужен отдельный переходный migration/export либо новая
БД. Локальную БД без нужных данных можно пересоздать командой `docker compose down -v`
и затем снова запустить Compose.

## Проверки

```bash
make fmt
make vet
make test
make build
```

Интеграционный smoke с реальными NATS/PostgreSQL:

```bash
docker compose up -d --build
docker run --rm --network proxy-server_default \
  -e PROXY_INTEGRATION_NATS_URL=nats://nats:4222 \
  -e PROXY_INTEGRATION_HTTP_URL=http://echo:5678/ \
  -e PROXY_INTEGRATION_PROXY_URL=http://proxy:8080 \
  -v "$PWD:/src" -w /src golang:1.25-alpine \
  go test -run Integration -v ./client
```

Smoke покрывает исходящий HTTP с полным ACK-handshake, static webhook и delegated
webhook response.

## Наблюдаемость

- `GET /health/live` — процесс жив;
- `GET /health/ready` — HTTP listener, NATS и PostgreSQL готовы;
- `GET /metrics` — requests по состояниям и pending deliveries;
- JSON logs содержат `request_id`/`delivery_id`, но не request body или secrets.

Алерты должны следить за ростом `ready`, `dispatching`, `unknown`, pending deliveries,
ошибками PostgreSQL/NATS, HTTP latency и насыщением host limits.

## Гарантии и ограничения

Гарантируется:

- после acceptance ACK request сохранен;
- повтор request с тем же ID не создает вторую операцию;
- HTTP не начинается до durable `dispatching`;
- ошибка записи результата не запускает повторный HTTP автоматически;
- сохраненный результат и webhook доставляются до ACK;
- restart-safe работа через PostgreSQL leases;
- at-least-once доставка NATS-сообщений с возможными дублями.

Не гарантируется:

- exactly-once внешний HTTP side effect;
- побайтовая идентичность HTTP wire-пакета;
- сохранение результата, если одновременно недоступны БД Proxy, клиент и падает процесс;
- безопасный retry write-запроса без idempotency внешнего API;
- порядок между параллельными goroutine;
- возможность запросить статус или отменить операцию через отдельный management API —
  такой API пока не реализован;
- строгая пауза между запросами к host на границе секундного rate window;
- ограничение destination: авторизованный клиент может обратиться к любому адресу,
  включая внутренние адреса и metadata endpoints, доступные из сети Proxy.

Последний пункт является осознанным бизнес-требованием. Его следует компенсировать
строгими Ed25519/NATS ACL, аудитом, лимитами и сетевой сегментацией окружения.

## Дополнительная документация

- [Логика системы для технического директора](docs/logic-for-tech-director.md)
- [Архитектура](wiki/architecture.md)
- [Требования](wiki/requirements.md)
- [Архитектурные решения](wiki/decisions.md)
- [Открытые production-вопросы](wiki/open-questions.md)
- [Production checklist](docs/production-checklist.md)
- [Operations runbook](docs/runbook.md)
- [SLO](docs/slo.md)
- [Security policy](SECURITY.md)
