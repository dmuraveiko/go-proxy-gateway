# HTTP–NATS Proxy

Proxy позволяет внутренним сервисам без прямого доступа в интернет выполнять HTTP
через NATS и принимать внешние callback.

Proxy универсальный: клиент сам передаёт адрес, method, headers и body. Proxy не знает
бизнес-смысл запроса, не добавляет credentials и не ограничивает список внешних
адресов.

Простое описание всей логики находится в [docs/logic.md](docs/logic.md). Именно этот
документ является основой для согласования поведения системы.

## Статус

Репозиторий содержит реализацию согласованного фундамента:

- один логический `proxy_id` использует одну PostgreSQL и может иметь несколько
  физических экземпляров с NATS queue group и DB leases;
- клиентская библиотека содержит готовое PostgreSQL-хранилище с таблицами
  `natsproxyclient_*`;
- исходящий клиентский API реализует `http.RoundTripper`;
- callback API принимает обычный `http.Handler`;
- исходящий и callback-код разделены по файлам;
- `unknown` автоматически возвращается клиенту как ошибка и очищается после
  завершения доставки, без ручного workflow;
- integration tests проверяют исходящий HTTP и оба callback-режима в окружении с
  двумя экземплярами одного Proxy; unit test проверяет неизменность URL/body.

## Основные решения

- Core NATS используется как транспорт, без JetStream.
- PostgreSQL хранит очередь, состояния и недоставленные результаты.
- Сообщения подписываются Ed25519.
- Клиент явно выбирает Proxy по `proxy_id`.
- Повторные сообщения дедуплицируются по стабильному ID.
- HTTP по умолчанию выполняется один раз.
- Proxy использует стандартный `net/http`.
- Содержимое HTTP-запроса и ответа не интерпретируется Proxy.

## Базы данных

Один логический `proxy_id` соответствует одной PostgreSQL:

```text
Proxy A ──► PostgreSQL A
Proxy B ──► PostgreSQL B
```

Несколько физических экземпляров `proxy-a` могут работать с PostgreSQL A. NATS queue
group распределяет сообщения, а DB leases/locks не дают выполнить одну операцию дважды.
У каждого процесса есть уникальный `instance_id`.

Proxy A и Proxy B могут быть подключены к одному NATS, но используют разные
`proxy_id` и разные БД.

Клиентская библиотека содержит готовое PostgreSQL-хранилище. Приложение передаёт DSN
или connection pool, а библиотека создаёт свои таблицы с префиксом
`natsproxyclient_`. Префикс настраивается, поэтому таблицы не пересекаются с таблицами
приложения. Самостоятельно реализовывать storage-интерфейс не требуется.

Для согласованной схемы нужны обе durable-стороны:

- БД Proxy нужна, чтобы Proxy не потерял очередь и результаты после рестарта;
- хранилище клиента нужно, чтобы клиент продолжил обмен после своего рестарта.

## Исходящий HTTP

Короткая схема:

```text
Service A → сохранить запрос → NATS → Proxy → сохранить запрос
Service A ← ACK ← Proxy
Service A → сохранить ACK → подтвердить ACK → Proxy
Proxy → записать «начинаю HTTP» → внешний HTTP
Proxy ← HTTP-ответ
Proxy → сначала отправить ответ клиенту → затем сохранить ответ
Service A → сохранить ответ → ACK → Proxy → подтвердить ACK
```

Полная последовательность из 15 шагов и поведение при каждом сбое описаны в
[docs/logic.md](docs/logic.md#обычный-http-запрос).

## Ошибка с неизвестным результатом

HTTP и PostgreSQL нельзя объединить в одну транзакцию. Если Proxy записал «начинаю
HTTP» и после этого упал, неизвестно, получил ли внешний сервер запрос.

В этом случае Proxy:

1. не повторяет HTTP автоматически;
2. отправляет клиенту ошибку «результат неизвестен»;
3. повторяет доставку ошибки до ACK;
4. после завершения обмена удаляет запись по обычному retention.

Если клиент ранее уже сохранил успешный ответ, последующая ошибка не заменяет успех.
Клиент подтверждает сообщение, но возвращает приложению сохранённый результат.

## API клиентской библиотеки

### Исходящие запросы

Целевой клиент реализует стандартный `http.RoundTripper`, потому что `http.Client` в
Go является структурой, а не интерфейсом.

```go
proxyTransport, err := proxyclient.OpenTransport(ctx, nc, clientDatabaseDSN,
    proxyclient.Config{
        ClientID: "service-a",
        ProxyID:  "proxy-main",
        Signer:   privateKey,
    },
)
if err != nil {
    return err
}
defer proxyTransport.Close()

httpClient := &http.Client{Transport: proxyTransport}
response, err := httpClient.Do(request)
```

Если приложение уже использует `pgxpool`, хранилище можно создать отдельно:

```go
store, err := proxyclient.NewPostgresStore(ctx, pool,
    proxyclient.WithTablePrefix("natsproxyclient_"),
)
```

Пользовательская реализация `Store` остаётся опциональной extension point.

Это позволит существующему Go-коду работать через Proxy без отдельного нестандартного
метода для каждого HTTP-запроса.

### Callback

Для callback клиентская библиотека принимает стандартный `http.Handler`:

```go
handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
})

err := proxyTransport.ServeCallbacks(handler)
if err != nil {
    return err
}

callback, err := proxyTransport.RegisterCallback(ctx, proxyclient.WebhookRegister{
    Name: "provider-b",
    Mode: "delegated",
    ResponderID: "service-a",
})
```

Библиотека получает callback из NATS, создаёт обычный `http.Request`, вызывает handler
и отправляет его status, headers и body обратно в Proxy.

Текущая реализация надёжно сохраняет callback-событие на клиентской стороне до вызова
handler. При этом результат выполнения handler пока не сохраняется отдельно до
отправки в NATS. Если клиент упадёт после выполнения handler, но до завершения ACK,
одно событие может быть передано в handler повторно. Поэтому прикладной handler должен
быть идемпотентным или самостоятельно дедуплицировать события по ID провайдера.

Команды регистрации, изменения и удаления callback идемпотентны на стороне Proxy.
Клиент повторяет команду с тем же `command_id`, пока работает вызвавший её context.
Сама незавершённая команда пока не хранится в клиентской PostgreSQL и после рестарта
клиента автоматически не возобновляется.

## Несколько клиентов и Proxy в одном NATS

Subjects содержат `proxy_id` и `client_id`:

```text
proxy.<proxy_id>.requests
client.<client_id>.proxy.<proxy_id>.results
```

Клиент отправляет каждый запрос одному явно выбранному Proxy. Proxy читает только свои
subjects, проверяет Ed25519-подпись и разрешение клиента. NATS ACL дополнительно
запрещает читать и публиковать чужие subjects.

Несколько клиентов могут работать с одним Proxy. Один клиент может использовать
несколько Proxy, но один конкретный запрос всегда принадлежит одному `proxy_id`.

Несколько экземпляров одного `proxy_id` используют одну NATS queue group и общую БД.
Один экземпляр принимает конкретное сообщение, а DB lease защищает его от параллельной
обработки другим экземпляром.

## Callback

Внутренний сервис регистрирует callback через NATS и получает публичный URL с секретным
токеном.

Поддерживаются два сценария:

- static: Proxy сохраняет callback и возвращает заранее настроенный HTTP-ответ;
- delegated: Proxy передаёт callback назначенному клиенту и возвращает внешний ответ,
  сформированный его `http.Handler`.

Один callback могут слушать несколько клиентов. Для каждого создаётся независимая
доставка. Только один клиент формирует синхронный HTTP-ответ внешнему сервису.

## Разделение кода

Код разделён по направлениям:

```text
client/
  outgoing.go       http.RoundTripper и исходящий ACK-протокол
  callback.go       доставка callback в http.Handler
  postgres_store.go готовое durable-хранилище

internal/
  app/            внешний callback HTTP ingress
  httpx/          выполнение исходящего HTTP
  transport/
    core.go        NATS connection, envelope и маршрутизация
    outgoing.go    исходящие HTTP-команды и ACK
    callback.go    callback control, events и responses
    delivery.go    повтор durable NATS deliveries
  worker/         durable очередь исходящих запросов
  repository/
    postgres.go    подключение, типы и migrations
    outgoing.go    состояния исходящего HTTP
    webhook.go     routes, callback events и control
    delivery.go    durable NATS deliveries
    limits.go      общие host limits
    maintenance.go recovery, cleanup и metrics
```

Так исходящий Proxy и callback Proxy можно читать и изменять независимо. Общий код
остаётся только там, где он действительно общий.

## HTTP-данные

Proxy сохраняет method, исходную строку URL с raw query, значения headers и точные
bytes body. URL не собирается заново, body не декодируется и не перекодируется.

Например, если `X-Signature` содержит HMAC от body или URL, внешний сервер получит те
же данные и подпись останется действительной.

Стандартный `net/http` может технически изменить порядок или регистр заголовков,
`Content-Length` и framing. Это допустимо; точные bytes URL/body остаются неизменными.
Побайтовая копия всего TCP-пакета не гарантируется.

## Повторы и нагрузка

По умолчанию выполняется одна HTTP-попытка. Повтор write-запроса разрешается только
клиентом и только при поддержке idempotency внешним сервисом.

Для каждого внешнего host настраиваются RPS и количество одновременно выполняемых
запросов, а также минимальная пауза между ними. Лишние запросы остаются в очереди
конкретного Proxy. HTTP-соединения по возможности используются повторно.

Значения по умолчанию задаются через `PROXY_DEFAULT_HOST_RPS`,
`PROXY_DEFAULT_HOST_CONCURRENCY` и `PROXY_DEFAULT_HOST_MIN_INTERVAL`. Исключения имеют
формат `PROXY_HOST_LIMITS=api.example.com=10:2:100ms;other.example=50:8:0s`.

## Структура проекта

```text
cmd/proxy/                 запуск сервера и migrations
client/                    RoundTripper, callback adapter и PostgreSQL Store
internal/app/              HTTP endpoints и callback ingress
internal/httpx/            выполнение внешнего HTTP
internal/transport/        отдельные outgoing/callback/delivery NATS handlers
internal/repository/       PostgreSQL по отдельным функциональным областям
internal/worker/           очередь исходящих запросов
```

Завершённые записи Proxy и клиента очищаются автоматически по retention. Для
клиентских таблиц период задаётся через `client.Config.Retention` и по умолчанию
составляет 30 дней. Стабильные operation/command ID нельзя повторно использовать после
окончания retention.

## Локальный запуск

```bash
docker compose up -d --build
curl -i http://localhost:8080/health/ready
```

Проверки:

```bash
make fmt
make vet
make test
make build
```

## Документация

- [Логика системы](docs/logic.md)
- [Открытые вопросы](wiki/open-questions.md)
- [Production checklist](docs/production-checklist.md)
- [Operations runbook](docs/runbook.md)
- [SLO](docs/slo.md)
- [Security](SECURITY.md)
