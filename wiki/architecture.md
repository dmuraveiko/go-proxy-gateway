# Целевая архитектура HTTP–NATS Proxy

Это реализованная базовая архитектура после замечаний от 16 июля 2026 года.

## Исходящий HTTP-запрос

```text
Service A
  │ signed command + request_id
  ▼
Core NATS
  │
  ▼
Proxy: verify Ed25519 ── PostgreSQL inbox ──► application acceptance ACK
                              │
                              ▼
                       leased worker
                              │ net/http
                              ▼
                       arbitrary Internet URL
                              │
                              ▼
              Core NATS result first ──► Service A stores result
                              │                       │
                              ▼                       ▼
                    PostgreSQL result          application result ACK
                              │                       │
                              └──── repeated delivery┘
```

Core NATS используется только как быстрый транспорт и уведомительный канал. Пока
Proxy не записал команду в PostgreSQL, отправитель не получил acceptance ACK и обязан
повторить публикацию с тем же `request_id`. Повторная команда с тем же ID не создает
новую операцию.

Acceptance ACK означает только «команда надежно принята», но не «HTTP-запрос
выполнен». Producer не держит NATS request открытым до ответа внешнего ресурса: итог
приходит отдельным асинхронным сообщением.

После acceptance handshake worker резервирует операцию, а непосредственно перед HTTP
надежно записывает состояние `dispatching`. После HTTP-ответа Proxy сначала пытается
опубликовать результат клиенту и только затем сохраняет результат и durable delivery в
своей БД. Такой порядок дает ответу шанс сохраниться у клиента даже при сбое БД Proxy.
Сохраненный результат повторно публикуется через Core NATS до прикладного ACK. Клиент
обязан сначала записать результат и дедуплицировать его по стабильному ID.

## HTTP-контракт

Команда содержит как минимум:

```text
request_id, method, url, headers[], body, timeout, retry_policy, reply_subject
```

`headers` представляются списком пар, а не JSON object, чтобы на границе контракта не
потерять повторяющиеся значения. Бинарный body кодируется без изменения, например
base64 внутри JSON envelope или напрямую в бинарном формате контракта.

В контракте сохраняется исходная строка URL вместе с raw path/query. Proxy не собирает
URL повторно и не меняет bytes body. Поэтому подпись, вычисленная клиентом от URL или
body и переданная в header, остаётся действительной.

Executor собирает `http.Request` без прикладных преобразований. Обязательные настройки:

- `CheckRedirect` возвращает исходный 3xx и не следует за ним автоматически;
- `DisableCompression: true` не допускает прозрачной декомпрессии;
- Proxy из environment не выбирается неявно;
- `Host` обрабатывается через специальное поле `Request.Host`;
- body не разбирается, не форматируется и не перекодируется;
- response status, header values и body возвращаются вызывающему сервису;
- таймауты, ограничения размера и отмена запроса задаются явно.

Стандартный `net/http` может изменить wire-framing, порядок или регистр заголовков.
Это не считается модификацией прикладных данных в рамках принятого контракта.

## Надежность без JetStream

Семантика доставки строится на PostgreSQL и прикладных ACK:

1. Producer повторяет неподтвержденную команду с тем же `request_id`.
2. Inbox с unique key дедуплицирует прием.
3. После ACK клиента worker берет операцию DB lease и до HTTP фиксирует `dispatching`.
4. После HTTP Proxy сначала дает клиенту шанс сохранить result через Core NATS.
5. Затем result и durable delivery сохраняются в БД Proxy.
6. Сохраненная delivery повторно публикуется до ACK получателя.
7. ACK обрабатывается идемпотентно и фиксируется в PostgreSQL.

Гарантия распространяется только на сообщения, для которых producer получил durable
acceptance ACK. Если ACK потерян после DB commit, повтор безопасен благодаря
`request_id`.

Истекшая lease в `reserved` безопасно возвращает операцию в очередь: HTTP еще не
начинался. Падение после `dispatching` создает `unknown outcome`: Proxy не может
достоверно узнать, успел ли внешний сервер выполнить операцию. Поэтому exactly-once
невозможно. По умолчанию `max_attempts = 1`; caller явно включает retry и отвечает за
идемпотентность внешнего вызова.

`unknown` автоматически доставляется клиенту как ошибка и после ACK очищается по
обычному retention. Ручного workflow нет. Если клиент ранее сохранил успешный result,
он не заменяет его более поздним `unknown`, но подтверждает доставку ошибки.

## Клиентские HTTP-интерфейсы

Исходящий клиент реализует `http.RoundTripper` и может использоваться как
`http.Client.Transport`. Callback-клиент принимает обычный `http.Handler`: собирает
`http.Request` из NATS-события, вызывает handler и возвращает его HTTP-ответ в Proxy.
После handler ответ сначала публикуется в NATS, затем сохраняется в клиентской БД.
Сохранённый ответ повторяется до ACK и после рестарта отправляется без повторного
вызова handler.

Для работы одного сервиса с несколькими Proxy создаётся отдельный Transport на каждый
`proxy_id`. Они могут использовать одни клиентские таблицы: операции имеют ключ
`(proxy_id, request_id)`, callback — `(proxy_id, delivery_id)`. Resume-запросы всегда
фильтруются по Proxy. На серверной стороне один Proxy разделяет клиентов через
`client_id` в БД и NATS subjects.

## Webhook

```text
External system ──HTTP──► Proxy ── PostgreSQL webhook inbox
                                      │ one delivery per subscriber
                         ┌────────────┴────────────┐
                         ▼                         ▼
                    Service B                 Service C
                       │ ACK                      │ ACK
                       └──────── PostgreSQL delivery state
```

Proxy сохраняет method, URL/query, значения headers и body webhook без прикладного
преобразования. Успешный HTTP-ответ внешней системе возвращается только после durable
DB commit. Каждому подписчику соответствует отдельная durable delivery; сбой
одного подписчика не блокирует ACK другого.

Owner создает route подписанной командой `webhook.register.v1` и получает capability
URL `/v1/webhooks/<webhook_id>/<secret-token>`. Целевой endpoint принимает любой HTTP
method. Команды register, update, subscribe, unsubscribe и delete выполняются
идемпотентно. Proxy надёжно хранит их результат и повторяет его до ACK; клиентская
библиотека пока не восстанавливает незавершённый control-вызов после своего рестарта.

В static mode Proxy возвращает заранее настроенные status, headers и body после DB
commit. В delegated mode он ждет ответ назначенного NATS responder и возвращает его
внешнему sender; при timeout отвечает `504`. Capability token защищает route, а
provider-specific подпись по исходным headers/body проверяет внутренний сервис. Если
такая подпись должна проверяться до HTTP-ответа provider, нужен отдельный auth handler.

## Несколько Proxy и экземпляров

Один логический Proxy определяется через `proxy_id` и одну PostgreSQL. Штатный режим —
один процесс. Несколько физических экземпляров являются опциональным HA-режимом и
используют одну NATS queue group и общую БД. `FOR UPDATE SKIP LOCKED`, leases и unique
constraints не дают им выполнить одну операцию одновременно. У каждого процесса есть
`instance_id` для адресных callback responses.

Глобальной блокировки между экземплярами нет. Возможная конкуренция сосредоточена в
пуле PostgreSQL и на общих строках host limiter для запросов к одному target host.

Другой логический Proxy использует другой `proxy_id` и другую БД. Они могут работать в
одном NATS, потому что subjects содержат `proxy_id` и `client_id`; доступ дополнительно
ограничен NATS ACL и Ed25519 ACL.

Один долгоживущий `net/http.Transport` повторно использует соединения. RPS,
concurrency и при необходимости `min_interval` по target host хранятся в общей БД
логического Proxy и соблюдаются всеми его экземплярами.

Исходящее направление и callback должны находиться в отдельных пакетах. Общими для них
остаются только NATS, подписи, ID и ACK-протокол.

## Граница безопасности

Proxy намеренно не ограничивает destination. Защита сосредоточена на идентификации
внутреннего клиента, Ed25519-подписи, NATS ACL, лимитах нагрузки, секретах вне
репозитория, TLS и аудите. Авторизованный сервис фактически получает egress-доступ от
имени Proxy, включая потенциально внутренние адреса, доступные его сети.
