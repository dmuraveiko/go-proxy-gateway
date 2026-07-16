# Operations runbook

## Рост очереди

Проверить `/metrics`, доступность PostgreSQL/NATS, host limits и внешний HTTP. Рост
`ready` может быть нормальным backpressure. Не увеличивать workers выше DB capacity и
лимитов внешних host. Запросы не переупорядочивать вручную: Proxy порядка не обещает.

## Недоступен PostgreSQL

Proxy перестает подтверждать новые команды и не начинает HTTP без durable-флага
`dispatching`. Workers, уже получившие HTTP-ответ, не повторяют HTTP, а пытаются
сохранить тот же результат. Проверить disk space, WAL, pool, locks и HA failover.

## Unknown outcome

`unknown` означает: HTTP мог уйти наружу, но durable-результат потерян. Небезопасный
request автоматически не повторяется. Ошибка доставляется клиенту до ACK и затем
очищается по retention. Если клиент ранее сохранил успех, ошибка его не заменяет.

## Pending deliveries

Проверить NATS connection, client subscription/ACL и клиентскую БД. Core NATS deliveries
повторяются до прикладного ACK. Дубли ожидаемы и должны дедуплицироваться по delivery ID.

## Host throttling

При `429` или жалобах provider уменьшить `RPS`/`concurrency` override и выполнить rolling
restart. Limiter и connection pool принадлежат конкретному Proxy. Если несколько Proxy
выходят через один NAT IP, проверять их суммарные соединения отдельно.

RPS считается в фиксированных секундных окнах: на границе окон возможен короткий
burst. Если provider требует строгую паузу между запросами, текущей настройки RPS
недостаточно — до запуска нужен отдельный `min_interval` limiter.

## Webhook timeout

Static mode зависит только от PostgreSQL commit. В delegated mode проверить responder,
его NATS ACL и timeout. После `504` provider может повторить callback; responder обязан
дедуплицировать бизнес-событие.

## Key rotation

Сначала добавить новый client public key/Proxy public key, затем перевести producer и
только после отсутствия старого traffic удалить старый. Existing webhook URL продолжает
работать; повтор старой register-команды после rotation signing key требует проверки.

## Disaster recovery

Восстановить PostgreSQL до согласованной PITR-точки, затем Core NATS и Proxy. После
старта истекшие `reserved` вернутся в `ready`, а истекшие `dispatching` станут `unknown`.
Proxy автоматически доставит pending results и ошибки `unknown` клиентам. Убедиться,
что конкретный `proxy_id` подключён только к своей восстановленной БД.

## Миграции

Текущая история схлопнута в одну `000001_initial` и предназначена для чистой БД. Если
в базе уже отмечена выполненной старая миграция с тем же номером, остановить rollout:
автоматически новая схема не применится. Для production подготовить отдельный
переходный migration. Локальную тестовую базу без ценных данных можно пересоздать через
`docker compose down -v`.
