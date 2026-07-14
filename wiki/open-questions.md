# Открытые вопросы перед production

Функциональный протокол реализован. До production deployment владельцы системы должны
согласовать значения и эксплуатационные политики:

1. Конкретные client IDs, логические proxy IDs и NATS account/subject ACL.
2. Процесс выдачи, rotation и emergency revocation Ed25519 keys.
3. Production limits request/response/webhook body и timeout.
4. Default и per-host RPS/concurrency для реальных внешних серверов.
5. Retention завершенных операций и ручной workflow для `unknown`.
6. PostgreSQL HA/PITR/RPO, NATS cluster topology и SLO после load tests.
7. Нужен ли отдельный query/cancel management API поверх существующего synchronous
   Go client и repeated result delivery.
8. Для каких webhook требуется provider-specific auth до DB commit, а для каких
   достаточно capability URL и последующей проверки сервисом-получателем.
9. Нужна ли глобальная квота HTTP connections с одного NAT IP поверх per-host limiter
   логического Proxy.
10. Нужен ли отдельным provider-ам строгий `min_interval`, которого не гарантирует
    текущий fixed-window limiter.
11. Нужны ли webhook methods кроме `POST`, а также list/get/update/unsubscribe и
    rotation capability token поверх текущих register/subscribe/delete.
12. Требуется ли полный durable ACK-handshake для webhook control commands. Сейчас
    register можно безопасно повторить, но его result best-effort, а subscribe/delete
    не возвращают success ACK.
13. Для любой среды со старой prototype-версией `000001` нужен отдельный переходный
    migration/export. Новая единая `000001_initial` применяется только к чистой БД.
