# Service levels

Начальные цели, которые должны быть подтверждены load/chaos tests:

- доступность durable-приема HTTP commands/static webhook: 99.95% в месяц;
- p99 acceptance ACK при здоровых NATS/PostgreSQL: до 1 секунды;
- p99 начала healthy HTTP GET после acceptance handshake и без throttling: до 2 секунд;
- подтвержденный request/result/webhook не теряется внутри PostgreSQL boundary;
- RPO PostgreSQL: 0 для синхронно подтвержденных транзакций;
- RTO: 30 минут;
- рост `unknown` выше нуля — отдельный operational incident.

Недоступность внешнего HTTP provider, delegated webhook responder и deliberate host
throttling считаются отдельно от доступности Proxy.

Сценарии, относительно которых проверяются эти цели, описаны в
[логике системы для согласования](logic.md).
