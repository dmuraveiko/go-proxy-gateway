# Production checklist

- Core NATS: cluster, TLS, accounts/NKeys и минимальные publish/subscribe subject ACL.
- PostgreSQL: HA, TLS, PITR, tested restore, disk/WAL alerts и connection proxy.
- `000001_initial` применять на чистой БД. Для среды со старой prototype-версией
  `000001` подготовить и проверить отдельный transition migration/export: одной замены
  файла недостаточно.
- Ed25519 private keys выдаются через Vault/KMS/CSI, public keys и client ACL проверены.
- Подтвердить бизнесом unrestricted destination, включая доступные внутренние адреса.
- Настроить default и per-host RPS/concurrency на основе реальных provider limits.
- Для каждого provider решить, допустим ли fixed-window burst; при требовании строгой
  паузы реализовать и нагрузочно проверить `min_interval` до подключения provider.
- Настроить retention; `unknown` не удалять автоматически без согласованного TTL.
- Проверить static/delegated webhook timeout, capability URL и provider retry behavior.
- Подтвердить, что webhook provider использует `POST`; согласовать нужный объем control
  API: текущий scope — register, subscribe и delete.
- Добавить durable control ACK для webhook register/subscribe/delete либо явно принять
  текущую модель: повтор register с тем же ID, без success ACK для subscribe/delete.
- Решить, нужен ли отдельный status/cancel management API для HTTP-операций.
- Load test на 2× peak; отдельно NATS loss, DB/disk-full, HTTP timeout и pod kill во время HTTP.
- Проверить, что client.Store действительно durable и идемпотентен.
- Pin digest, SBOM, vulnerability scan, image signature, dashboards и alerts.
