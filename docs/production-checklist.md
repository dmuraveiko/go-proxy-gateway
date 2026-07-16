# Production checklist

- Core NATS: cluster, TLS, accounts/NKeys и минимальные publish/subscribe subject ACL.
- PostgreSQL: HA, TLS, PITR, tested restore, disk/WAL alerts и connection proxy.
- `000001_initial` применять на чистой БД. Для среды со старой prototype-версией
  `000001` подготовить и проверить отдельный transition migration/export: одной замены
  файла недостаточно.
- Ed25519 private keys выдаются через Vault/KMS/CSI, public keys и client ACL проверены.
- Подтвердить бизнесом unrestricted destination, включая доступные внутренние адреса.
- Настроить default и per-host RPS/concurrency на основе реальных provider limits.
- Заменить fixed-window limiter прототипа на настраиваемые RPS/concurrency/min_interval
  и проверить их нагрузочным тестом.
- Настроить retention для завершённых операций и доставленных ошибок `unknown`.
- Проверить static/delegated webhook timeout, capability URL и provider retry behavior.
- Разрешить callback methods, которые передаёт provider, а не только текущий `POST`.
- Добавить idempotent register/update/subscribe/unsubscribe/delete и полный durable
  control ACK.
- Решить, нужен ли отдельный status/cancel management API для HTTP-операций.
- Load test на 2× peak; отдельно NATS loss, DB/disk-full, HTTP timeout и pod kill во время HTTP.
- Проверить, что storage adapter клиента действительно durable и идемпотентен.
- Проверить уникальность `proxy_id` и отдельную PostgreSQL каждого Proxy.
- Проверить совместимость исходящего клиента с `http.RoundTripper`, а callback — с
  `http.Handler`.
- Pin digest, SBOM, vulnerability scan, image signature, dashboards и alerts.
