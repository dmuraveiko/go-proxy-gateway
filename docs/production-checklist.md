# Production checklist

- Core NATS: cluster, TLS, accounts/NKeys и минимальные publish/subscribe subject ACL.
- PostgreSQL: HA, TLS, PITR, tested restore, disk/WAL alerts и connection proxy.
- `000001_initial` применять на чистой БД. Для среды со старой prototype-версией
  `000001` подготовить и проверить отдельный transition migration/export: одной замены
  файла недостаточно.
- Ed25519 private keys выдаются через Vault/KMS/CSI, public keys и client ACL проверены.
- Подтвердить бизнесом unrestricted destination, включая доступные внутренние адреса.
- Настроить default и per-host RPS/concurrency на основе реальных provider limits.
- Проверить настроенные RPS/concurrency/min_interval нагрузочным тестом.
- Настроить retention для завершённых операций и доставленных ошибок `unknown`.
- Проверить static/delegated webhook timeout, capability URL и provider retry behavior.
- Проверить callback methods, которые передаёт каждый provider.
- Решить, нужен ли отдельный status/cancel management API для HTTP-операций.
- Load test на 2× peak; отдельно NATS loss, DB/disk-full, HTTP timeout и pod kill во время HTTP.
- Проверить migrations, durability и идемпотентность встроенного клиентского
  PostgreSQL Store с префиксом `natsproxyclient_`.
- Штатно запускать один процесс на `proxy_id`. Если включается HA, проверить общую
  queue group/БД, разные `instance_id` и влияние конкуренции на PostgreSQL и host
  limiter. Разные `proxy_id` не должны делить БД.
- Проверить совместимость исходящего клиента с `http.RoundTripper`, а callback — с
  `http.Handler`.
- Расширить fault-injection тесты HMAC/URL/body на обрывы NATS и PostgreSQL.
- Pin digest, SBOM, vulnerability scan, image signature, dashboards и alerts.
