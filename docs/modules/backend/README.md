# Backend

## Назначение

Регистрировать ожидания завершения процессов, сохранять события и координировать доставку в Codex.

## Границы ответственности

Backend владеет связью между ожиданием, наблюдаемым процессом, исходным чатом и разрешённым продолжением. Состояние приёма Codex хранить отдельно от факта завершения процесса. Не выполнять рассуждения модели и не запускать наблюдаемый процесс.

- [Регистрация ожидания](operations/register-watch.md).
- [Отмена ожидания](operations/cancel-watch.md).
- [Доставка события](jobs/deliver-event.md).
- [Восстановление состояния](jobs/recover-state.md).
- [Очередь effectum](queue/README.md).
- [Хранение SQLite](database/README.md).
- [Настройки](settings.md).
- [Кэши](caches.md).

## Зависимости

Использовать effectum с файловой SQLite, [process-watch](../process-watch/README.md) и [codex-delivery](../codex-delivery/README.md). Не записывать напрямую во внутреннюю базу Desktop.
