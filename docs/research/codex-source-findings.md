# Доставка событий и пробуждение в исходниках Codex

Исследовано 2026-10-26. Репозиторий `openai/codex`, commit `970111f6cb62cc02fe36a82ea7d71b37a2889c97` (2026-10-23).

## Вывод

В открытом Rust-harness уже реализованы входы, способные возобновить простаивающий загруженный чат. Собственный inference runtime или собственный планировщик ходов для этого писать не нужно. Получение результата от конкретного процесса/CI и его доставка в нужный экземпляр Codex остаются отдельными задачами интеграции.

Исследование статическое: прочитаны реализация и upstream-тесты. Эти Rust-тесты локально не запускались. В действующие чаты события не отправлялись, серверы не запускались и настройки Desktop не менялись.

Дополнительно штатной командой `app-server generate-json-schema --experimental` выгружена схема bundled CLI установленного Desktop (`0.163.0-alpha.11.1`). В ней присутствуют `TurnStartParams.toolOutput`, `ThreadQueueAddParams` и `McpServerEventStreamStartParams`. Это подтверждает наличие контрактов в установленном бинарнике, но не проверяет доступность транспорта и сквозное пробуждение приложения.

## 1. Прямой вход: turn/start.toolOutput

В `TurnStartParams` есть `toolOutput` с полями `name`, `namespace`, `output` и `retain`.

Цепочка реализации:

1. `turn_start_inner` находит уже загруженный чат через `thread_manager.get_thread`. Самостоятельно холодный чат этот метод не возобновляет.
2. Проверяет разрешённость прямого ввода и параметры. Нельзя одновременно передать `toolOutput` и непустой `input`.
3. Преобразует результат в `ResponseItem::FunctionCallOutput` с отсутствующим `call_id` — самостоятельный результат инструмента, а не ответ на обязательный ранее открытый вызов.
4. Вызывает `thread.start_or_steer_turn`.
5. Core при активном ходе пытается добавить вход в него. При `NoActiveTurn` создаёт `RegularTask` через `spawn_task`.

Следовательно, внешний результат может запустить новый ход без предварительного вопроса модели «завершилось ли задание». При занятом чате это **steer**, а не обязательно отложенный следующий ход. Есть проверки допуска, состояния и разрешений: ответ сервера нужно проверять.

Пример формы запроса, только для иллюстрации, не выполнен:

```json
{
  "id": 2,
  "method": "turn/start",
  "params": {
    "threadId": "<existing-loaded-thread-id>",
    "input": [],
    "toolOutput": {
      "namespace": "agent_wake",
      "name": "event_completed",
      "output": "{\"job_id\":\"example\",\"exit_code\":1}"
    }
  }
}
```

До запроса необходимо подключиться к нужному App Server и выполнить handshake. Завершение процесса остаётся результатом инструмента, а не новой авторизацией пользователя.

Источники:

- [Контракт TurnToolOutput / TurnStartParams](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server-protocol/src/protocol/v2/turn.rs#L153-L199).
- [Обработчик RPC и преобразование входа](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/src/request_processors/turn_processor.rs#L528-L685).
- [Уже загруженный чат](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/src/request_processors/turn_processor.rs#L377-L392).
- [Core: активный ход либо запуск RegularTask](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/core/src/session/turn_input.rs#L329-L450).
- [Интеграционный тест: standalone tool output запускает ход](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/tests/suite/v2/thread_inject_items.rs#L295-L350).

Это уточняет прежнюю осторожную формулировку о «специальном интеграционном пути»: в исследованном открытом Codex этот путь действительно реализован, а не только предположен автором agent-wake.

## 2. Штатная durable-очередь: codex queue / thread/queue/add

CLI вызывает `thread/queue/add`. Сервер передаёт вход в `QueuedItemService.enqueue`, сохраняет его, уведомляет клиентов и сразу вызывает `wake_if_loaded`.

Для загруженного простаивающего чата:

`enqueue → wake_if_loaded → idle lifecycle → dispatch_if_idle → start_turn_if_idle`.

Для занятого чата очередь остаётся до подходящей idle-границы. Явное прерывание пользователем приостанавливает автоматическую обработку; `on_thread_idle` не обрабатывает причину `Interrupted`. В очереди разрешён пользовательский ввод, а не произвольные tool results. Поэтому перенос сырого CI-лога в эту очередь имеет другую семантику доверия, чем `toolOutput`.

Есть отдельный фоновый механизм обнаружения внешних изменений SQLite: `watch_external_messages` проверяет `change_version` каждые 10 секунд и выбирает изменённые очереди загруженных чатов. Это локальный Rust-код, **не запрос к модели**. Прямое enqueue на сервере-владельце не ждёт этого интервала, но вся подсистема очереди не является полностью свободной от polling.

Холодный чат может хранить сообщение в очереди; дальнейший resume приводит к его обработке. Простое наличие записи не гарантирует немедленную загрузку любого чата. Реализация CLI специально старается выбирать общий сервер и содержит защиту от некоторых случаев записи через отдельный сервер. Поэтому нельзя считать любой вызов CLI гарантированным адресованием текущего Desktop.

Источники:

- [CLI queue](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/tui/src/session_queue_commands.rs#L29-L130).
- [Сохранение и немедленный wake](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/ext/queue/src/service.rs#L265-L281).
- [Dispatch и idle lifecycle](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/ext/queue/src/service.rs#L405-L489), [пауза после interrupt](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/ext/queue/src/service.rs#L549-L568).
- [10-секундная проверка внешних изменений](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/ext/queue/src/service.rs#L89-L145).
- [Тест idle_queue_dispatch_preserves_client_id](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/tests/suite/v2/thread_queue.rs#L368-L427).
- [Тест cold_thread_resume_dispatches_a_persisted_queued_submission](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/tests/suite/v2/thread_queue.rs#L429-L558).

API очереди помечен experimental и требует соответствующей capability при initialize, даже если пользовательская команда CLI описана как stable.

## 3. MCP Events: транспорт есть, универсального локального wake-hook здесь нет

В протоколе есть `mcpServer/event/stream/start`, `stop` и `notification`. Внутри открывается `events/stream`, входящие уведомления ожидаются асинхронно через `recv`/`tokio::select!`.

Но исследованный обработчик явно отказывает серверу, отличному от `CODEX_APPS_MCP_SERVER_NAME`: «MCP event subscriptions are only supported for hosted apps». Кроме того, он пересылает уведомления подключённому клиенту. В этом обработчике нет вызова запуска хода агента.

Поэтому нельзя заявить, что произвольный локальный MCP-сервер может послать notification и тем самым автоматически разбудить модель. Приём события, клиентская политика подписки и запуск хода — разные части системы.

- [Ограничение hosted apps](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/src/request_processors/mcp_event_stream.rs#L49-L60).
- [Пересылка уведомлений клиенту и ожидание stream.recv](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/app-server/src/request_processors/mcp_event_stream.rs#L195-L213).

## 4. Завершение встроенного фонового exec

`unified_exec::spawn_exit_watcher` уже ждёт завершения процесса через уведомления и публикует `ExecCommandEnd`. Путь ведёт к ToolEmitter и отправке события клиентам/истории; в исследованной цепочке нет вызова `start_or_steer_turn` или другого запуска RegularTask.

Это доказывает наличие событий о завершении процесса, но не автоматическое продолжение уже завершённого хода. Поведение конкретного Desktop-клиента поверх события здесь не установлено. Ожидание результата внутри ещё активного вызова инструмента — отдельный сценарий.

- [Событийный watcher завершения](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/core/src/unified_exec/async_watcher.rs#L155-L247).
- [Публикация результата](https://github.com/openai/codex/blob/970111f6cb62cc02fe36a82ea7d71b37a2889c97/codex-rs/core/src/unified_exec/async_watcher.rs#L327-L390).

## Что это меняет для нашего проекта

1. Отдельный backend, реализующий контекст агента и планирование его ходов, не обоснован: Core это уже делает.
2. Для событий-результатов прямой `toolOutput` семантически ближе к задаче. Он требует доступного транспорта к серверу с нужным загруженным чатом; при активном ходе результат может попасть в него.
3. Очередь полезна для заранее авторизованного follow-up, который должен дождаться завершения текущего хода. Она durable, но содержит фоновый опрос SQLite и принимает именно пользовательский ввод.
4. Для управляемого нами локального процесса может хватить небольшой обёртки или интеграции с существующим exec watcher. Для CI всё ещё нужен источник push-события и канал до компьютера.
5. На момент исследования необходимость службы и язык ещё обсуждались. Последующее решение зафиксировано в [ADR-0001](../adr/0001-rust-nats-jetstream.md).

Следующий этап исследования — проверить поддерживаемый способ адресовать **тот App Server, который обслуживает Desktop**, и поведение очереди именно с ним. Публичный исходник harness не равен полной реализации Desktop. Доказательство в коде и в сгенерированной схеме не заменяет отдельный разрешённый сквозной тест в чате.
