# Direct App Server queue probe

Диагностический клиент на Rust для выбранного `thread/queue/add`. Подключается по WebSocket через **явно указанный Unix-сокет уже работающего App Server**. Не запускает Codex, не меняет Desktop, не использует внутренний IPC Desktop и не пишет в SQLite Codex.

Это проверка канала доставки. Production backend, effectum и наблюдение за процессами в прототип не входят. Его параметры и классификация результатов не закрепляют незавершённые production-контракты S1–S5 из [индекса](../../docs/modules/index.md).

## Проверки кода

Корневой `rust-toolchain.toml` закрепляет Rust 1.99.1 и компоненты Clippy и rustfmt. CI запускает проверки на macOS для всех целей и features прототипа. Те же команды можно выполнить локально из корня репозитория:

```sh
cargo fmt --manifest-path prototypes/app-server-queue/Cargo.toml --all -- --check
cargo clippy --manifest-path prototypes/app-server-queue/Cargo.toml --locked --all-targets --all-features -- -D warnings
cargo test --manifest-path prototypes/app-server-queue/Cargo.toml --locked --all-targets --all-features
```

Clippy проверяет стандартные, `pedantic` и `nursery` правила, а также отдельные ограничения на потенциальные паники и сложность. Это эвристики: они не доказывают соблюдение SOLID и могут требовать обоснованных локальных исключений.

## Мок-проверка

Из корня репозитория, с установленным Rust toolchain:

```sh
cargo test --manifest-path prototypes/app-server-queue/Cargo.toml
```

Тесты поднимают временный WebSocket-сервер на Unix-сокете, запускают настоящий клиент и проверяют handshake, передачу ID чата и Unicode-сообщения, пагинацию загруженных чатов, проверку receipt, отказ до отправки и неоднозначный результат после обрыва/таймаута. Сервер мока не вызывает модель и не имитирует доказательство настоящего пробуждения.

## Проверка настоящего endpoint без отправки

Указать фактический сокет App Server и ID уже загруженного чата:

```sh
cargo run --manifest-path prototypes/app-server-queue/Cargo.toml -- \
  --socket /absolute/path/to/app-server.sock \
  --thread '<thread-id>' \
  --timeout-ms 5000 \
  --check-only
```

Клиент выполняет `initialize` с `experimentalApi: true`, отправляет `initialized`, затем читает страницы `thread/loaded/list`. При отсутствии целевого ID возвращает `not_sent`. Он не вызывает `thread/resume`, `thread/start` или `turn/start`. Проверка загруженного ID не доказывает, что именно Desktop подключён к этому endpoint: это нужно проверить отдельно при настройке общего сервера.

Текущий stdio-процесс Desktop не становится доступен через сокет от запуска этого клиента. Путь `~/.codex/ipc/ipc.sock` относится к другому протоколу и не подходит этому клиенту. Отсутствующий endpoint не заменяется вторым App Server.

## Однократная отправка

Подготовить UTF-8 файл сообщения вне Git, затем выполнить:

```sh
cargo run --manifest-path prototypes/app-server-queue/Cargo.toml -- \
  --socket /absolute/path/to/app-server.sock \
  --thread '<thread-id>' \
  --timeout-ms 5000 \
  --client-message-id '<explicit-event-id>' \
  --message-file /absolute/path/to/message.txt
```

Этот режим действительно ставит сообщение в очередь. CLI требует все параметры явно; `5000` здесь — параметр запуска, а не production-default. Таймаут применяется отдельно к установлению соединения и каждой RPC-операции.

После проверки загруженного чата клиент вызывает `thread/queue/add` ровно один раз. `input` содержит один текстовый элемент, текст берётся из файла без изменения. `clientUserMessageId` берётся из аргумента; клиент не предполагает дедупликацию по этому полю.

## Результаты

stdout содержит один JSON-объект без текста сообщения. Коды завершения:

| Код | `status` | Что установлено |
|---|---|---|
| 0 | `checked` | Нужный ID найден на подключённом сервере; отправка не выполнялась |
| 0 | `accepted` | Получен совпадающий receipt очереди; запуск и завершение агента не подтверждены |
| 2 | `not_sent` | Вызов добавления не начинался: неверные параметры, недоступный endpoint, ошибка preflight или отсутствующий загруженный ID |
| 3 | `rejected` | `thread/queue/add` вернул ошибку протокола `-32600`, `-32601` или `-32602` |
| 4 | `delivery_unknown` | Добавление могло состояться: обрыв, таймаут, некорректный receipt или другая ошибка после начала отправки |

Включая случаи `delivery_unknown`, автоматических повторов нет. Запуск CLI второй раз — новая внешняя попытка, даже при прежнем `clientUserMessageId`. Прототип не сохраняет журнал и не обеспечивает восстановление после падения; для реального теста receipt и факт нового хода нужно сопоставить отдельно.

`accepted` требует непустой `queuedSubmission.id`, совпадения `clientUserMessageId` и единственного текстового элемента receipt с отправленным текстом. Уведомления между запросом и ответом пропускаются, ответ сопоставляется по RPC `id`.

## Граница проверки

Успех мока подтверждает поведение клиента на заданных ответах протокола. Он не подтверждает поддержку транспорта установленным Desktop, idle/busy/interrupted/cold поведение, запуск модели, надёжность effectum или exactly-once доставку. Ранее проверенный путь через второй App Server и общее хранилище не является проверкой выбранной прямой схемы.

Протокол сверяется с [официальным App Server API](https://learn.chatgpt.com/docs/app-server) и схемой, генерируемой установленным `codex app-server generate-json-schema --experimental`. `thread/queue/add` в исследованной версии требует experimental capability; совместимость будущих версий не гарантируется этим стендом.
