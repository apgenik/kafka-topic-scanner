kafka-scan
==========

Описание
--------

`kafka-scan` — небольшая утилита на Go для сканирования YAML/JSON конфигураций в поиске имён Kafka-топиков.
Она рекурсивно обходит структуру файла, находит ключи и значения, которые выглядят как определения топиков
(ключи с `producer` или `consumer`) и сохраняет найденные топики в JSON-файл.

Использование
------------

Запуск из каталога с исходником:

```bash
go run kafka-scan.go <input.yaml> [output.json]
```

- `<input.yaml>` — входной YAML или JSON-файл для сканирования.
- `[output.json]` — необязательный путь для вывода (по умолчанию `kafka_topics.json`).

Пример
------

```bash
go run kafka-scan.go application.yaml topics.json
```

Результат
---------

Утилита генерирует JSON-массив объектов с полями:

- `cluster` — имя кластера (выводится из ближайшего ключа-родителя, если доступно).
- `bootstrap` — значение `bootstrap` или `bootstrap-servers`, найденное рядом с настройкой.
- `type` — `producer` или `consumer` в зависимости от контекста.
- `topic` — имя топика.
- `path` — путь в структуре YAML (точечная нотация с индексами массива), где найдено значение.

Особенности и ограничения
-------------------------

- Сканер использует простые эвристики: ключи, содержащие слова `producer` или `consumer` считаются местом, где могут
  быть объявлены топики.
- Значения `bootstrap` записываются только если они представлены строкой рядом с местом поиска.
- Парсер основан на `gopkg.in/yaml.v3`, поэтому входной JSON также корректно обрабатывается.
- Инструмент не выполняет валидацию формата топиков и не подключается к Kafka — он только извлекает строки из конфигурации.

Контакты
--------

Код находится в файле `kafka-scan.go` в этом репозитории.

Пример входного YAML
--------------------

```yaml
app:
  kafka-cluster:
    bootstrap: "kafka01:9092"
    producers:
      topicA: "orders.created"
      topicB: "orders.updated"
    consumers:
      - "orders.created"
      - "orders.failed"
```

Ожидаемый фрагмент JSON-вывода
-----------------------------

```json
[
  {
    "cluster": "kafka-cluster",
    "bootstrap": "kafka01:9092",
    "type": "producer",
    "topic": "orders.created",
    "path": "root.app.kafka-cluster.producers.topicA"
  },
  {
    "cluster": "kafka-cluster",
    "bootstrap": "kafka01:9092",
    "type": "consumer",
    "topic": "orders.created",
    "path": "root.app.kafka-cluster.consumers[0]"
  }
]
```
