# QVD File Format — Complete Technical Documentation

## Overview

QVD (QlikView Data) — проприетарный бинарный формат файлов Qlik Sense/QlikView для хранения одной реляционной таблицы. Формат оптимизирован для максимально быстрого чтения/записи движком Qlik (до нескольких миллионов строк в секунду).

## Структура файла

QVD файл состоит из трёх последовательных частей:

```
┌─────────────────────────────┐
│  1. XML Header (метаданные) │  ← текстовый, UTF-8
├─────────────────────────────┤
│  2. Symbol Tables           │  ← бинарный, column-major
│     (уникальные значения    │
│      каждой колонки)        │
├─────────────────────────────┤
│  3. Index Table             │  ← бинарный, row-major, bit-stuffed
│     (строки = индексы       │
│      в symbol tables)       │
└─────────────────────────────┘
```

Все три части "плотно склеены" без разделителей. Между XML заголовком и бинарной частью — символы `\r\n` и нулевой байт `\0`.

---

## 1. XML Header (Метаданные)

### Структура XML

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<QvdTableHeader>
  <QvBuildNo>7314</QvBuildNo>
  <CreatorDoc></CreatorDoc>
  <CreateUtcTime>2019-04-03 06:24:33</CreateUtcTime>
  <SourceCreateUtcTime></SourceCreateUtcTime>
  <SourceFileUtcTime></SourceFileUtcTime>
  <SourceFileSize>-1</SourceFileSize>
  <StaleUtcTime></StaleUtcTime>
  <TableName>tab1</TableName>
  <Fields>
    <QvdFieldHeader>
      <FieldName>ID</FieldName>
      <BitOffset>0</BitOffset>
      <BitWidth>3</BitWidth>
      <Bias>-2</Bias>
      <NumberFormat>
        <Type>0</Type>
        <nDec>0</nDec>
        <UseThou>0</UseThou>
        <Fmt></Fmt>
        <Dec></Dec>
        <Thou></Thou>
      </NumberFormat>
      <NoOfSymbols>4</NoOfSymbols>
      <Offset>0</Offset>
      <Length>40</Length>
      <Comment></Comment>
      <Tags>
        <String>$numeric</String>
        <String>$integer</String>
      </Tags>
    </QvdFieldHeader>
  </Fields>
  <Compression></Compression>
  <RecordByteSize>1</RecordByteSize>
  <NoOfRecords>5</NoOfRecords>
  <Offset>77</Offset>
  <Length>5</Length>
  <Lineage>
    <LineageInfo>
      <Discriminator>...</Discriminator>
      <Statement>...</Statement>
    </LineageInfo>
  </Lineage>
  <Comment></Comment>
</QvdTableHeader>
```

### Основные поля метаданных

| Поле | Описание |
|------|----------|
| `QvBuildNo` | Номер билда Qlik, создавшего файл |
| `CreatorDoc` | Имя QVW/QVF файла-источника |
| `CreateUtcTime` | Время создания файла (UTC) |
| `TableName` | Имя таблицы в Qlik |
| `NoOfRecords` | Количество строк в таблице |
| `RecordByteSize` | Размер одной записи в index table (байты) |
| `Offset` | Смещение index table от начала бинарной части |
| `Length` | Длина index table в байтах |
| `Compression` | Сжатие (обычно пусто) |

### Поля метаданных колонки (QvdFieldHeader)

| Поле | Описание |
|------|----------|
| `FieldName` | Имя колонки |
| `BitOffset` | Битовое смещение в записи index table |
| `BitWidth` | Битовая ширина индекса в записи |
| `Bias` | Смещение индексов (0 или -2 для NULL) |
| `NoOfSymbols` | Количество уникальных значений |
| `Offset` | Смещение symbol table колонки от начала бинарной части |
| `Length` | Длина symbol table колонки в байтах |

### NumberFormat Types

| Значение | Тип |
|----------|-----|
| 0 | UNKNOWN |
| 1 | DATE |
| 2 | TIME |
| 3 | TIMESTAMP |
| 4 | INTERVAL |
| 5 | INTEGER |
| 6 | MONEY |
| 7 | REAL |
| 8 | ASCII |

> Поле `Type` в NumberFormat практически бесполезно для определения реального типа данных. В 90% случаев = 0 (UNKNOWN).

---

## 2. Symbol Table (Таблица символов)

### Общая структура

Для таблицы из N колонок будет N бинарных блоков symbol table, идущих подряд без разделителей. Каждый блок содержит уникальные значения одной колонки.

Смещение каждого блока: `Offset` в QvdFieldHeader (относительно начала бинарной части).
Длина: `Length` в QvdFieldHeader.

### Формат записи в symbol table

Каждое значение:
```
[1 байт: тип] [опциональное бинарное значение] [опциональная строка с нулевым терминатором]
```

### Типы данных (первый байт)

| Код | Тип | Бинарная часть | Строковая часть |
|-----|-----|----------------|-----------------|
| `0x01` | Integer | 4 байта (i32, little-endian) | нет |
| `0x02` | Double | 8 байт (f64, little-endian) | нет |
| `0x04` | String | нет | null-terminated строка (UTF-8) |
| `0x05` | Dual Int | 4 байта (i32, little-endian) | null-terminated строка |
| `0x06` | Dual Double | 8 байт (f64, little-endian) | null-terminated строка |

> **Код 0x03 не используется** (отсутствует в спецификации).

### Важные особенности

1. **Смешанные типы**: одна колонка может содержать значения разных типов (int, double, string).
2. **Невозможность индексации**: строковые значения имеют переменную длину → нельзя перейти к символу N без чтения всех предыдущих.
3. **Dual-типы**: содержат и числовое, и строковое представление. При чтении строковое представление имеет приоритет для отображения.

### Пример бинарного представления

Поле ID со значениями `123.12, 124, -2, 1`:
```
06 [8 байт: 123.12 как f64] "123.12\0"    — dual double
05 [4 байта: 124 как i32]   "124\0"        — dual int
05 [4 байта: -2 как i32]    "-2\0"         — dual int
05 [4 байта: 1 как i32]     "1\0"          — dual int
```
Итого: 40 байт.

---

## 3. Index Table (Таблица строк / Битовый индекс)

### Общая структура

- Расположение: смещение `Offset` от начала бинарной части (из корня XML)
- Размер: `NoOfRecords × RecordByteSize` байт
- Каждая запись имеет фиксированный размер `RecordByteSize` байт

### Формат записи

Каждая строка — это конкатенация битовых полей (индексов в symbol tables). Поля упакованы в минимальное количество бит:

- Порядок полей определяется `BitOffset` (от младших бит к старшим)
- Ширина каждого поля: `BitWidth` бит
- Общая длина выровнена до границы байта

### Порядок байтов (ВАЖНО!)

**Байты в записи хранятся в обратном порядке (little-endian byte order):**
- Первый байт записи → самые правые (младшие) биты
- Последний байт записи → самые левые (старшие) биты

Для чтения нужно:
1. Перевернуть байты записи
2. Читать биты от старших к младшим
3. Поле с `BitOffset=0` окажется в самых правых битах

### Вычисление индекса символа

```
raw_index = биты[BitOffset .. BitOffset + BitWidth]  (как unsigned integer)
actual_index = raw_index + Bias
```

### Обработка NULL

- Если `Bias = -2`: поле может содержать NULL
- NULL, когда `actual_index < 0` (т.е. raw_index = 0 при Bias = -2 → actual_index = -2)
- Также `actual_index = -1` считается NULL
- Все "настоящие" индексы увеличены на |Bias| (обычно на 2)

### Специальные случаи

1. **`BitWidth = 0`**: поле имеет только одно уникальное значение (или только NULL). В index table не занимает места.
2. **`NoOfSymbols = 0`**: поле всегда NULL, symbol table пуста.

### Пример

Таблица 5 строк, 2 колонки, `RecordByteSize = 1`:
```
Поле ID:   BitOffset=0, BitWidth=3, Bias=-2
Поле NAME: BitOffset=3, BitWidth=5, Bias=0
```

Запись `0x02` = бинарно `00000010`:
```
Биты 0-2 (ID):   010 = 2, + Bias(-2) = 0 → символ[0] = "123.12"
Биты 3-7 (NAME): 00000 = 0            = 0 → символ[0] = "Pete"
```

Запись `0x20` = бинарно `00100000`:
```
Биты 0-2 (ID):   000 = 0, + Bias(-2) = -2 → NULL
Биты 3-7 (NAME): 00100 = 4            = 4 → символ[4] = "None"
```

---

## 4. Даты и Timestamps

- **Дата**: целое число = количество дней от начала эпохи Qlik (30 декабря 1899 года)
- **Timestamp**: дробное число, целая часть = дата, дробная = время дня
  - `.0` = 00:00:00
  - `.999999` = 23:59:59
- При dual-типах (5, 6) строковое представление содержит читаемую дату

---

## 5. Функция EXISTS() в Qlik

### Как работает

`EXISTS(field, value)` проверяет, было ли значение `value` уже загружено в поле `field` в процессе выполнения скрипта загрузки.

### Внутренний механизм

1. Qlik строит **Symbol Table** для каждого поля — хранит только уникальные значения
2. Symbol Table использует **hash-based** структуру данных
3. `EXISTS()` выполняет **O(1) lookup** по хешу в Symbol Table
4. Symbol Table обновляется динамически при загрузке каждой новой строки

### Ключевые свойства

- Работает значительно быстрее JOIN для фильтрации строк
- Проверяет только одно поле за раз
- Для составных ключей нужно конкатенировать поля
- Позволяет "оптимизированную загрузку" QVD (самый быстрый режим)

### Реализация в нашей библиотеке

Для реализации аналога `EXISTS()`:
- При чтении QVD строим `HashSet<QvdValue>` для каждой колонки из symbol table
- При загрузке данных из другого QVD с `WHERE EXISTS` — проверяем через HashSet
- O(1) lookup по хешу, как в Qlik

---

## 6. Существующие реализации

### Rust (Python-биндинг)
- **[qvd-utils](https://github.com/SBentley/qvd-utils)** — Rust + PyO3, только чтение. Зависимости: `quick-xml`, `serde`, `bitvec`, `pyo3`

### Python
- **[PyQvd](https://github.com/MuellerConstantin/PyQvd)** — чтение и запись
- **[qvdfile](https://github.com/korolmi/qvdfile)** — исследовательская версия (автор статей на Хабре)

### C
- **[qvdreader](https://github.com/devinsmith/qvdreader)** — утилита чтения, использует libxml2

### JavaScript
- **[qvd-reader](https://github.com/mafuentes22/qvd-reader)** — чтение в массив объектов

### Java
- **[QVDConverter](https://github.com/ralfbecher/QlikView_QVDReader_Examples)** — чтение и конвертация

---

## 7. Режимы загрузки QVD в Qlik

### Optimized Load (до 100x быстрее)

Qlik поддерживает два режима чтения QVD:

- **Optimized** — данные загружаются как есть, без распаковки строк. До 100x быстрее.
- **Standard** — данные распаковываются и обрабатываются построчно.

**Optimized load работает ТОЛЬКО когда:**
- Загружаются все поля (или только переименовываются)
- Нет WHERE-условий (кроме простого `WHERE EXISTS`)
- Нет вычислимых полей (ApplyMap, конкатенации и т.д.)
- Нет трансформаций в том же LOAD-выражении

**Что ломает optimized load:**
- `WHERE` с любым условием кроме `EXISTS`
- `ApplyMap()` в LOAD
- Вычислимые поля (`field1 & '-' & field2`)
- Любые агрегации в том же LOAD

**Workaround в Qlik:** двухшаговая загрузка — сначала optimized LOAD в resident, потом LOAD из resident с трансформациями.

**Workaround с rustqvd:** выполнить трансформации ВНЕ Qlik (JOIN, WHERE, ApplyMap), сохранить результат в QVD, и Qlik сделает optimized load готового файла.

### Производительность на реальных данных

| Режим | 22 млн строк |
|-------|-------------|
| Optimized | ~3 сек |
| Standard | ~9 сек |
| С ApplyMap | ~15 сек |

---

## 8. Архитектура библиотеки `rustqvd`

### Текущие возможности (v0.1.0)

- Чтение QVD файлов (парсинг XML + бинарной части)
- Запись QVD файлов (byte-identical roundtrip)
- Генерация QVD с нуля через `QvdTableBuilder`
- Быстрый `exists()` через HashSet lookup (O(1))
- Оптимизированная `filter_rows_by_exists_fast()` — фильтрация на уровне символов
- Python-биндинги через PyO3/maturin

### Зависимости

- **Нулевые зависимости** для core-библиотеки
- `pyo3` — опционально, только для Python-биндингов (feature `python`)
- XML парсер написан с нуля внутри библиотеки

### Модули

```
src/
├── lib.rs          — публичный API, re-exports
├── error.rs        — типы ошибок (QvdError, QvdResult)
├── header.rs       — парсинг/генерация XML заголовка (свой XML парсер)
├── value.rs        — типы данных QVD (QvdSymbol, QvdValue)
├── symbol.rs       — чтение/запись symbol tables (бинарный формат)
├── index.rs        — чтение/запись index table (bit-stuffing)
├── reader.rs       — высокоуровневый reader (QvdTable, read_qvd_file)
├── writer.rs       — высокоуровневый writer + QvdTableBuilder
├── exists.rs       — ExistsIndex с HashSet + filter_rows_by_exists_fast
└── python.rs       — PyO3 биндинги (QvdTable, ExistsIndex, filter_exists)
```

### Производительность

Протестировано на 20 реальных QVD файлах (от 11 КБ до 2.8 ГБ):

| Файл | Размер | Строки | Колонки | Чтение | Запись |
|------|--------|--------|---------|--------|-------|
| sample_tiny.qvd | 11 KB | 12 | 5 | 0.0s | 0.0s |
| sample_small.qvd | 418 KB | 2,746 | 8 | 0.0s | 0.0s |
| sample_medium.qvd | 41 MB | 465,810 | 12 | 0.5s | 0.0s |
| sample_large.qvd | 587 MB | 5,458,618 | 15 | 6.1s | 0.4s |
| sample_xlarge.qvd | 1.7 GB | 87,617,047 | 6 | 36.8s | 1.6s |
| sample_huge.qvd | 2.8 GB | 11,907,648 | 42 | 24.3s | 2.4s |

**Все 20 файлов — byte-identical roundtrip (MD5 match).**

### Бенчмарк: rustqvd vs PyQvd (Pure Python, v2.3.1)

| Файл | Размер | Строки | Колонки | PyQvd | rustqvd | Ускорение |
|------|--------|--------|---------|-------|---------|-----------|
| sample_tiny.qvd | 11 KB | 12 | 5 | 0.016s | 0.000s | **33x** |
| sample_small.qvd | 418 KB | 2,746 | 8 | 0.047s | 0.002s | **22x** |
| sample_3mb.qvd | 3.3 MB | 81,343 | 7 | 0.449s | 0.014s | **32x** |
| sample_10mb_a.qvd | 10 MB | 248,311 | 9 | 1.9s | 0.1s | **33x** |
| sample_10mb_b.qvd | 10 MB | 1,423,886 | 4 | 4.983s | 0.171s | **29x** |
| sample_medium.qvd | 41 MB | 465,810 | 12 | 8.5s | 0.5s | **16x** |
| sample_500mb.qvd | 480 MB | 11,994,296 | 10 | 79.4s | 2.3s | **35x** |
| sample_large.qvd | 560 MB | 5,458,618 | 15 | 126.6s | 6.3s | **20x** |
| sample_xlarge.qvd | 1.7 GB | 87,617,047 | 6 | >10 мин (не завершился) | 29.6s | **>20x** |
| sample_huge.qvd | 2.8 GB | 11,907,648 | 42 | не тестировалось | 24.3s | — |

**rustqvd в 16-35 раз быстрее PyQvd.** На файлах >500 МБ PyQvd работает минутами, а на 1.7 ГБ (87M строк) не завершился за 10 минут. rustqvd читает тот же файл за 30 секунд.

### Python API

```python
import qvd

# Чтение
table = qvd.read_qvd("file.qvd")
table.columns          # имена колонок
table.num_rows         # количество строк
table.head(5)          # первые 5 строк как list[dict]
table.symbols("col")   # уникальные значения колонки
table.to_dict()        # весь файл как dict {col: [values]}
table.save("out.qvd")  # сохранение (byte-identical roundtrip)

# EXISTS — O(1) lookup
idx = qvd.ExistsIndex(table, "ClientID")
idx.exists("12345")      # True/False
"12345" in idx            # поддержка оператора in

# Фильтрация строк по EXISTS
rows = qvd.filter_exists(other_table, "ClientID", idx)
```

---

## 9. Конкурентный анализ

### Библиотеки для работы с QVD

| Библиотека | Язык | Backend | Чтение | Запись | Скорость | На crates.io |
|---|---|---|---|---|---|---|
| **rustqvd (наш)** | Rust + Python | Rust | да | да (byte-identical) | ~3M rows/sec | нет (будет первым) |
| qvd-utils | Python | Rust (PyO3) | да | нет | ~2M rows/sec | нет |
| PyQvd | Python | Pure Python | да | да | ~100K rows/sec | — |
| qvdfile | Python | Pure Python | да | да | очень медленно | — |
| qvd4js | JavaScript | JS | да | нет | медленно | — |
| qvdreader | C | C | да | нет | быстро | — |

**Ключевой факт: на crates.io НЕТ ни одного QVD crate.** Мы будем первыми.

### Коммерческие инструменты

| Инструмент | Статус | Описание |
|---|---|---|
| EasyQlik QViewer | **Закрыт** (заменён CSViewer) | Был основным QVD-просмотрщиком |
| EasyQlik CSViewer | Бесплатный | Просмотр QVD, Parquet и других форматов |
| Q-Eye | Бесплатный | QVD/QVX viewer и editor (Microsoft Store) |
| QData | Open source | Desktop viewer/editor на PyQvd |
| Alteryx QVD Tools | Community prototype | Чтение/запись, но с багами (даты=NULL, ошибки symbol table) |

### Тренд QVD → Parquet

С августа 2023 Qlik Sense нативно поддерживает Parquet для STORE/LOAD. Однако:
- **Проблема dual values**: QVD хранит loosely-typed duals (число+строка), Parquet — strongly-typed. Конвертация теряет данные.
- **Проблема смешанных типов**: одно поле QVD может содержать int, double и string. Parquet не может.
- **Размер**: QVD 1.7 ГБ → Parquet без сжатия 6 ГБ (dictionary encoding + bit stuffing в QVD очень эффективны).
- **Вывод**: QVD остаётся актуальным, особенно внутри Qlik-экосистемы.

---

## 10. Рыночные возможности и боли пользователей

### Что ищут пользователи (и не находят)

1. **SQL-запросы к QVD вне Qlik** — обсуждается на Qlik Community с 2017 года, решения нет
2. **QVD + DuckDB** — нет расширения DuckDB для QVD
3. **QVD + ClickHouse** — нет интеграции
4. **Streaming QVD reader** — все библиотеки грузят файл целиком в память
5. **Быстрая Python-библиотека с записью** — PyQvd пишет, но медленно; qvd-utils быстрый, но read-only

### Vendor lock-in

- QVD — проприетарный формат, данные заперты внутри Qlik
- Экспорт QVD из Qlik Cloud **не поддерживается** напрямую
- Организации хотят использовать QVD-данные в AI/ML пайплайнах
- Единственный путь — конвертация через Python (медленно) или CSV-export (неэффективно)

### Как rustqvd решает эти проблемы

| Боль пользователя | Решение rustqvd |
|---|---|
| "Нужен SQL к QVD" | DataFusion TableProvider / DuckDB VTab |
| "Нужна конвертация в Parquet" | Arrow RecordBatch → Parquet (без промежуточных файлов) |
| "QVD reload слишком медленный" | Pre-filter/pre-join вне Qlik, optimized load готового QVD |
| "Нет streaming reader" | Streaming reader с чанками (планируется) |
| "Python qvd-библиотека устарела" | PyO3 биндинги, read+write, 30x быстрее PyQvd |

---

## 11. План развития

### Фаза 1 — Core (ГОТОВО)

- [x] Чтение QVD (XML header + symbol tables + bit-stuffed index)
- [x] Запись QVD (byte-identical roundtrip)
- [x] EXISTS() через HashSet (O(1) lookup)
- [x] Python-биндинги (PyO3/maturin)
- [x] Тестирование на 20 реальных файлах (до 2.8 ГБ, 87M строк)
- [ ] Публикация на crates.io
- [ ] Публикация на PyPI

### Фаза 2 — Arrow & Streaming

- [ ] Streaming reader (чтение чанками, не грузить весь файл в память)
- [ ] Arrow RecordBatch output (мост к DuckDB, DataFusion, Polars)
- [ ] DuckDB VTab (виртуальная таблица QVD в DuckDB)
- [ ] CLI утилита (`qvd inspect`, `qvd head`, `qvd convert`, `qvd sql`)

### Фаза 3 — SQL Engine

- [ ] DataFusion TableProvider (SQL-запросы прямо к QVD файлам)
- [ ] Projection pushdown (читать только нужные колонки)
- [ ] Filter pushdown (фильтрация на уровне символов)
- [ ] JOIN нескольких QVD через DataFusion
- [ ] QVD → Parquet конвертация со сжатием

### Фаза 4 — ETL Platform

- [ ] Инкрементальное слияние QVD файлов
- [ ] Валидация данных (типы, дубликаты, свежесть)
- [ ] WASM-модуль для браузера (QVD Viewer Online)
- [ ] Polars IO plugin

---

## 12. Интеграция с экосистемой Rust Data Engineering

### Ключевой принцип: Arrow как универсальный мост

```
QVD файл ──► rustqvd ──► Arrow RecordBatch ──┬──► DuckDB (SQL)
                                               ├──► DataFusion (SQL)
                                               ├──► Polars (DataFrame)
                                               ├──► Parquet (файл)
                                               ├──► ClickHouse (import)
                                               └──► Delta Lake (таблица)
```

### DataFusion TableProvider

```rust
// QVD файлы как SQL-таблицы
let ctx = SessionContext::new();
ctx.register_table("sales", Arc::new(QvdTableProvider::open("sales.qvd")?))?;

let df = ctx.sql("
    SELECT region, SUM(amount) as total
    FROM sales
    WHERE date >= '2024-01-01'
    GROUP BY region
").await?;
```

### DuckDB Virtual Table

```rust
// QVD как виртуальная таблица DuckDB
let conn = Connection::open_in_memory()?;
conn.register_table_function::<QvdVTab>("read_qvd")?;

let result = conn.query_arrow("
    SELECT * FROM read_qvd('facts.qvd') f
    JOIN read_qvd('dim.qvd') d ON f.id = d.id
")?;
```

### Streaming Reader (планируется)

```rust
// Не грузит весь файл в память
let reader = QvdStreamReader::open("facts_50gb.qvd")?;
for batch in reader.batches(65536) {
    // batch: arrow::RecordBatch (~65K строк)
    // обрабатываем чанками, RAM = O(chunk_size)
}
```

### Ландшафт Rust Data Engineering

| Crate | Назначение | Связь с rustqvd |
|---|---|---|
| `arrow` | In-memory columnar формат | Выход rustqvd → RecordBatch |
| `datafusion` | SQL query engine | TableProvider для QVD |
| `polars` | DataFrame библиотека | IO plugin для QVD |
| `duckdb` | Embedded OLAP БД | VTab для QVD |
| `delta-rs` | Delta Lake таблицы | QVD → Delta конвертация |
| `parquet` | Parquet файлы | QVD → Parquet конвертация |
| `lance` | AI-optimized формат | QVD → Lance конвертация |

---

## 13. Практические сценарии применения

### Сценарий 1: ETL вне Qlik

```
Проблема: 400 Qlik скриптов, 3.5 ТБ QVD, тяжёлые JOIN на 3-15 млрд строк.
          Qlik съедает 390 ГБ RAM, reload занимает часы.

Решение:
  Source QVD → [rustqvd + DuckDB: JOIN, GROUP BY, Window] → Result QVD
                50 ГБ RAM, минуты

  Result QVD → [Qlik: optimized load] → Dashboard
                0 ГБ RAM, секунды
```

### Сценарий 2: Инкрементальная обработка

```
День N:
  prev_result.qvd (готов)  +  facts_day_N.qvd (30 МБ)
       ↓                          ↓
  [rustqvd + DuckDB: UNION ALL + GROUP BY]
       ↓
  result_day_N.qvd

Время: секунды. RAM: < 1 ГБ. Qlik не нужен.
```

### Сценарий 3: Pre-filter для Qlik

```
Проблема: WHERE EXISTS() ломает optimized load в Qlik.

Решение:
  big_facts.qvd ──► [rustqvd: filter_rows_by_exists_fast()]
                         ↓
                    filtered_facts.qvd (меньший файл)
                         ↓
                    [Qlik: optimized load — 100x быстрее]
```

### Сценарий 4: QVD как источник для BI без Qlik

```
QVD файлы (3.5 ТБ) ──► [DataFusion + rustqvd]
                              ↓
                         SQL API / REST API
                              ↓
                    Metabase / Grafana / Jupyter
```

### Сценарий 5: Валидация и мониторинг

```bash
# CLI: проверка свежести данных
qvd inspect /data/qvd/**/*.qvd --check-freshness 24h

# CLI: поиск дубликатов
qvd sql "SELECT id, COUNT(*) FROM 'facts.qvd' GROUP BY id HAVING COUNT(*) > 1"

# CLI: конвертация
qvd convert facts.qvd --to parquet --compression zstd
```

---

## 14. Известные edge cases формата QVD

### Обнаруженные особенности

1. **Dual values для дат**: TIME и TIMESTAMP оба хранятся как dual double. Различаются только по `<NumberFormat><Type>` в XML (TIME vs TIMESTAMP).

2. **Смешанные типы в одном поле**: QVD позволяет хранить int, double и string в одном поле. При конвертации в strongly-typed форматы (Parquet, Arrow) нужно выбирать общий тип.

3. **Symbol table inconsistency**: при записи QVD критически важно точно воспроизвести формат symbol table. Прототип Alteryx QVD Tools имеет баг с "Symbol table inconsistency" — наша библиотека это решает через byte-identical roundtrip.

4. **Нулевые байты после XML**: между XML и бинарной частью могут быть `\r\n\0` или просто `\0`. Наш парсер обрабатывает оба варианта.

5. **NumberFormat Type**: в 90% файлов = "UNKNOWN" или "0". Тип данных определяется ТОЛЬКО по flag byte перед каждым символом в symbol table, а НЕ из XML метаданных.

6. **BitOffset не обязательно последовательны**: поля могут быть расположены в index table в произвольном порядке (определяется BitOffset), не совпадающем с порядком в XML.

7. **Пустые symbol tables**: если `NoOfSymbols = 0` и `BitWidth = 0`, поле всегда NULL.

8. **Кодировка строк**: UTF-8 (указано в XML заголовке). Поддерживаются Unicode символы включая emoji.

---

## Источники

### Формат QVD
- [PyQvd Documentation — QVD File Format](https://pyqvd.readthedocs.io/stable/guide/qvd-file-format.html)
- [Qlik Cloud Help — QVD files](https://help.qlik.com/en-US/cloud-services/Subsystems/Hub/Content/Sense_Hub/Scripting/QVD-files-scripting.htm)
- [Qlik Help — Working with QVD files](https://help.qlik.com/en-US/cloud-services/Subsystems/Hub/Content/Sense_Hub/Scripting/work-with-QVD-files.htm)
- [QVD Reverse Engineering Blog (Alteryx)](https://kongsoncheung.blogspot.com/2023/07/qlikview-data-file-reverse-engineering.html)
- Статьи на Хабре: "QVD файлы — что внутри" (части 1-3)

### Существующие реализации
- [SBentley/qvd-utils (GitHub)](https://github.com/SBentley/qvd-utils) — Rust+PyO3, read-only
- [MuellerConstantin/PyQvd (GitHub)](https://github.com/MuellerConstantin/PyQvd) — Pure Python, read+write
- [korolmi/qvdfile (GitHub)](https://github.com/korolmi/qvdfile) — исследовательская версия
- [devinsmith/qvdreader (GitHub)](https://github.com/devinsmith/qvdreader) — C
- [mafuentes22/qvd-reader (GitHub)](https://github.com/mafuentes22/qvd-reader) — JavaScript
- [MuellerConstantin/qvd4js (GitHub)](https://github.com/MuellerConstantin/qvd4js) — JavaScript/Node.js
- [MuellerConstantin/qdata (GitHub)](https://github.com/MuellerConstantin/qdata) — Desktop viewer
- [kongson-cheung/Alteryx-QVD-Tools (GitHub)](https://github.com/kongson-cheung/Alteryx-QVD-Tools) — Alteryx prototype
- [ralfbecher/QVDConverter (GitHub)](https://github.com/ralfbecher/QlikView_QVDReader_Examples) — Java

### Qlik производительность
- [Qlik Help — EXISTS function](https://help.qlik.com/en-US/sense/November2025/Subsystems/Hub/Content/Sense_Hub/Scripting/InterRecordFunctions/Exists.htm)
- [BitMetric — Qlik Optimized Load](https://www.bitmetric.nl/blog/qlik-optimized-load/)
- [Quick Intelligence — Optimised QVD Loads](https://www.quickintelligence.co.uk/qlikview-optimised-qvd-loads/)
- [BigBear.ai — Optimizing Qlik Load Time](https://bigbear.ai/blog/optimizing-qlik-load-time/)
- [Qlik Community — EXISTS and Optimized Load](https://community.qlik.com/t5/QlikView-App-Dev/using-exists-function-to-load-qvd-as-optimized-load/td-p/123585)
- [Qlik Community — ApplyMap vs Join Performance](https://community.qlik.com/t5/Visualization-and-Usability/Lookup-vs-Join-Performance-ApplyMap-vs-Inner-Join/td-p/1811152)

### Rust Data Engineering
- [DataFusion — TableProvider trait](https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html)
- [DataFusion — Custom Table Providers guide](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)
- [datafusion-table-providers (GitHub)](https://github.com/datafusion-contrib/datafusion-table-providers)
- [DuckDB Rust crate — VTab](https://docs.rs/duckdb/latest/duckdb/)
- [arrow-rs — RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html)
- [Polars — IO Plugins](https://docs.pola.rs/user-guide/plugins/io_plugins/)
- [Polars vs DataFusion 2026](https://dasroot.net/posts/2026/01/rust-data-processing-polars-vs-datafusion/)

### Рыночный контекст
- [Pandas Issue #18259 — read_qvd request](https://github.com/pandas-dev/pandas/issues/18259)
- [Qlik Community — SQL on QVD](https://community.qlik.com/t5/QlikView/How-to-perform-SQL-query-on-QVD/td-p/1568409)
- [Quo Vadis QVD — vendor lock-in](https://www.linkedin.com/pulse/quo-vadis-qvd-need-qlik-sense-data-files-2020-boris-michel)
- [Rise of Parquet as QVD replacement](https://medium.com/@durgesh.patel13/rise-of-parquet-files-as-a-replacement-for-qvd-in-data-analytics-0d9ed6668154)
- [Offload Qlik data to Lakehouse](https://medium.com/@irregularbi/offload-your-qlik-data-into-a-lakehouse-finally-1c6a27b9733c)
- [Qlik Community — Parquet vs QVD](https://community.qlik.com/t5/Connectivity-Data-Prep/parquet-vs-qvd-QS-August-2023/td-p/2111224)
- [EasyQlik QViewer (retired)](https://easyqlik.com/qviewer/)
- [EasyQlik CSViewer](https://easyqlik.com/csviewer/)
- [Q-Eye QVD Viewer](https://www.etl-tools.com/products/q-eye.html)
