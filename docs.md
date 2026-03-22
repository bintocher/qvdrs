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

## 7. Архитектура нашей библиотеки `rustqvd`

### Возможности
- Чтение QVD файлов (парсинг XML + бинарной части)
- Запись QVD файлов (генерация XML + бинарной части)
- Быстрый `exists()` через HashSet lookup
- Python-биндинги через PyO3/maturin

### Зависимости Rust
- `quick-xml` + `serde` — парсинг/генерация XML
- `pyo3` — Python биндинги
- `maturin` — сборка Python-пакета

### Модули
```
src/
├── lib.rs          — публичный API библиотеки
├── header.rs       — парсинг/генерация XML заголовка
├── symbol.rs       — чтение/запись symbol tables
├── index.rs        — чтение/запись index table (bit-stuffing)
├── value.rs        — типы данных QVD (QvdValue)
├── reader.rs       — высокоуровневый reader
├── writer.rs       — высокоуровневый writer
├── exists.rs       — реализация exists() с HashSet
├── error.rs        — типы ошибок
└── python.rs       — PyO3 биндинги
```

---

## Источники

- [PyQvd Documentation — QVD File Format](https://pyqvd.readthedocs.io/stable/guide/qvd-file-format.html)
- [Qlik Cloud Help — QVD files](https://help.qlik.com/en-US/cloud-services/Subsystems/Hub/Content/Sense_Hub/Scripting/QVD-files-scripting.htm)
- [SBentley/qvd-utils (GitHub)](https://github.com/SBentley/qvd-utils)
- [MuellerConstantin/PyQvd (GitHub)](https://github.com/MuellerConstantin/PyQvd)
- [korolmi/qvdfile (GitHub)](https://github.com/korolmi/qvdfile)
- [devinsmith/qvdreader (GitHub)](https://github.com/devinsmith/qvdreader)
- Статьи на Хабре: "QVD файлы — что внутри" (части 1-3)
- [Qlik Help — EXISTS function](https://help.qlik.com/en-US/sense/November2025/Subsystems/Hub/Content/Sense_Hub/Scripting/InterRecordFunctions/Exists.htm)
