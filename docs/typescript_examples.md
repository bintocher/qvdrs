# TypeScript / Node.js Examples

> Requires Node.js 22+. All I/O operations are async by default (run on libuv thread pool, never block the event loop).

## Install

```bash
npm install qvdrs
```

## Read and Write QVD

```typescript
import { readQvd, saveQvd } from 'qvdrs'

const table = await readQvd('data.qvd')
console.log(`Rows: ${table.numRows}, Cols: ${table.numCols}`)
console.log(`Table: ${table.tableName}`)
console.log(`Columns: ${table.columns}`)

// Save (byte-identical roundtrip)
await saveQvd(table, 'copy.qvd')
```

## Sync Variants (for scripts/CLI)

```typescript
import { readQvdSync, saveQvdSync } from 'qvdrs'

const table = readQvdSync('data.qvd')
saveQvdSync(table, 'copy.qvd')
```

## Access Cell Values

```typescript
const table = await readQvd('data.qvd')

// By index
const val = table.get(0, 0) // row 0, col 0

// By column name
const name = table.getByName(0, 'ClientName')

// Entire column
const ids = table.columnValuesByName('ClientID')
```

## Convert to JSON

```typescript
const table = await readQvd('data.qvd')

// All rows
const allRows = table.toJson()
// [{col1: "val1", col2: "val2"}, ...]

// First 5 rows
const preview = table.head(5)
```

## EXISTS() — O(1) Lookup

```typescript
import { readQvd, JsExistsIndex, filterExists } from 'qvdrs'

// Build index from a table column
const clients = await readQvd('clients.qvd')
const index = JsExistsIndex.fromColumn(clients, 'ClientID')

// Or from explicit values
const index2 = JsExistsIndex.fromValues(['100', '200', '300'])

// O(1) lookup
console.log(index.exists('12345')) // true/false
console.log(index.len)             // number of unique values

// Filter rows — returns matching row indices
const facts = await readQvd('facts.qvd')
const matchingRows = filterExists(facts, 'ClientID', index)
const filtered = facts.subsetRows(matchingRows)
```

## Streaming Filtered Read

```typescript
import { readQvdFiltered, JsExistsIndex } from 'qvdrs'

// Reads only matching rows — memory-efficient for large files
const table = await readQvdFiltered(
  'large_file.qvd',
  'ActionID',                         // filter column
  ['7', '9'],                         // values to match
  ['ClientID', 'Date', 'ActionID'],   // select columns (optional)
  65536                               // chunk size (optional)
)
```

## Filter by Values

```typescript
const table = await readQvd('data.qvd')
const filtered = table.filterByValues('Status', ['Active', 'Pending'])
console.log(filtered.numRows)
```

## Concatenate — Pure Append

```typescript
import { readQvd, saveQvd } from 'qvdrs'

const jan = await readQvd('sales_jan.qvd')
const feb = await readQvd('sales_feb.qvd')

// Strict mode (default) — columns must match
const merged = jan.concatenate(feb)
await saveQvd(merged, 'sales_q1.qvd')

// Union mode — missing columns filled with NULL
const merged2 = jan.concatenate(feb, 'union')
```

### File-Level Concatenation

```typescript
import { concatenateQvd } from 'qvdrs'

// Read + merge + write in one async operation
await concatenateQvd('jan.qvd', 'feb.qvd', 'all.qvd', 'strict')
```

## Concatenate with PK — Upsert/Dedup

```typescript
const master = await readQvd('master.qvd')
const delta = await readQvd('delta.qvd')

// Replace: new rows win on PK collision (upsert)
const updated = master.concatenatePk(delta, ['ID'], 'replace')

// Skip: existing rows kept on collision
const safe = master.concatenatePk(delta, ['ID'], 'skip')

// Error: throw if any PK collision
const strict = master.concatenatePk(delta, ['ID'], 'error')

// Composite PK
const multi = master.concatenatePk(delta, ['RegionID', 'ProductID'], 'replace')
```

### File-Level PK Merge

```typescript
import { concatenatePkQvd } from 'qvdrs'

await concatenatePkQvd(
  'master.qvd',
  'delta.qvd',
  'result.qvd',
  ['ID'],
  'replace',  // on_conflict
  'strict'    // schema
)
```

## Symbols (Distinct Values)

```typescript
const table = await readQvd('data.qvd')

const symbols = table.symbols('Status')
// ["Active", "Pending", "Closed"]

const count = table.numSymbols('Status')
// 3
```

## Normalize for Qlik Compatibility

```typescript
const table = await readQvd('data.qvd')
table.normalize() // Converts DualInt→Int, sets proper NumberFormat/Tags
await saveQvd(table, 'normalized.qvd')
```

## Error Handling

```typescript
import { readQvd } from 'qvdrs'

try {
  const table = await readQvd('nonexistent.qvd')
} catch (e) {
  console.error(e.message) // "No such file or directory..."
}
```

## Typical ETL Pipeline

```typescript
import { readQvd, saveQvd, JsExistsIndex, readQvdFiltered } from 'qvdrs'

// 1. Load dimension table
const clients = await readQvd('dim_clients.qvd')
const clientIndex = JsExistsIndex.fromColumn(clients, 'ClientID')

// 2. Stream-filter fact table (only matching clients)
const facts = await readQvdFiltered(
  'fact_transactions.qvd',
  'ClientID',
  [...Array.from({ length: clientIndex.len }, (_, i) => String(i))],
  ['ClientID', 'Amount', 'Date']
)

// 3. Save filtered result
await saveQvd(facts, 'fact_filtered.qvd')
console.log(`Filtered: ${facts.numRows} rows`)
```
