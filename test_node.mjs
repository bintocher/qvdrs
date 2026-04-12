// Quick test of the qvdrs Node.js bindings
import { createRequire } from 'module'
const require = createRequire(import.meta.url)
const qvd = require('./index.js')

const {
  readQvd, readQvdSync, saveQvd, saveQvdSync,
  filterExists, readQvdFiltered,
  concatenateQvd,
  JsQvdTable, JsExistsIndex
} = qvd

const TEST_FILE = 'qvd_input/AAPL.qvd'
const OUT_FILE = 'qvd_output/test_node_output.qvd'
let passed = 0
let failed = 0

function assert(cond, msg) {
  if (cond) {
    passed++
    console.log(`  PASS: ${msg}`)
  } else {
    failed++
    console.log(`  FAIL: ${msg}`)
  }
}

async function main() {
  console.log('=== qvdrs Node.js bindings test ===\n')

  // 1. Sync read
  console.log('1. readQvdSync')
  const table = readQvdSync(TEST_FILE)
  assert(table.numRows > 0, `numRows = ${table.numRows}`)
  assert(table.numCols > 0, `numCols = ${table.numCols}`)
  assert(table.tableName.length > 0, `tableName = "${table.tableName}"`)
  assert(table.columns.length === table.numCols, `columns.length = ${table.columns.length}`)
  console.log(`   Columns: ${table.columns.join(', ')}`)

  // 2. Async read
  console.log('\n2. readQvd (async)')
  const table2 = await readQvd(TEST_FILE)
  assert(table2.numRows === table.numRows, `async numRows matches sync: ${table2.numRows}`)

  // 3. Cell access
  console.log('\n3. Cell access')
  const val = table.get(0, 0)
  assert(val !== undefined, `get(0,0) = "${val}"`)
  const byName = table.getByName(0, table.columns[0])
  assert(byName !== undefined, `getByName(0, "${table.columns[0]}") = "${byName}"`)

  // 4. Column values
  console.log('\n4. Column values')
  const col0 = table.columnValues(0)
  assert(col0.length === table.numRows, `columnValues(0).length = ${col0.length}`)
  const colByName = table.columnValuesByName(table.columns[0])
  assert(colByName.length === table.numRows, `columnValuesByName.length = ${colByName.length}`)

  // 5. head / toJson
  console.log('\n5. head / toJson')
  const headRows = table.head(3)
  assert(headRows.length === 3, `head(3).length = ${headRows.length}`)
  console.log(`   First row: ${JSON.stringify(headRows[0]).substring(0, 100)}...`)

  // 6. Symbols
  console.log('\n6. Symbols')
  const syms = table.symbols(table.columns[0])
  const nSyms = table.numSymbols(table.columns[0])
  assert(syms.length === nSyms, `symbols.length = ${syms.length}, numSymbols = ${nSyms}`)

  // 7. Filter by values
  console.log('\n7. filterByValues')
  if (syms.length > 0) {
    const filtered = table.filterByValues(table.columns[0], [syms[0]])
    assert(filtered.numRows > 0, `filtered by "${syms[0]}": ${filtered.numRows} rows`)
  }

  // 8. Subset rows
  console.log('\n8. subsetRows')
  const subset = table.subsetRows([0, 1, 2])
  assert(subset.numRows === 3, `subsetRows([0,1,2]).numRows = ${subset.numRows}`)

  // 9. ExistsIndex
  console.log('\n9. ExistsIndex')
  const idx = JsExistsIndex.fromColumn(table, table.columns[0])
  assert(idx.len > 0, `ExistsIndex.len = ${idx.len}`)
  assert(idx.isEmpty === false, `isEmpty = false`)
  if (syms.length > 0) {
    assert(idx.exists(syms[0]) === true, `exists("${syms[0]}") = true`)
  }
  const idx2 = JsExistsIndex.fromValues(['test1', 'test2'])
  assert(idx2.len === 2, `fromValues.len = ${idx2.len}`)
  assert(idx2.exists('test1') === true, `exists("test1") = true`)
  assert(idx2.exists('nope') === false, `exists("nope") = false`)

  // 10. filterExists
  console.log('\n10. filterExists')
  const matchingRows = filterExists(table, table.columns[0], idx)
  assert(matchingRows.length > 0, `filterExists: ${matchingRows.length} matching rows`)

  // 11. Sync save + roundtrip
  console.log('\n11. Sync save + roundtrip')
  saveQvdSync(table, OUT_FILE)
  const reloaded = readQvdSync(OUT_FILE)
  assert(reloaded.numRows === table.numRows, `roundtrip rows: ${reloaded.numRows} === ${table.numRows}`)
  assert(reloaded.numCols === table.numCols, `roundtrip cols: ${reloaded.numCols} === ${table.numCols}`)

  // 12. Async save
  console.log('\n12. Async save')
  await saveQvd(table, 'qvd_output/test_node_async.qvd')
  const reloaded2 = readQvdSync('qvd_output/test_node_async.qvd')
  assert(reloaded2.numRows === table.numRows, `async save roundtrip: ${reloaded2.numRows} rows`)

  // 13. Normalize
  console.log('\n13. normalize')
  const t = readQvdSync(TEST_FILE)
  t.normalize()
  assert(t.numRows === table.numRows, `after normalize: ${t.numRows} rows`)

  // 14. Concatenate
  console.log('\n14. concatenate')
  const a = readQvdSync(TEST_FILE)
  const b = readQvdSync(TEST_FILE)
  const merged = a.concatenate(b)
  assert(merged.numRows === a.numRows + b.numRows, `concatenate: ${merged.numRows} = ${a.numRows} + ${b.numRows}`)

  // 15. Concatenate QVD files (async)
  console.log('\n15. concatenateQvd (async file-level)')
  await concatenateQvd(TEST_FILE, TEST_FILE, 'qvd_output/test_concat.qvd')
  const concatResult = readQvdSync('qvd_output/test_concat.qvd')
  assert(concatResult.numRows === table.numRows * 2, `file concat: ${concatResult.numRows} rows`)

  // 16. Filtered read (async)
  console.log('\n16. readQvdFiltered (async)')
  if (syms.length > 0) {
    const filteredTable = await readQvdFiltered(
      TEST_FILE,
      table.columns[0],
      [syms[0]],
    )
    assert(filteredTable.numRows > 0, `filtered read: ${filteredTable.numRows} rows`)
  }

  // Summary
  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`)
  process.exit(failed > 0 ? 1 : 0)
}

main().catch(e => {
  console.error('FATAL:', e)
  process.exit(1)
})
