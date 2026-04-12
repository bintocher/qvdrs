// Test all QVD files: read → save → re-read, verify row/col counts match
import { createRequire } from 'module'
import { readdirSync, unlinkSync } from 'fs'
import { join } from 'path'

const require = createRequire(import.meta.url)
const { readQvdSync, saveQvdSync } = require('./index.js')

const INPUT_DIR = 'qvd_input'
const OUT_FILE = 'qvd_output/_roundtrip_test.qvd'

const files = readdirSync(INPUT_DIR).filter(f => f.endsWith('.qvd'))
console.log(`Testing ${files.length} QVD files...\n`)

let passed = 0
let failed = 0
const errors = []
const start = Date.now()

for (const file of files) {
  const path = join(INPUT_DIR, file)
  try {
    // Read
    const table = readQvdSync(path)
    const rows = table.numRows
    const cols = table.numCols
    const name = table.tableName

    // Save
    saveQvdSync(table, OUT_FILE)

    // Re-read
    const reloaded = readQvdSync(OUT_FILE)

    // Verify
    if (reloaded.numRows !== rows) {
      throw new Error(`Row mismatch: ${reloaded.numRows} vs ${rows}`)
    }
    if (reloaded.numCols !== cols) {
      throw new Error(`Col mismatch: ${reloaded.numCols} vs ${cols}`)
    }

    passed++
    process.stdout.write(`\r  ${passed + failed}/${files.length} — ${file} (${rows} rows, ${cols} cols)        `)
  } catch (e) {
    failed++
    errors.push({ file, error: e.message })
    process.stdout.write(`\r  FAIL: ${file} — ${e.message}\n`)
  }

  // Cleanup
  try { unlinkSync(OUT_FILE) } catch {}
}

const elapsed = ((Date.now() - start) / 1000).toFixed(1)
console.log(`\n\n=== Results: ${passed} passed, ${failed} failed (${elapsed}s) ===`)

if (errors.length > 0) {
  console.log('\nFailed files:')
  for (const { file, error } of errors) {
    console.log(`  ${file}: ${error}`)
  }
}

process.exit(failed > 0 ? 1 : 0)
