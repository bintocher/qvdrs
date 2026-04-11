/**
 * High-performance Qlik QVD file reader/writer — Rust-powered Node.js bindings.
 *
 * All I/O operations have both async (default) and sync variants.
 * Async functions run on the libuv thread pool and return Promises.
 */

/** QVD table — the main data structure. */
export class JsQvdTable {
  /** Number of rows. */
  get numRows(): number
  /** Number of columns. */
  get numCols(): number
  /** Table name from QVD metadata. */
  get tableName(): string
  /** Column names. */
  get columns(): string[]

  /** Get a single cell value by row and column index. */
  get(row: number, col: number): string | null
  /** Get a single cell value by row index and column name. */
  getByName(row: number, colName: string): string | null
  /** Get all values of a column by index. */
  columnValues(col: number): (string | null)[]
  /** Get all values of a column by name. */
  columnValuesByName(colName: string): (string | null)[]

  /** Convert to array of row objects. */
  toJson(): Record<string, string | null>[]
  /** Get first N rows as array of objects (default: 10). */
  head(n?: number): Record<string, string | null>[]

  /** Get unique symbols (distinct values) for a column. */
  symbols(colName: string): string[]
  /** Number of unique symbols in a column. */
  numSymbols(colName: string): number

  /** Filter rows where column matches any of the given values. */
  filterByValues(colName: string, values: string[]): JsQvdTable
  /** Create a new table from a subset of row indices. */
  subsetRows(rowIndices: number[]): JsQvdTable

  /** Normalize for maximum Qlik Sense compatibility. */
  normalize(): void

  /**
   * Concatenate with another table (pure append).
   * @param other - Table to append
   * @param schema - "strict" (default) or "union"
   */
  concatenate(other: JsQvdTable, schema?: 'strict' | 'union'): JsQvdTable

  /**
   * Concatenate with PK-based deduplication.
   * @param other - Table with new/updated rows
   * @param pk - Primary key column(s)
   * @param onConflict - "replace" (default), "skip", or "error"
   * @param schema - "strict" (default) or "union"
   */
  concatenatePk(
    other: JsQvdTable,
    pk: string[],
    onConflict?: 'replace' | 'skip' | 'error',
    schema?: 'strict' | 'union',
  ): JsQvdTable
}

/** O(1) hash index for EXISTS() lookups. */
export class JsExistsIndex {
  /** Number of unique values in the index. */
  get len(): number
  /** Whether the index is empty. */
  get isEmpty(): boolean

  /** Build from a QvdTable column. */
  static fromColumn(table: JsQvdTable, colName: string): JsExistsIndex
  /** Build from an explicit list of values. */
  static fromValues(values: string[]): JsExistsIndex

  /** Check if a value exists (O(1)). */
  exists(value: string): boolean
  /** Check multiple values at once. */
  existsMany(values: string[]): boolean[]
}

// ── Async functions (return Promise, run on thread pool) ──

/** Read a QVD file. Returns Promise<JsQvdTable>. */
export function readQvd(path: string): Promise<JsQvdTable>

/** Save a QvdTable to a file. Returns Promise<void>. */
export function saveQvd(table: JsQvdTable, path: string): Promise<void>

/**
 * Read QVD with streaming EXISTS() filter.
 * @param path - Path to QVD file
 * @param filterCol - Column to filter on
 * @param values - Values to match (EXISTS semantics)
 * @param select - Optional column selection
 * @param chunkSize - Chunk size for streaming (default: 65536)
 */
export function readQvdFiltered(
  path: string,
  filterCol: string,
  values: string[],
  select?: string[],
  chunkSize?: number,
): Promise<JsQvdTable>

/**
 * Concatenate two QVD files and write result.
 * @param pathA - First QVD file
 * @param pathB - Second QVD file
 * @param outputPath - Output QVD file
 * @param schema - "strict" (default) or "union"
 */
export function concatenateQvd(
  pathA: string,
  pathB: string,
  outputPath: string,
  schema?: 'strict' | 'union',
): Promise<void>

/**
 * Concatenate two QVD files with PK dedup and write result.
 * @param pathA - Existing QVD file
 * @param pathB - New/updated QVD file
 * @param outputPath - Output QVD file
 * @param pk - Primary key column(s)
 * @param onConflict - "replace" (default), "skip", or "error"
 * @param schema - "strict" (default) or "union"
 */
export function concatenatePkQvd(
  pathA: string,
  pathB: string,
  outputPath: string,
  pk: string[],
  onConflict?: 'replace' | 'skip' | 'error',
  schema?: 'strict' | 'union',
): Promise<void>

// ── Sync functions (block event loop — for scripts/CLI) ──

/** Read a QVD file synchronously. */
export function readQvdSync(path: string): JsQvdTable

/** Save a QvdTable to a file synchronously. */
export function saveQvdSync(table: JsQvdTable, path: string): void

// ── Utility ──

/**
 * Filter rows where column value exists in the index.
 * Returns matching row indices.
 */
export function filterExists(
  table: JsQvdTable,
  colName: string,
  index: JsExistsIndex,
): number[]
