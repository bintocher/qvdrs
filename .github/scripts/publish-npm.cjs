'use strict'

const { readFileSync, readdirSync } = require('node:fs')
const path = require('node:path')
const { spawnSync } = require('node:child_process')

function isExpectedRepository(repository) {
  const value = typeof repository === 'string' ? repository : repository?.url
  if (typeof value !== 'string') return false
  try {
    const url = new URL(value.replace(/^git\+/, ''))
    return url.protocol === 'https:' && url.hostname === 'github.com' &&
      !url.username && !url.password && !url.port && !url.search && !url.hash &&
      url.pathname.replace(/\/$/, '').replace(/\.git$/, '').toLowerCase() === '/bintocher/qvdrs'
  } catch {
    return false
  }
}

function readPackage(directory) {
  const manifest = JSON.parse(readFileSync(path.join(directory, 'package.json'), 'utf8'))
  if (typeof manifest.name !== 'string' || !manifest.name ||
      typeof manifest.version !== 'string' || !manifest.version) {
    throw new Error(`Некорректное имя или версия пакета: ${directory}`)
  }
  if (!isExpectedRepository(manifest.repository)) {
    throw new Error(`Неожиданный репозиторий пакета ${manifest.name}`)
  }
  return { directory, manifest }
}

function loadPackages(projectDirectory) {
  const main = readPackage(projectDirectory)
  const nativeDirectory = path.join(projectDirectory, 'npm')
  const native = readdirSync(nativeDirectory, { withFileTypes: true })
    .filter(entry => entry.isDirectory())
    .sort((left, right) => left.name.localeCompare(right.name))
    .map(entry => readPackage(path.join(nativeDirectory, entry.name)))
  const optionalDependencies = main.manifest.optionalDependencies
  if (!native.length || !optionalDependencies || typeof optionalDependencies !== 'object' ||
      Array.isArray(optionalDependencies)) {
    throw new Error('Отсутствуют платформенные пакеты или optionalDependencies')
  }
  const names = new Set()
  for (const { manifest } of native) {
    if (names.has(manifest.name) || manifest.name === main.manifest.name) {
      throw new Error(`Повторное имя платформенного пакета ${manifest.name}`)
    }
    names.add(manifest.name)
    if (manifest.version !== main.manifest.version ||
        optionalDependencies[manifest.name] !== main.manifest.version) {
      throw new Error(`Версии пакетов не совпадают: ${manifest.name}`)
    }
  }
  for (const [name, version] of Object.entries(optionalDependencies)) {
    if (!names.has(name) || version !== main.manifest.version) {
      throw new Error(`Неожиданная зависимость или версия: ${name}`)
    }
  }
  return [...native, main]
}

function runNpm(args, directory) {
  return spawnSync('npm', args, {
    cwd: directory,
    encoding: 'utf8',
    shell: false,
    maxBuffer: 10 * 1024 * 1024,
  })
}

function parseJson(text) {
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

function failureCode(result) {
  const jsonCode = parseJson(result.stdout)?.error?.code
  const code = typeof jsonCode === 'string' ? jsonCode :
    String(result.stderr || '').match(/npm error code ([A-Z0-9_]+)/)?.[1]
  return typeof code === 'string' && /^[A-Z0-9_]+$/.test(code) ? `, ${code}` : ''
}

function publishPackage(item, { npm = runNpm, log = console.log } = {}) {
  const { manifest, directory } = item
  const spec = `${manifest.name}@${manifest.version}`
  const view = npm(['view', spec, '--json', '--registry=https://registry.npmjs.org/'], directory)
  if (view.error || view.status === null || !Number.isInteger(view.status)) {
    throw new Error(`Не удалось выполнить npm view для ${spec}`)
  }
  const response = parseJson(view.stdout)
  // npm 11 возвращает объект, npm 12 - массив даже для одной точной версии.
  const metadata = Array.isArray(response) && response.length === 1 ? response[0] : response
  if (view.status === 0) {
    if (!metadata || Array.isArray(metadata) || metadata.name !== manifest.name ||
        metadata.version !== manifest.version || !isExpectedRepository(metadata.repository) ||
        typeof metadata.dist?.integrity !== 'string' || !metadata.dist.integrity.trim()) {
      throw new Error(`Опубликованный пакет не прошел проверку: ${spec}`)
    }
    log(`[OK] Уже опубликован и проверен: ${spec}`)
    return 'existing'
  }
  if (metadata?.error?.code !== 'E404') {
    throw new Error(`npm view завершился ошибкой для ${spec} (${view.status}${failureCode(view)})`)
  }
  const result = npm([
    'publish', '--provenance', '--access', 'public', '--registry=https://registry.npmjs.org/',
  ], directory)
  if (result.error || result.status !== 0) {
    throw new Error(`npm publish завершился ошибкой для ${spec} (${result.status}${failureCode(result)})`)
  }
  log(`[OK] Опубликован: ${spec}`)
  return 'published'
}

function publishAll(projectDirectory, options = {}) {
  // Проверяем все описания до первой сетевой операции и публикуем основной пакет последним.
  const packages = loadPackages(projectDirectory)
  return packages.map(item => ({
    name: item.manifest.name,
    version: item.manifest.version,
    result: publishPackage(item, options),
  }))
}

module.exports = { isExpectedRepository, loadPackages, publishPackage, publishAll, runNpm }

if (require.main === module) {
  try {
    publishAll(path.resolve(process.argv[2] || path.join(__dirname, '../..')))
  } catch (error) {
    console.error(`[ERR] ${error.message}`)
    process.exitCode = 1
  }
}
