'use strict'

const assert = require('node:assert/strict')
const { mkdtempSync, mkdirSync, writeFileSync, rmSync } = require('node:fs')
const { tmpdir } = require('node:os')
const path = require('node:path')
const test = require('node:test')
const { publishPackage, publishAll } = require('./publish-npm.cjs')

const repository = { type: 'git', url: 'git+https://github.com/bintocher/qvdrs.git' }
const manifest = { name: 'qvdrs-linux-x64-gnu', version: '0.7.2', repository }
const item = { directory: '/fixture/linux-x64-gnu', manifest }
const missing = { status: 1, stdout: JSON.stringify({ error: { code: 'E404' } }), stderr: '' }
const success = { status: 0, stdout: '', stderr: '' }
const quiet = () => {}

function existing(overrides = {}) {
  return { status: 0, stdout: JSON.stringify({ ...manifest, dist: { integrity: 'sha512-fixture' }, ...overrides }), stderr: '' }
}

test('403 не разрешает публикацию', () => {
  const calls = []
  const npm = args => {
    calls.push(args)
    return { status: 1, stdout: JSON.stringify({ error: { code: 'E403' } }), stderr: '' }
  }
  assert.throws(() => publishPackage(item, { npm, log: quiet }), /E403/)
  assert.equal(calls.length, 1)
  assert.equal(calls[0][0], 'view')
})

test('404 разрешает публикацию с provenance и public', () => {
  const calls = []
  const npm = (args, directory) => {
    calls.push({ args, directory })
    return args[0] === 'view' ? missing : success
  }
  assert.equal(publishPackage(item, { npm, log: quiet }), 'published')
  assert.equal(calls.length, 2)
  assert.deepEqual(calls[1], {
    args: ['publish', '--provenance', '--access', 'public', '--registry=https://registry.npmjs.org/'],
    directory: item.directory,
  })
})

test('существующая версия с корректным репозиторием пропускается', () => {
  const calls = []
  const npm = args => { calls.push(args); return existing() }
  assert.equal(publishPackage(item, { npm, log: quiet }), 'existing')
  assert.equal(calls.length, 1)
})

test('npm 12: точная существующая версия в массиве пропускается', () => {
  const response = existing()
  response.stdout = JSON.stringify([JSON.parse(response.stdout)])
  let calls = 0
  assert.equal(publishPackage(item, {
    npm: () => { calls++; return response }, log: quiet,
  }), 'existing')
  assert.equal(calls, 1)
})

test('неоднозначный или пустой массив версий блокирует публикацию', () => {
  const metadata = JSON.parse(existing().stdout)
  for (const values of [[], [metadata, metadata]]) {
    let calls = 0
    const npm = () => { calls++; return { status: 0, stdout: JSON.stringify(values), stderr: '' } }
    assert.throws(() => publishPackage(item, { npm, log: quiet }), /не прошел проверку/)
    assert.equal(calls, 1)
  }
})

test('чужой репозиторий существующей версии блокирует публикацию', () => {
  let calls = 0
  const npm = () => { calls++; return existing({ repository: { url: 'https://github.com/another/qvdrs' } }) }
  assert.throws(() => publishPackage(item, { npm, log: quiet }), /не прошел проверку/)
  assert.equal(calls, 1)
})

test('пустая integrity и несовпадающая версия не считаются успешным выпуском', () => {
  for (const metadata of [{ dist: { integrity: ' ' } }, { version: '0.7.1' }, { name: 'another-package' }]) {
    assert.throws(() => publishPackage(item, { npm: () => existing(metadata), log: quiet }), /не прошел проверку/)
  }
})

test('сетевая ошибка и E404 только в stderr не разрешают публикацию', () => {
  for (const result of [
    { status: null, stdout: '', stderr: '', error: new Error('network') },
    { status: 1, stdout: '', stderr: 'npm error code E404' },
  ]) {
    let calls = 0
    assert.throws(() => publishPackage(item, { npm: () => { calls++; return result }, log: quiet }))
    assert.equal(calls, 1)
  }
})

function fixture(t) {
  const directory = mkdtempSync(path.join(tmpdir(), 'qvdrs-publish-'))
  t.after(() => rmSync(directory, { recursive: true, force: true }))
  const optionalDependencies = {
    'qvdrs-linux-x64-gnu': '0.7.2',
    '@bintocher/qvdrs-win32-x64-msvc': '0.7.2',
  }
  for (const [subdirectory, name] of [
    ['linux-x64-gnu', 'qvdrs-linux-x64-gnu'],
    ['win32-x64-msvc', '@bintocher/qvdrs-win32-x64-msvc'],
  ]) {
    const target = path.join(directory, 'npm', subdirectory)
    mkdirSync(target, { recursive: true })
    writeFileSync(path.join(target, 'package.json'), JSON.stringify({ name, version: '0.7.2', repository }))
  }
  const main = { name: 'qvdrs', version: '0.7.2', repository, optionalDependencies }
  writeFileSync(path.join(directory, 'package.json'), JSON.stringify(main))
  return { directory, main }
}

test('при ошибке Windows основной пакет не проверяется и не публикуется', t => {
  const { directory } = fixture(t)
  const calls = []
  const npm = (args, cwd) => {
    calls.push({ args, cwd })
    if (args[0] === 'view') return missing
    return cwd.endsWith('win32-x64-msvc') ?
      { status: 1, stdout: JSON.stringify({ error: { code: 'E403' } }), stderr: '' } : success
  }
  assert.throws(() => publishAll(directory, { npm, log: quiet }), /E403/)
  assert.equal(calls.filter(call => call.args[0] === 'publish').length, 2)
  assert.ok(calls.every(call => call.cwd !== directory))
})

test('несовпадающая optionalDependency блокирует все сетевые операции', t => {
  const { directory, main } = fixture(t)
  main.optionalDependencies['@bintocher/qvdrs-win32-x64-msvc'] = '0.7.1'
  writeFileSync(path.join(directory, 'package.json'), JSON.stringify(main))
  let calls = 0
  assert.throws(() => publishAll(directory, { npm: () => { calls++; return success }, log: quiet }), /не совпадают/)
  assert.equal(calls, 0)
})

test('основной пакет публикуется после всех платформенных', t => {
  const { directory } = fixture(t)
  const publishedDirectories = []
  const npm = (args, cwd) => {
    if (args[0] === 'view') return missing
    publishedDirectories.push(cwd)
    return success
  }
  assert.equal(publishAll(directory, { npm, log: quiet }).length, 3)
  assert.equal(publishedDirectories.at(-1), directory)
})
