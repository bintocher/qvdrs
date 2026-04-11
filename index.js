/* eslint-disable no-console */

const { existsSync, readFileSync } = require('fs')
const { join } = require('path')

const { platform, arch } = process

let nativeBinding = null
let loadError = null

// Try loading local development build first (napi build output)
try {
  nativeBinding = require(join(__dirname, 'qvdrs.node'))
} catch {
  // Fall through to platform-specific loading
}

function isMusl() {
  // For Node 12+
  if (!process.report || typeof process.report.getReport !== 'function') {
    try {
      const lddPath = require('child_process').execSync('which ldd').toString().trim()
      return readFileSync(lddPath, 'utf8').includes('musl')
    } catch {
      return true
    }
  } else {
    const { glibcVersionRuntime } = process.report.getReport().header
    return !glibcVersionRuntime
  }
}

if (!nativeBinding) switch (platform) {
  case 'win32':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(join(__dirname, 'qvdrs.win32-x64-msvc.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./qvdrs.win32-x64-msvc.node')
          } else {
            nativeBinding = require('qvdrs-win32-x64-msvc')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(`Unsupported architecture on Windows: ${arch}`)
    }
    break
  case 'darwin':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(join(__dirname, 'qvdrs.darwin-x64.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./qvdrs.darwin-x64.node')
          } else {
            nativeBinding = require('qvdrs-darwin-x64')
          }
        } catch (e) {
          loadError = e
        }
        break
      case 'arm64':
        localFileExisted = existsSync(join(__dirname, 'qvdrs.darwin-arm64.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./qvdrs.darwin-arm64.node')
          } else {
            nativeBinding = require('qvdrs-darwin-arm64')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(`Unsupported architecture on macOS: ${arch}`)
    }
    break
  case 'linux':
    switch (arch) {
      case 'x64':
        localFileExisted = existsSync(join(__dirname, 'qvdrs.linux-x64-gnu.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./qvdrs.linux-x64-gnu.node')
          } else {
            nativeBinding = require('qvdrs-linux-x64-gnu')
          }
        } catch (e) {
          loadError = e
        }
        break
      case 'arm64':
        localFileExisted = existsSync(join(__dirname, 'qvdrs.linux-arm64-gnu.node'))
        try {
          if (localFileExisted) {
            nativeBinding = require('./qvdrs.linux-arm64-gnu.node')
          } else {
            nativeBinding = require('qvdrs-linux-arm64-gnu')
          }
        } catch (e) {
          loadError = e
        }
        break
      default:
        throw new Error(`Unsupported architecture on Linux: ${arch}`)
    }
    break
  default:
    throw new Error(`Unsupported OS: ${platform}, architecture: ${arch}`)
}

if (!nativeBinding) {
  if (loadError) {
    throw loadError
  }
  throw new Error('Failed to load native binding')
}

module.exports = nativeBinding
