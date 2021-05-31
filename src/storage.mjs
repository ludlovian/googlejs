import { pipeline } from 'stream/promises'
import { createReadStream, createWriteStream } from 'fs'
import { stat as fsStat, chmod, utimes } from 'fs/promises'
import { extname } from 'path'

import mime from 'mime/lite.js'

import once from 'pixutil/once'
import createSpeedo from 'speedo/gen'
import throttler from 'throttler/gen'
import progressStream from 'progress-stream/gen'
import hashFile from 'hash-stream/simple'
import hashStream from 'hash-stream/gen'

export function parse (uri) {
  const u = new URL(uri)
  if (u.protocol !== 'gs:') throw new Error('Invalid protocol')
  const bucket = u.hostname
  const file = u.pathname.replace(/^\//, '')
  return { bucket, file }
}

export async function stat (uri) {
  const { bucket: bucketName, file: fileName } = parse(uri)
  const storage = await getStorageAPI()
  const bucket = storage.bucket(bucketName)
  const file = bucket.file(fileName)
  const [metadata] = await file.getMetadata()
  return cleanMetadata({
    ...metadata,
    ...unpackMetadata(metadata.metadata)
  })
}

export async function upload (src, dest, options = {}) {
  const { onProgress, progressInterval = 1000, rateLimit, acl } = options
  const { bucket: bucketName, file: fileName } = parse(dest)
  const { contentType, ...metadata } = await getLocalMetadata(src)
  const storage = await getStorageAPI()
  const bucket = storage.bucket(bucketName)
  const file = bucket.file(fileName)

  const speedo = createSpeedo({ total: metadata.size })
  const writeOptions = {
    public: acl === 'public',
    private: acl === 'private',
    resumable: metadata.size > 5e6,
    metadata: {
      contentType: metadata.contentType,
      metadata: packMetadata(metadata)
    }
  }

  await pipeline(
    ...[
      createReadStream(src),
      rateLimit && throttler(rateLimit),
      onProgress && speedo,
      onProgress &&
        progressStream({ onProgress, interval: progressInterval, speedo }),
      file.createWriteStream(writeOptions)
    ].filter(Boolean)
  )
}

export async function download (src, dest, options = {}) {
  const { onProgress, progressInterval = 1000, rateLimit } = options
  const { bucket: bucketName, file: fileName } = parse(src)

  const { size, md5Hash, atime, mtime, mode } = await stat(src)
  const md5 = Buffer.from(md5Hash, 'base64').toString('hex')

  const storage = await getStorageAPI()
  const bucket = storage.bucket(bucketName)
  const file = bucket.file(fileName)

  const hasher = hashStream()
  const speedo = createSpeedo({ total: size })

  await pipeline(
    ...[
      file.createReadStream(),
      hasher,
      rateLimit && throttler(rateLimit),
      onProgress && speedo,
      onProgress &&
        progressStream({ onProgress, interval: progressInterval, speedo }),
      createWriteStream(dest)
    ].filter(Boolean)
  )

  if (hasher.hash !== md5) {
    throw new Error(`Error downloading ${src} to ${dest}`)
  }

  if (mode) await chmod(dest, mode & 0o777)
  if (mtime && atime) await utimes(dest, atime, mtime)
}

export async function * scan (uri) {
  const { bucket: bucketName, file: prefix } = parse(uri)
  const storage = await getStorageAPI()
  const bucket = storage.bucket(bucketName)

  const files = bucket.getFilesStream({ prefix: prefix || undefined })
  for await (const { metadata } of files) {
    yield cleanMetadata({
      ...metadata,
      ...unpackMetadata(metadata.metadata)
    })
  }
}

async function getLocalMetadata (file) {
  const { mtimeMs, ctimeMs, atimeMs, size, mode } = await fsStat(file)
  const md5 = await hashFile(file)
  const contentType = mime.getType(extname(file))
  const defaults = { uid: 1000, gid: 1000, uname: 'alan', gname: 'alan' }
  return {
    ...defaults,
    mtime: Math.floor(mtimeMs),
    ctime: Math.floor(ctimeMs),
    atime: Math.floor(atimeMs),
    size,
    mode,
    md5,
    contentType
  }
}

function packMetadata (obj, key = 'gsjs') {
  return {
    [key]: Object.keys(obj)
      .sort()
      .map(k => [k, obj[k]])
      .filter(([, v]) => v != null)
      .map(([k, v]) => `${k}:${v}`)
      .join('/')
  }
}

function unpackMetadata (md, key = 'gsjs') {
  if (!md || !md[key]) return {}
  return Object.fromValues(md[key].split('/').map(x => x.split(':')))
}

function cleanMetadata (obj) {
  const integerRegex = /^-?\d+$/
  const floatRegex = /^-?\d+\.\d+$/
  const dateRegex = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z$/
  const knownTimes = new Set(['atime', 'mtime', 'ctime'])
  for (const k of Object.keys(obj)) {
    let v = obj[k]
    if (typeof v !== 'string') continue
    if (integerRegex.test(v)) {
      v = parseInt(v, 10)
    } else if (floatRegex.test(v)) {
      v = parseFloat(v)
    } else if (dateRegex.test(v)) {
      v = new Date(v)
    }
    if (knownTimes.has(k)) {
      v = new Date(v)
    }
    obj[k] = v
  }
  return obj
}

const getStorageAPI = once(async function getStorageAPI ({
  credentials = 'credentials.json'
} = {}) {
  const { Storage } = await import('@google-cloud/storage')
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials
  }

  const storage = new Storage()
  return storage
})
