import once from 'pixutil/once'
import arrify from 'pixutil/arrify'
import clone from 'pixutil/clone'
import equal from 'pixutil/equal'
import log from 'logjs'
import teme from 'teme'

import { clean } from './util.mjs'

const debug = log
  .prefix('googlejs:datastore:')
  .colour()
  .level(5)

const PREV = Symbol('prev')
const KEY = Symbol('key')

export class Table {
  constructor (kind) {
    this.kind = kind
  }

  async * fetch ({ where, order, factory, ...rest } = {}) {
    const datastore = await getDatastoreAPI(rest)
    let query = datastore.createQuery(this.kind)
    if (where && typeof where === 'object') {
      if (!Array.isArray(where)) where = Object.entries(where)
      for (const args of where) {
        query = query.filter(...args)
      }
    }
    if (Array.isArray(order)) {
      for (const args of order) {
        query = query.order(...arrify(args))
      }
    }
    for await (const entity of query.runStream()) {
      yield createRowfromEntity(entity, datastore, factory)
    }
  }

  async select (options) {
    const entities = await teme(this.fetch(options)).collect()
    debug('%d records loaded from %s', entities.length, this.kind)
    return entities
  }

  async upsert (rows) {
    const datastore = await getDatastoreAPI()
    const { kind } = this
    for (const entities of getEntities(rows, { kind, datastore })) {
      await datastore.upsert(entities)
      debug('%d records upserted to %s', entities.length, this.kind)
    }
  }

  async delete (rows) {
    const datastore = await getDatastoreAPI()
    for (const keys of getKeys(rows)) {
      await datastore.delete(keys)
      debug('%d records deleted from %s', keys.length, this.kind)
    }
  }
}

Table.getKey = o => o[KEY]
Table.getPrev = o => o[PREV]

function createRowfromEntity (entity, datastore, factory) {
  const Factory = factory || Object
  const row = new Factory()
  setPrivate(row, { key: entity[datastore.KEY], prev: clone(entity) })
  if (row.deserialize) row.deserialize(clone(entity))
  else Object.assign(row, clone(entity))
  return row
}

function * getEntities (arr, { kind, datastore, size = 400 }) {
  const batch = []
  for (const row of arrify(arr)) {
    const data = row.serialize ? row.serialize() : clean(row)
    if (row[PREV] && equal(row[PREV], data)) continue
    if (!row[KEY]) setPrivate(row, { key: datastore.key([kind]) })
    const entity = { key: row[KEY], data }
    setPrivate(row, { prev: clone(data) })
    if (batch.push(entity) >= size) yield batch.splice(0)
  }
  if (batch.length) yield batch
}

function * getKeys (arr, { size = 400 } = {}) {
  const batch = []
  for (const row of arrify(arr)) {
    if (!row[KEY]) continue
    if (batch.push(row[KEY]) >= size) yield batch.splice(0)
    setPrivate(row, { key: undefined, prev: undefined })
  }
  if (batch.length) yield batch
}

function setPrivate (row, data) {
  const defs = {}
  if ('prev' in data) {
    defs[PREV] = { value: data.prev, configurable: true }
  }
  if ('key' in data) {
    defs[KEY] = { value: data.key, configurable: true }
  }
  return Object.defineProperties(row, defs)
}

const getDatastoreAPI = once(async function getDatastoreAPI ({
  credentials = 'credentials.json'
} = {}) {
  const { Datastore } = await import('@google-cloud/datastore')
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials
  }

  const datastore = new Datastore()
  return datastore
})
