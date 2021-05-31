import once from 'pixutil/once'
import arrify from 'pixutil/arrify'
import clone from 'pixutil/clone'
import teme from 'teme'
import equal from 'pixutil/equal'
import log from 'logjs'

import { clean } from './util.mjs'

const debug = log
  .prefix('googlejs:datastore:')
  .colour()
  .level(5)

export class Table {
  constructor (kind) {
    this.kind = kind
  }

  async * fetch ({ where, order, factory, ...rest } = {}) {
    if (factory && !(factory.prototype instanceof Row)) {
      throw new Error('Factory for new rows must subclass Row')
    }
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
      yield Row.fromEntity(entity, datastore, factory)
    }
  }

  async select (options) {
    const entities = await teme(this.fetch(options)).collect()
    debug('%d records loaded from %s', entities.length, this.kind)
    return entities
  }

  async insert (rows) {
    const datastore = await getDatastoreAPI()
    const { kind } = this
    for (const entities of getEntities(rows, { kind, datastore })) {
      await datastore.insert(entities)
      debug('%d records inserted to %s', entities.length, this.kind)
    }
  }

  async update (rows) {
    const datastore = await getDatastoreAPI()
    const { kind } = this
    for (const entities of getEntities(rows, { kind, datastore })) {
      await datastore.update(entities)
      debug('%d records updated to %s', entities.length, this.kind)
    }
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

const KEY = Symbol('rowKey')
const PREV = Symbol('prev')

export class Row {
  static fromEntity (entity, datastore, Factory) {
    const data = clone(entity)
    const row = new (Factory || Row)(data)
    Object.defineProperties(row, {
      [KEY]: { value: entity[datastore.KEY], configurable: true },
      [PREV]: { value: clone(data), configurable: true }
    })
    return row
  }

  constructor (data) {
    Object.assign(this, clean(data))
  }

  asJSON () {
    return { ...this }
  }

  _changed () {
    return !equal(clean(this.asJSON()), this[PREV])
  }

  _key () {
    return this[KEY]
  }
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

function getEntities (arr, { kind, datastore, size = 400 }) {
  return teme(arrify(arr))
    .map(row => (row instanceof Row ? row : new Row(row)))
    .filter(row => row._changed())
    .map(row => ({
      key: row._key || datastore.key([kind]),
      data: clean(row.asJSON())
    }))
    .batch(size)
    .map(group => group.collect())
}

function getKeys (arr, { size = 400 } = {}) {
  return teme(arrify(arr))
    .filter(row => row instanceof Row && row._key)
    .map(row => row._key)
    .batch(size)
    .map(group => group.collect())
}
