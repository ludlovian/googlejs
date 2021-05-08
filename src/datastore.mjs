import once from 'pixutil/once'
import arrify from 'pixutil/arrify'
import clone from 'pixutil/clone'
import batch from 'teme/batch'
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
    const Factory = factory || Row
    for await (const entity of query.runStream()) {
      yield new Factory(entity, datastore)
    }
  }

  async select (options) {
    const entities = []
    for await (const entity of this.fetch(options)) {
      entities.push(entity)
    }
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
  constructor (entity, datastore) {
    Object.assign(this, clone(clean(entity)))
    Object.defineProperties(this, {
      [KEY]: { value: entity[datastore.KEY], configurable: true },
      [PREV]: { value: clone(entity), configurable: true }
    })
  }

  get _key () {
    return this[KEY]
  }

  _changed () {
    // unwrap from class and clean before comparing
    return !equal(clean(this), this[PREV])
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

function * getEntities (arr, { kind, datastore, group = 400 }) {
  const entities = arrify(arr)
    .filter(row => !(row instanceof Row) || row._changed())
    .map(row => ({
      key: row._key || datastore.key([kind]),
      data: clone(row)
    }))

  yield * batch(group)(entities)
}

function * getKeys (arr, { group = 400 } = {}) {
  const keys = arrify(arr)
    .filter(row => row instanceof Row)
    .map(row => row._key)

  yield * batch(group)(keys)
}
