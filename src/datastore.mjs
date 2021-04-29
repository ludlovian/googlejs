import { once, arrify } from './util.mjs'

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
    return entities
  }

  async insert (rows) {
    const datastore = await getDatastoreAPI()
    const entities = makeEntities(rows, { kind: this.kind, datastore })
    await datastore.insert(entities)
  }

  async update (rows) {
    const datastore = await getDatastoreAPI()
    const entities = makeEntities(rows, { kind: this.kind, datastore })
    await datastore.update(entities)
  }

  async upsert (rows) {
    const datastore = await getDatastoreAPI()
    const entities = makeEntities(rows, { kind: this.kind, datastore })
    await datastore.upsert(entities)
  }

  async delete (rows) {
    const datastore = await getDatastoreAPI()
    const keys = extractKeys(rows)
    await datastore.delete(keys)
  }
}

const KEY = Symbol('rowKey')

export class Row {
  constructor (entity, datastore) {
    const _key = entity[datastore.KEY]
    for (const k of Object.keys(entity).sort()) {
      this[k] = entity[k]
    }
    Object.defineProperty(this, KEY, { value: _key, configurable: true })
  }

  get _key () {
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

function makeEntities (arr, { kind, datastore }) {
  return arrify(arr).map(row => {
    if (row instanceof Row) return { key: row._key, data: { ...row } }
    return {
      key: row._id ? datastore.key([kind, row._id]) : datastore.key([kind]),
      data: { ...row }
    }
  })
}

function extractKeys (arr) {
  return arrify(arr)
    .filter(row => row instanceof Row)
    .map(row => row._key)
}
