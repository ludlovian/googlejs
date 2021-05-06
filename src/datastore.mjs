import once from 'pixutil/once'
import arrify from 'pixutil/arrify'
import clone from 'pixutil/clone'
import equal from 'pixutil/equal'

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
    const { kind } = this
    for (const entities of getEntities(rows, { kind, datastore })) {
      await datastore.insert(entities)
    }
  }

  async update (rows) {
    const datastore = await getDatastoreAPI()
    const { kind } = this
    for (const entities of getEntities(rows, { kind, datastore })) {
      await datastore.update(entities)
    }
  }

  async upsert (rows) {
    const datastore = await getDatastoreAPI()
    const { kind } = this
    for (const entities of getEntities(rows, { kind, datastore })) {
      await datastore.upsert(entities)
    }
  }

  async delete (rows) {
    const datastore = await getDatastoreAPI()
    for (const keys of getKeys(rows)) {
      await datastore.delete(keys)
    }
  }
}

const KEY = Symbol('rowKey')
const PREV = Symbol('prev')

export class Row {
  constructor (entity, datastore) {
    Object.assign(this, clone(entity))
    Object.defineProperties(this, {
      [KEY]: { value: entity[datastore.KEY], configurable: true },
      [PREV]: { value: clone(entity), configurable: true }
    })
  }

  get _key () {
    return this[KEY]
  }

  _changed () {
    // unwrap from class before comparing
    return !equal({ ...this }, this[PREV])
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
  let batch = []
  for (const row of arrify(arr)) {
    if (row instanceof Row && !row._changed) continue
    batch.push({
      key: row instanceof Row ? row._key : datastore.key([kind]),
      data: clone(row)
    })
    if (batch.length === group) {
      yield batch
      batch = []
    }
  }
  if (batch.length) {
    yield batch
  }
}

function * getKeys (arr, { group = 400 } = {}) {
  let batch = []
  for (const row of arrify(arr)) {
    if (!(row instanceof Row)) continue
    batch.push(row._key)
    if (batch.length === group) {
      yield batch
      batch = []
    }
  }
  if (batch.length) {
    yield batch
  }
}
