import { once } from './util.mjs'

let KEY

export class Table {
  constructor (kind) {
    this.kind = kind
  }

  async * fetch () {
    const datastore = await getDatastoreAPI()
    const query = datastore.createQuery(this.kind)
    for await (const entity of query.runStream()) {
      yield entity
    }
  }

  async fetchAll (options) {
    const entities = []
    for await (const entity of this.fetch(options)) {
      entities.push(entity)
    }
    return entities
  }

  async insert (entities) {
    const datastore = await getDatastoreAPI()
    entities = verifyEntities(entities, { kind: this.kind, datastore })
    await datastore.insert(entities)
  }

  async upsert (entities) {
    const datastore = await getDatastoreAPI()
    entities = verifyEntities(entities, { kind: this.kind, datastore })
    await datastore.upsert(entities)
  }

  async delete (entities) {
    const datastore = await getDatastoreAPI()
    entities = verifyEntities(entities, { kind: this.kind, datastore })
    await datastore.delete(entities.map(e => e[datastore.KEY]))
  }
}

export function getEntityKey (entity) {
  return entity[KEY]
}

const getDatastoreAPI = once(async function getDatastoreAPI ({
  credentials = 'credentials.json'
} = {}) {
  const { Datastore } = await import('@google-cloud/datastore')
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials
  }

  const datastore = new Datastore()
  KEY = datastore.KEY
  return datastore
})

function verifyEntities (arr, { kind, datastore }) {
  if (!Array.isArray(arr)) arr = [arr]
  for (const entity of arr) {
    if (!(datastore.KEY in entity)) {
      if ('id' in entity) {
        entity[datastore.KEY] = datastore.key([kind, entity.id])
        delete entity.id
      } else {
        entity[datastore.KEY] = datastore.key([kind])
      }
    }
  }
  return arr
}
