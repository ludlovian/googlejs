import { Table } from './datastore.mjs'

export class IndexedTable extends Table {
  constructor (name) {
    super(name)
    this.name = name
    this.ix = {}
  }

  async load () {
    const rows = await super.select({ factory: this.factory })
    if (this.order) rows.sort(this.order)
    for (const k in this.ix) this.ix[k].rebuild(rows)
    this._changed = new Set()
    this._deleted = new Set()
  }

  async save () {
    const changed = [...this._changed]
    const deleted = [...this._deleted]
    if (changed.length) {
      await super.upsert(changed)
    }

    if (deleted.length) {
      await super.delete(deleted)
    }
  }

  set (data) {
    const row = this.ix.main.get(data)
    if (row) {
      Object.assign(row, data)
      this._changed.add(row)
      return row
    } else {
      const row = { ...data }
      for (const k in this.ix) this.ix[k].add(row)
      this._changed.add(row)
      return row
    }
  }

  delete (data) {
    const row = this.ix.main.get(data)
    if (!row) return
    for (const k in this.ix) this.ix[k].delete(row)
    this._deleted.add(row)
    return row
  }

  values () {
    return this.ix.main ? this.ix.main.map.values() : []
  }
}

export class Index {
  constructor (fn) {
    this.fn = fn
    this.map = new Map()
  }

  rebuild (rows) {
    this.map.clear()
    for (const row of rows) {
      this.add(row)
    }
  }

  add (row) {
    const key = this.fn(row)
    const entry = this.map.get(key)
    if (entry) {
      entry.add(row)
    } else {
      this.map.set(key, new Set([row]))
    }
  }

  delete (row) {
    const key = this.fn(row)
    const entry = this.map.get(key)
    if (!entry) return
    entry.delete(row)
    if (!entry.size) this.map.delete(key)
  }

  get (data) {
    const key = this.fn(data)
    return this.map.get(key) || []
  }
}

export class UniqueIndex extends Index {
  add (row) {
    const key = this.fn(row)
    this.map.set(key, row)
  }

  delete (row) {
    const key = this.fn(row)
    this.map.delete(key)
  }

  get (data) {
    const key = this.fn(data)
    return this.map.get(key)
  }
}
