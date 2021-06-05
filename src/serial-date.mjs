const epochStartInSerial = 25569
const msInDay = 24 * 60 * 60 * 1000

export default class SerialDate {
  static fromSerial (n) {
    return new SerialDate(n)
  }

  static fromUTCDate (d) {
    return new SerialDate(d.getTime() / msInDay + epochStartInSerial)
  }

  static fromParts (parts) {
    parts = [...parts, 0, 0, 0, 0, 0, 0, 0].slice(0, 7)
    parts[1]--
    return SerialDate.fromUTCDate(new Date(Date.UTC(...parts)))
  }

  static fromLocalDate (d) {
    return SerialDate.fromParts([
      d.getFullYear(),
      d.getMonth() + 1,
      d.getDate(),
      d.getHours(),
      d.getMinutes(),
      d.getSeconds(),
      d.getMilliseconds()
    ])
  }

  constructor (serial) {
    this.serial = serial
    Object.freeze(this)
  }

  utcDate () {
    return new Date((this.serial - epochStartInSerial) * msInDay)
  }

  parts () {
    const d = this.utcDate()
    return [
      d.getUTCFullYear(),
      d.getUTCMonth() + 1,
      d.getUTCDate(),
      d.getUTCHours(),
      d.getUTCMinutes(),
      d.getUTCSeconds(),
      d.getUTCMilliseconds()
    ]
  }

  localDate () {
    const parts = this.parts()
    parts[1]--
    return new Date(...parts)
  }
}
