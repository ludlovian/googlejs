const epochStartInSerial = 25569
const msInDay = 24 * 60 * 60 * 1000
const msInMinute = 60 * 1000

export default class SerialDate {
  static fromSerial (n) {
    return new SerialDate(n)
  }

  static fromUTCms (ms) {
    return SerialDate.fromSerial(ms / msInDay + epochStartInSerial)
  }

  static fromUTCDate (d) {
    return SerialDate.fromUTCms(d.getTime())
  }

  static fromParts (parts) {
    parts = [...parts, 0, 0, 0, 0, 0, 0, 0].slice(0, 7)
    parts[1]--
    return SerialDate.fromUTCms(Date.UTC(...parts))
  }

  static fromLocalDate (d) {
    return SerialDate.fromUTCms(
      d.getTime() - d.getTimezoneOffset() * msInMinute
    )
  }

  constructor (serial) {
    this.serial = serial
    Object.freeze(this)
  }

  utcMs () {
    return Math.round((this.serial - epochStartInSerial) * msInDay)
  }

  utcDate () {
    return new Date(this.utcMs())
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
