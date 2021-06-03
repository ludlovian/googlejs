const epochStartInSerial = 25569
const msInDay = 24 * 60 * 60 * 1000

export default class SerialDate {
  constructor (serial) {
    this.serial = serial
    Object.freeze(this)
  }

  utcDate () {
    return new Date((this.serial - epochStartInSerial) * msInDay)
  }

  localDate () {
    const d = this.utcDate()
    return new Date(
      d.getUTCFullYear(),
      d.getUTCMonth(),
      d.getUTCDate(),
      d.getUTCHours(),
      d.getUTCMinutes(),
      d.getUTCSeconds(),
      d.getUTCMilliseconds()
    )
  }

  static fromSerial (n) {
    return new SerialDate(n)
  }

  static fromUTCDate (d) {
    return new SerialDate(d.getTime() / msInDay + epochStartInSerial)
  }

  static fromLocalDate (d) {
    return SerialDate.fromUTCDate(
      new Date(
        Date.UTC(
          d.getFullYear(),
          d.getMonth(),
          d.getDate(),
          d.getHours(),
          d.getMinutes(),
          d.getSeconds(),
          d.getMilliseconds()
        )
      )
    )
  }
}
