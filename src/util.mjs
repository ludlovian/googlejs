export function toSerial (dt) {
  const ms = dt.getTime()
  const localMs = ms - dt.getTimezoneOffset() * 60 * 1000
  const localDays = localMs / (1000 * 24 * 60 * 60)
  const epochStart = 25569
  return epochStart + localDays
}

export function toDate (serial) {
  const epochStart = 25569
  const ms = (serial - epochStart) * 24 * 60 * 60 * 1000
  const tryDate = new Date(ms)
  const offset = tryDate.getTimezoneOffset() * 60 * 1000
  return new Date(ms + offset)
}
