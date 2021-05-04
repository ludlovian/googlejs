export function jsDateToSerialDate (dt) {
  const ms = dt.getTime()
  const localMs = ms - dt.getTimezoneOffset() * 60 * 1000
  const localDays = localMs / (1000 * 24 * 60 * 60)
  const epochStart = 25569
  return epochStart + localDays
}
