export function once (fn) {
  function f (...args) {
    if (f.called) return f.value
    f.value = fn(...args)
    f.called = true
    return f.value
  }

  if (fn.name) {
    Object.defineProperty(f, 'name', { value: fn.name, configurable: true })
  }

  return f
}

export function jsDateToSerialDate (dt) {
  const ms = dt.getTime()
  const localMs = ms - dt.getTimezoneOffset() * 60 * 1000
  const localDays = localMs / (1000 * 24 * 60 * 60)
  const epochStart = 25569
  return epochStart + localDays
}
