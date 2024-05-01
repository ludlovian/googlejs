import once from 'pixutil/once'

import SerialDate from './serial-date.mjs'

const SCOPES = {
  rw: ['https://www.googleapis.com/auth/spreadsheets'],
  ro: ['https://www.googleapis.com/auth/spreadsheets.readonly']
}

export const scopes = SCOPES
export const toDate = s => SerialDate.fromSerial(s).localDate()
export const toSerial = d => SerialDate.fromLocalDate(d).serial
export { SerialDate }

export async function getRange ({ sheet, range, ...options }) {
  const sheets = await getSheetAPI(options)

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: sheet,
    range,
    valueRenderOption: 'UNFORMATTED_VALUE',
    dateTimeRenderOption: 'SERIAL_NUMBER',
    majorDimension: 'ROWS'
  })

  if (response.status !== 200) {
    throw Object.assign(new Error('Failed to read sheet'), { response })
  }
  return response.data.values
}

export async function updateRange ({ sheet, range, data, ...options }) {
  const sheets = await getSheetAPI(options)

  data = data.map(row =>
    row.map(val => (val instanceof Date ? toSerial(val) : val))
  )

  const response = await sheets.spreadsheets.values.update({
    spreadsheetId: sheet,
    range,
    valueInputOption: 'RAW',
    requestBody: {
      range,
      majorDimension: 'ROWS',
      values: data
    }
  })

  if (response.status !== 200) {
    throw Object.assign(new Error('Failed to update sheet'), { response })
  }
}

export function getColumn (col) {
  // Convert a column number (1, 2, ..., 26, 27, ...)
  // into a column name (A, B, ..., Z, AA, ...)
  //
  // inspired by bb26
  //
  const toChar = n => String.fromCharCode(64 + n)
  let s = ''
  for (let n = col; n > 0; n = Math.floor(--n / 26)) {
    s = toChar(n % 26 || 26) + s
  }
  return s
}

const getSheetAPI = once(async function getSheetAPI ({
  credentials = 'credentials.json',
  scopes = SCOPES.ro
} = {}) {
  const sheetsApi = await import('@googleapis/sheets')
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials
  }

  const auth = new sheetsApi.auth.GoogleAuth({ scopes })
  const authClient = await auth.getClient()
  return sheetsApi.sheets({ version: 'v4', auth: authClient })
})
