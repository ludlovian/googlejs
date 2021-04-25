import { once, jsDateToSerialDate } from './util.mjs'

const SCOPES = {
  rw: ['https://www.googleapis.com/auth/spreadsheets'],
  ro: ['https://www.googleapis.com/auth/spreadsheets.readonly']
}

export const scopes = SCOPES

export async function getRange ({ sheet, range, ...options }) {
  const sheets = await getSheetAPI(options)

  const query = {
    spreadsheetId: sheet,
    range,
    valueRenderOption: 'UNFORMATTED_VALUE'
  }

  const response = await sheets.spreadsheets.values.get(query)

  if (response.status !== 200) {
    throw Object.assign(Error('Failed to read sheet'), { response })
  }
  return response.data.values
}

export async function updateRange ({ sheet, range, data, ...options }) {
  const sheets = await getSheetAPI(options)

  data = data.map(row =>
    row.map(val => (val instanceof Date ? jsDateToSerialDate(val) : val))
  )

  const query = {
    spreadsheetId: sheet,
    range,
    valueInputOption: 'RAW',
    resource: {
      range,
      majorDimension: 'ROWS',
      values: data
    }
  }
  const response = await sheets.spreadsheets.values.update(query)

  if (response.status !== 200) {
    throw Object.assign(Error('Failed to update sheet'), { response })
  }
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