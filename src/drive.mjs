import { once } from './util.mjs'

const SCOPES = {
  rw: ['https://www.googleapis.com/auth/drive'],
  ro: ['https://www.googleapis.com/auth/drive.readonly']
}

export const scopes = SCOPES

export async function * list ({ folder, ...options }) {
  const drive = await getDriveAPI(options)
  const query = {
    fields: 'nextPageToken, files(id, name, mimeType, parents)'
  }

  if (folder) query.q = `'${folder}' in parents`

  let pResponse = drive.files.list(query)

  while (pResponse) {
    const response = await pResponse
    const { status, data } = response
    if (status !== 200) {
      throw Object.assign(new Error('Bad result reading folder'), { response })
    }

    // fetch the next one if there is more
    if (data.nextPageToken) {
      query.pageToken = data.nextPageToken
      pResponse = drive.files.list(query)
    } else {
      pResponse = null
    }

    for (const file of data.files) {
      yield file
    }
  }
}

const getDriveAPI = once(async function getDriveAPI ({
  credentials = 'credentials.json',
  scopes = SCOPES.ro
} = {}) {
  const driveApi = await import('@googleapis/drive')
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials
  }

  const auth = new driveApi.auth.GoogleAuth({ scopes })
  const authClient = await auth.getClient()
  return driveApi.drive({ version: 'v3', auth: authClient })
})
