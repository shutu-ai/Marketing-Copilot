import api from './index'

export interface DirectoryEntry {
  name: string
  path: string
}

export interface DirectoryListing {
  path: string
  parent: string | null
  entries: DirectoryEntry[]
}

export function listDirectories(path = '') {
  return api.get<DirectoryListing>('/filesystem/directories', { params: { path } })
}
