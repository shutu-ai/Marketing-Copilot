import api from './index'

export interface UserRow {
  id: number
  username: string
  display_name: string
  role: 'admin' | 'marketing' | 'analyst'
  enabled: number
  last_login_at: string | null
  created_at: string
  updated_at: string
}

export function getUsers() {
  return api.get<UserRow[]>('/users')
}

export function createUser(data: {
  username: string
  password: string
  role: string
  display_name: string
  enabled: boolean
}) {
  return api.post<UserRow>('/users', data)
}

export function updateUser(
  id: number,
  data: { display_name?: string; role?: string; enabled?: boolean; password?: string }
) {
  return api.put<UserRow>(`/users/${id}`, data)
}

export function deleteUser(id: number) {
  return api.delete<{ ok: boolean }>(`/users/${id}`)
}

export function changeMyPassword(oldPassword: string, newPassword: string) {
  return api.put<{ ok: boolean }>('/users/me/password', {
    old_password: oldPassword,
    new_password: newPassword,
  })
}
