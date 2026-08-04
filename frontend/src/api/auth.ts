import api from './index'

export interface LoginResponse {
  token: string
  username: string
  role: string
  expires_in: number
}

export function login(username: string, password: string) {
  return api.post<LoginResponse>('/auth/login', { username, password })
}

export function logout() {
  return api.post<{ ok: boolean }>('/auth/logout')
}

export function me() {
  return api.get<{ username: string; role: string }>('/auth/me')
}
