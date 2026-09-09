import { defineStore } from 'pinia'
import { ref } from 'vue'
import * as authApi from '@/api/auth'

const TOKEN_KEY = 'mc_token'
const USER_KEY = 'mc_user'
const ROLE_KEY = 'mc_role'
function normalizeRole(value: string) {
  return ({ operator: 'marketing', viewer: 'analyst' } as Record<string, string>)[value] || value
}

export const useAuthStore = defineStore('auth', () => {
  const token = ref<string>(localStorage.getItem(TOKEN_KEY) || '')
  const username = ref<string>(localStorage.getItem(USER_KEY) || '')
  const role = ref<string>(normalizeRole(localStorage.getItem(ROLE_KEY) || ''))

  if (token.value && !role.value) {
    void authApi.me().then((res) => {
      role.value = normalizeRole(res.data.role)
      username.value = res.data.username
      localStorage.setItem(ROLE_KEY, role.value)
      localStorage.setItem(USER_KEY, username.value)
    }).catch(() => undefined)
  }

  function isAuthenticated() {
    return !!token.value
  }

  async function signIn(name: string, password: string) {
    const res = await authApi.login(name, password)
    token.value = res.data.token
    username.value = res.data.username
    role.value = normalizeRole(res.data.role)
    localStorage.setItem(TOKEN_KEY, token.value)
    localStorage.setItem(USER_KEY, username.value)
    localStorage.setItem(ROLE_KEY, role.value)
    return res.data
  }

  function signOut() {
    authApi.logout().catch(() => undefined)
    token.value = ''
    username.value = ''
    role.value = ''
    localStorage.removeItem(TOKEN_KEY)
    localStorage.removeItem(USER_KEY)
    localStorage.removeItem(ROLE_KEY)
  }

  return { token, username, role, isAuthenticated, signIn, signOut }
})
