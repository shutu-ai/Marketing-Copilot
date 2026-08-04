import { defineStore } from 'pinia'
import { ref } from 'vue'
import * as authApi from '@/api/auth'

const TOKEN_KEY = 'mc_token'
const USER_KEY = 'mc_user'

export const useAuthStore = defineStore('auth', () => {
  const token = ref<string>(localStorage.getItem(TOKEN_KEY) || '')
  const username = ref<string>(localStorage.getItem(USER_KEY) || '')
  const role = ref<string>('admin')

  function isAuthenticated() {
    return !!token.value
  }

  async function signIn(name: string, password: string) {
    const res = await authApi.login(name, password)
    token.value = res.data.token
    username.value = res.data.username
    role.value = res.data.role
    localStorage.setItem(TOKEN_KEY, token.value)
    localStorage.setItem(USER_KEY, username.value)
    return res.data
  }

  function signOut() {
    authApi.logout().catch(() => undefined)
    token.value = ''
    username.value = ''
    role.value = ''
    localStorage.removeItem(TOKEN_KEY)
    localStorage.removeItem(USER_KEY)
  }

  return { token, username, role, isAuthenticated, signIn, signOut }
})
