import axios from 'axios'

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE || '/api',
  timeout: 30000,
})

api.interceptors.response.use(
  (response) => response,
  (error) => {
    const msg = error.response?.data?.detail || error.message || '请求失败'
    console.error('[API Error]', msg)
    return Promise.reject(error)
  }
)

export default api
