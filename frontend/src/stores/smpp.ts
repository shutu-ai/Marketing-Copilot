import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { SmppConfig, SmppConfigForm } from '@/types/smpp'
import * as api from '@/api/smpp'

export const useSmppStore = defineStore('smpp', () => {
  const configs = ref<SmppConfig[]>([])
  const loading = ref(false)

  async function fetchConfigs() {
    loading.value = true
    try {
      const res = await api.getSmppConfigs()
      configs.value = res.data
    } finally {
      loading.value = false
    }
  }

  async function addConfig(data: SmppConfigForm) {
    const res = await api.createSmppConfig(data)
    await fetchConfigs()
    return res.data
  }

  async function editConfig(id: number, data: Partial<SmppConfigForm>) {
    const res = await api.updateSmppConfig(id, data)
    await fetchConfigs()
    return res.data
  }

  async function removeConfig(id: number) {
    await api.deleteSmppConfig(id)
    await fetchConfigs()
  }

  async function testConnection(data: { config_id?: number; host: string; port: number; system_id: string; password: string; bind_type: string }) {
    const res = await api.testSmppConnection(data)
    return res.data
  }

  return {
    configs, loading,
    fetchConfigs, addConfig, editConfig, removeConfig, testConnection,
  }
})
