import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { MonitorConfig, MonitorFile } from '@/types/monitor'
import * as api from '@/api/monitor'

export const useMonitorStore = defineStore('monitor', () => {
  const configs = ref<MonitorConfig[]>([])
  const files = ref<MonitorFile[]>([])
  const loading = ref(false)
  const isMonitoring = ref(false)
  let pollTimer: ReturnType<typeof setInterval> | null = null

  async function fetchConfigs() {
    const res = await api.getMonitorConfigs()
    configs.value = res.data
  }

  async function addConfig(directoryPath: string, targetDirectories?: string[]) {
    const res = await api.addMonitorConfig(directoryPath, targetDirectories)
    await fetchConfigs()
    return res.data
  }

  async function updateConfig(configId: number, data: Partial<MonitorConfig>) {
    const res = await api.updateMonitorConfig(configId, data)
    await fetchConfigs()
    return res.data
  }

  async function removeConfig(configId: number) {
    await api.deleteMonitorConfig(configId)
    await fetchConfigs()
  }

  async function toggleConfig(configId: number, enabled: number) {
    const res = await api.toggleMonitorConfig(configId, enabled)
    await fetchConfigs()
    return res.data
  }

  async function fetchFiles(status?: string) {
    loading.value = true
    try {
      const res = await api.getMonitorFiles(status)
      files.value = res.data
    } finally {
      loading.value = false
    }
  }

  async function scan(configId: number) {
    const res = await api.scanDirectory(configId)
    await fetchFiles()
    return res.data
  }

  function startPolling(interval = 5000) {
    isMonitoring.value = true
    pollTimer = setInterval(() => {
      fetchFiles()
    }, interval)
  }

  function stopPolling() {
    isMonitoring.value = false
    if (pollTimer) {
      clearInterval(pollTimer)
      pollTimer = null
    }
  }

  return {
    configs, files, loading, isMonitoring,
    fetchConfigs, addConfig, updateConfig, removeConfig, toggleConfig,
    fetchFiles, scan, startPolling, stopPolling,
  }
})
