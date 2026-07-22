import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { UploadHistory, PreviewResult } from '@/types/whitelist'
import * as api from '@/api/whitelist'

export const useWhitelistStore = defineStore('whitelist', () => {
  const history = ref<UploadHistory[]>([])
  const preview = ref<PreviewResult | null>(null)
  const loading = ref(false)

  async function fetchHistory() {
    const res = await api.getUploadHistory()
    history.value = res.data
  }

  async function uploadPreview(file: File) {
    loading.value = true
    try {
      const res = await api.previewFile(file)
      preview.value = res.data
      return res.data
    } finally {
      loading.value = false
    }
  }

  async function importWhitelist(filePath: string, importMode: string = 'append') {
    const res = await api.importFile(filePath, importMode)
    await fetchHistory()
    return res.data
  }

  return {
    history, preview, loading,
    fetchHistory, uploadPreview, importWhitelist,
  }
})
