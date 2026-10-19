<template>
  <el-tag :type="type" :size="size" effect="plain">
    <slot>{{ label }}</slot>
  </el-tag>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import i18n from '@/i18n'

const props = defineProps<{
  status: string
  size?: 'small' | 'default' | 'large'
}>()

const typeMap: Record<string, string> = {
  pending: 'warning',
  processing: 'primary',
  completed: 'success',
  failed: 'danger',
  partial: 'warning',
  unknown: 'danger',
  active: 'success',
  inactive: 'info',
  enabled: 'success',
  disabled: 'info',
  not_started: 'info',
  in_progress: 'warning',
  ended: 'success',
  queued: 'warning',
  sending: 'primary',
  submitted: 'primary',
  accepted: 'success',
  delivered: 'success',
  retrying: 'warning',
  expired: 'info',
  cancelled: 'info',
  socket_written: 'success',
  submit_sm_resp_success: 'success',
  submit_sm_resp_error: 'danger',
  write_failed: 'danger',
}

const type = computed(() => typeMap[props.status] || 'info')
const vnextLabels: Record<string, string> = {
  socket_written: 'smsTasks.socketWritten',
  submit_sm_resp_success: 'smsTasks.smscAccepted',
  submit_sm_resp_error: 'smsTasks.smscRejected',
  write_failed: 'smsTasks.writeFailed',
  unknown: 'smsTasks.unknown',
  cancelled: 'smsTasks.cancelled',
  expired: 'smsTasks.expired',
}
const label = computed(() => {
  const key = vnextLabels[props.status]
  return key ? i18n.global.t(key) : props.status
})
</script>
