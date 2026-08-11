<template>
  <el-tag :type="type" :size="size" effect="plain">
    <slot>{{ label }}</slot>
  </el-tag>
</template>

<script setup lang="ts">
import { computed } from 'vue'

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
}

const type = computed(() => typeMap[props.status] || 'info')
const label = computed(() => props.status)
</script>
