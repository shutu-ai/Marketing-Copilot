<template><div class="polling-status" :class="`polling-status--${state}`" role="status"><span class="polling-status__dot" aria-hidden="true"/><span>{{ stateLabel }}</span><span v-if="updatedAt" class="polling-status__time">{{ timeLabel }}</span></div></template>
<script setup lang="ts">
import i18n from '@/i18n'
import { computed } from 'vue'
const props=defineProps<{state:'live'|'stale'|'offline';updatedAt?:Date|null}>()
const stateLabel=computed(()=>({live:i18n.global.t('legacy.x2843e2f6'),stale:i18n.global.t('legacy.x3f01b403'),offline:i18n.global.t('legacy.x96a72862')}[props.state]))
const timeLabel=computed(()=>props.updatedAt?i18n.global.t('dynamic.pollingUpdatedAt',{time:props.updatedAt.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'})}):'')
</script>
<style scoped>.polling-status{display:inline-flex;align-items:center;gap:6px;color:var(--mc-text-secondary);font-size:12px;white-space:nowrap}.polling-status__dot{width:7px;height:7px;border-radius:50%;background:var(--mc-success);box-shadow:0 0 0 3px rgb(22 163 74 / 10%)}.polling-status__time{color:var(--mc-text-tertiary)}.polling-status--stale .polling-status__dot{background:var(--mc-warning)}.polling-status--offline .polling-status__dot{background:var(--mc-danger)}</style>
