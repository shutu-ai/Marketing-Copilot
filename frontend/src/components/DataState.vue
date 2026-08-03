<template>
  <div v-if="loading && !hasData" class="data-state" role="status"><el-icon class="is-loading" :size="24"><Loading /></el-icon><span>{{ loadingText }}</span></div>
  <div v-else-if="error && !hasData" class="data-state data-state--error" role="alert"><el-icon :size="26"><WarningFilled /></el-icon><strong>{{ errorTitle }}</strong><span>{{ error }}</span><el-button type="primary" plain @click="$emit('retry')">{{ retryText }}</el-button></div>
  <div v-else-if="empty" class="data-state"><el-icon :size="28"><FolderOpened /></el-icon><strong>{{ emptyTitle }}</strong><span v-if="emptyDescription">{{ emptyDescription }}</span><slot name="empty-action" /></div>
  <slot v-else />
</template>
<script setup lang="ts">
defineProps({loading:Boolean,hasData:Boolean,empty:Boolean,error:{type:String,default:''},loadingText:{type:String,default:'正在加载…'},errorTitle:{type:String,default:'数据加载失败'},retryText:{type:String,default:'重新加载'},emptyTitle:{type:String,default:'暂无数据'},emptyDescription:{type:String,default:''}})
defineEmits<{ retry: [] }>()
</script>
<style scoped>.data-state{min-height:220px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:10px;padding:32px;text-align:center;color:var(--mc-text-secondary)}.data-state strong{color:var(--mc-text);font-size:15px}.data-state--error>.el-icon{color:var(--mc-danger)}</style>
