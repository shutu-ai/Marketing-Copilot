<template>
  <div class="sms-preview">
    <el-card shadow="never">
      <template #header>
        <span>{{ $t('smsContent.preview') }}</span>
      </template>
      <div v-if="rendered" class="preview-content">
        <div class="sms-bubble">{{ rendered }}</div>
        <div class="preview-meta">
          <el-tag size="small">{{ $t('smsContent.characterCount') }}: {{ charCount }}</el-tag>
          <el-tag size="small" type="success">{{ $t('smsContent.smsSegments') }}: {{ segments }}</el-tag>
        </div>
      </div>
      <el-empty v-else :description="$t('smsContent.preview')" />
    </el-card>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  rendered: string
  charCount: number
  segments: number
}>()
</script>

<style scoped>
.sms-bubble {
  background: var(--mc-primary-soft);
  border: 1px solid #ead8a4;
  border-radius: var(--mc-radius-lg) var(--mc-radius-lg) 4px var(--mc-radius-lg);
  padding: 14px 16px;
  color: var(--mc-text);
  font-size: 14px;
  line-height: 1.6;
  word-break: break-all;
  margin-bottom: 12px;
}
.preview-meta {
  display: flex;
  gap: 8px;
}
</style>
