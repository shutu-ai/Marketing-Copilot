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
  background: #ecf5ff;
  border: 1px solid #d9ecff;
  border-radius: 12px;
  padding: 12px 16px;
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
