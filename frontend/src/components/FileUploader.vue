<template>
  <el-upload
    drag
    :auto-upload="false"
    :accept="accept"
    :on-change="handleChange"
    :limit="1"
  >
    <el-icon class="el-icon--upload" :size="40"><UploadFilled /></el-icon>
    <div class="el-upload__text">
      {{ $t('whitelist.uploadArea') }}
    </div>
    <template #tip>
      <div class="el-upload__tip">
        {{ $t('whitelist.uploadHint') }}
      </div>
    </template>
  </el-upload>
</template>

<script setup lang="ts">
const emit = defineEmits<{ file: [file: File] }>()

const accept = '.csv'

function handleChange(uploadFile: any) {
  if (uploadFile.raw) {
    emit('file', uploadFile.raw)
  }
}
</script>
