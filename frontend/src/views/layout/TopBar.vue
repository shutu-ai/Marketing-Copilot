<template>
  <div class="top-bar">
    <div class="top-bar-left">
      <el-button class="nav-toggle" text :aria-label="$t('layout.toggleNavigation')" @click="emit('toggle')">
        <el-icon><Fold /></el-icon>
      </el-button>
      <el-breadcrumb separator="/">
        <el-breadcrumb-item :to="{ path: '/' }">{{ $t('layout.home') }}</el-breadcrumb-item>
        <el-breadcrumb-item v-if="title">{{ title }}</el-breadcrumb-item>
      </el-breadcrumb>
    </div>
    <div class="top-bar-right">
      <el-dropdown trigger="click" @command="onUserCommand">
        <span class="user-chip">
          <el-avatar :size="28" class="user-avatar">{{ userInitial }}</el-avatar>
          <span class="user-name">{{ username || $t('layout.guest') }}</span>
          <el-icon class="user-caret"><ArrowDown /></el-icon>
        </span>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item disabled class="user-info-item">
              <span class="user-info-name">{{ username }}</span>
              <el-tag size="small" :type="role === 'admin' ? 'danger' : 'primary'">
                {{ $t(`auth.role_${role}`) }}
              </el-tag>
            </el-dropdown-item>
            <el-dropdown-item divided command="password">
              <el-icon><Key /></el-icon>
              {{ $t('layout.changePassword') }}
            </el-dropdown-item>
            <el-dropdown-item divided command="logout">
              <el-icon><SwitchButton /></el-icon>
              {{ $t('layout.logout') }}
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
      <el-select
        :aria-label="$t('layout.language')"
        :model-value="currentLocale"
        @update:model-value="switchLocale"
        size="small"
        style="width: 120px"
      >
        <el-option label="中文" value="zh-CN" />
        <el-option label="English" value="en" />
      </el-select>
    </div>

    <el-dialog
      v-model="passwordDialogVisible"
      :title="$t('layout.changePassword')"
      width="420px"
      destroy-on-close
      append-to-body
    >
      <el-form :model="passwordForm" label-width="100px" size="small">
        <el-form-item :label="$t('layout.oldPassword')">
          <el-input v-model="passwordForm.old" type="password" show-password />
        </el-form-item>
        <el-form-item :label="$t('layout.newPassword')">
          <el-input v-model="passwordForm.newPwd" type="password" show-password />
        </el-form-item>
        <el-form-item :label="$t('layout.confirmPassword')">
          <el-input v-model="passwordForm.confirm" type="password" show-password @keyup.enter="submitPassword" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button size="small" @click="passwordDialogVisible = false">{{ $t('common.cancel') }}</el-button>
        <el-button size="small" type="primary" :loading="passwordSaving" @click="submitPassword">
          {{ $t('common.confirm') }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'

import { useElementPlusLocale } from '@/utils/locale'
import { useAuthStore } from '@/stores/auth'
import { changeMyPassword } from '@/api/users'

const props = defineProps<{ title: string }>()
const emit = defineEmits<{ toggle: [] }>()

const { locale, t } = useI18n()
const router = useRouter()
const authStore = useAuthStore()
const currentLocale = computed(() => locale.value)
const username = computed(() => authStore.username)
const role = computed(() => authStore.role || 'admin')
const userInitial = computed(() => (authStore.username || 'M').slice(0, 1).toUpperCase())
const passwordDialogVisible = ref(false)
const passwordSaving = ref(false)
const passwordForm = reactive({ old: '', newPwd: '', confirm: '' })

function switchLocale(val: string) {
  locale.value = val
  localStorage.setItem('locale', val)
  useElementPlusLocale(val)
}

function onUserCommand(command: string) {
  if (command === 'logout') {
    authStore.signOut()
    router.push('/login')
  } else if (command === 'password') {
    Object.assign(passwordForm, { old: '', newPwd: '', confirm: '' })
    passwordDialogVisible.value = true
  }
}

async function submitPassword() {
  if (!passwordForm.old || !passwordForm.newPwd) {
    ElMessage.warning(t('layout.passwordRequired'))
    return
  }
  if (passwordForm.newPwd !== passwordForm.confirm) {
    ElMessage.warning(t('layout.passwordMismatch'))
    return
  }
  passwordSaving.value = true
  try {
    await changeMyPassword(passwordForm.old, passwordForm.newPwd)
    ElMessage.success(t('layout.passwordChanged'))
    passwordDialogVisible.value = false
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || t('layout.passwordChangeFailed'))
  } finally {
    passwordSaving.value = false
  }
}
</script>

<style scoped>
.top-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 64px;
  padding: 0 28px;
  background: rgb(255 255 255 / 94%);
  border-bottom: 1px solid var(--mc-border);
  box-shadow: 0 1px 0 rgb(35 64 60 / 3%);
  backdrop-filter: blur(12px);
}
.top-bar-left {
  display: flex;
  align-items: center;
  gap: 18px;
}
.top-bar-right {
  display: flex;
  align-items: center;
  gap: 12px;
}
.user-chip {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 4px 10px 4px 4px;
  border-radius: 999px;
  cursor: pointer;
  transition: background 0.2s ease;
}
.user-chip:hover {
  background: var(--mc-surface-muted);
}
.user-avatar {
  color: #fff;
  font-size: 13px;
  font-weight: 700;
  background: var(--mc-primary);
}
.user-name {
  color: var(--mc-text);
  font-size: 13px;
  font-weight: 600;
}
.user-caret {
  color: var(--mc-text-secondary);
  font-size: 12px;
}
.user-info-item {
  display: flex;
  align-items: center;
  gap: 8px;
}
.user-info-name {
  font-weight: 650;
}
.nav-toggle {
  width: 36px;
  min-height: 36px;
  padding: 0;
  color: var(--mc-text-secondary);
  font-size: 19px;
  background: var(--mc-surface-muted);
  border-radius: 10px;
}
:deep(.el-breadcrumb__inner) {
  color: var(--mc-text-secondary);
  font-weight: 550;
}
:deep(.el-breadcrumb__item:last-child .el-breadcrumb__inner) {
  color: var(--mc-text);
  font-weight: 700;
}
@media (max-width: 767px) {
  .top-bar { padding: 0 14px; }
  :deep(.el-breadcrumb__item:first-child) { display: none; }
}
</style>
