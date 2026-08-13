<template>
  <div class="login-page">
    <el-card class="login-card" shadow="always">
      <div class="login-brand">
        <span class="login-mark" aria-hidden="true">M</span>
        <div class="login-titles">
          <h1 class="login-name">Marketing Copilot</h1>
          <p class="login-sub">{{ $t('auth.subtitle') }}</p>
        </div>
      </div>

      <el-form label-position="top" @submit.prevent="submit">
        <el-form-item :label="$t('auth.username')">
          <el-input
            v-model="username"
            size="large"
            autocomplete="username"
            :placeholder="$t('auth.usernamePlaceholder')"
            @keyup.enter="submit"
          >
            <template #prefix><el-icon><User /></el-icon></template>
          </el-input>
        </el-form-item>
        <el-form-item :label="$t('auth.password')">
          <el-input
            v-model="password"
            size="large"
            type="password"
            show-password
            autocomplete="current-password"
            :placeholder="$t('auth.passwordPlaceholder')"
            @keyup.enter="submit"
          >
            <template #prefix><el-icon><Lock /></el-icon></template>
          </el-input>
        </el-form-item>
        <el-button
          type="primary"
          size="large"
          class="login-button"
          :loading="loading"
          @click="submit"
        >
          {{ $t('auth.login') }}
        </el-button>
      </el-form>
      <el-select v-model="currentLocale" class="login-language" :aria-label="$t('layout.language')" size="small">
        <el-option v-for="item in supportedLocales" :key="item.value" :label="item.label" :value="item.value" />
      </el-select>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useI18n } from 'vue-i18n'
import { useAuthStore } from '@/stores/auth'
import { setDocumentLocale, supportedLocales } from '@/i18n'

const authStore = useAuthStore()
const router = useRouter()
const route = useRoute()
const { t, locale } = useI18n()
const currentLocale = computed({ get: () => locale.value, set: (value: string) => { locale.value = value; localStorage.setItem('locale', value); setDocumentLocale(value) } })

const username = ref('')
const password = ref('')
const loading = ref(false)

async function submit() {
  if (!username.value || !password.value) {
    ElMessage.warning(t('auth.required'))
    return
  }
  loading.value = true
  try {
    await authStore.signIn(username.value, password.value)
    ElMessage.success(t('auth.loginSuccess'))
    const redirect = typeof route.query.redirect === 'string' ? route.query.redirect : '/'
    router.replace(redirect)
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || t('auth.loginFailed'))
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.login-page {
  width: 100%;
  height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background:
    radial-gradient(1100px 500px at 15% -10%, rgb(46 125 115 / 18%), transparent 60%),
    radial-gradient(900px 460px at 100% 0%, rgb(246 181 106 / 14%), transparent 55%),
    var(--mc-bg);
  padding: 20px;
}
.login-card {
  width: 400px;
  max-width: 100%;
  border-radius: 18px;
  border: 1px solid var(--mc-border);
}
.login-brand {
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 26px;
}
.login-mark {
  width: 52px;
  height: 52px;
  display: inline-flex;
  flex: 0 0 52px;
  align-items: center;
  justify-content: center;
  color: #fffdf6;
  font-size: 23px;
  font-weight: 800;
  background: var(--mc-primary);
  border-radius: 15px;
  box-shadow: 0 10px 24px rgb(27 43 41 / 22%);
}
.login-titles {
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.login-name {
  margin: 0;
  font-size: 19px;
  font-weight: 750;
  color: var(--mc-text);
  letter-spacing: -0.01em;
}
.login-sub {
  margin: 0;
  font-size: 12px;
  color: var(--mc-text-secondary);
}
.login-button {
  width: 100%;
  margin-top: 6px;
  border-radius: 10px;
}
.login-language { display: block; width: 120px; margin-top: 14px; margin-right: auto; }
</style>
