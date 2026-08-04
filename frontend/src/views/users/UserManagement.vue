<template>
  <div class="user-management">
    <div v-if="!embedded" class="page-header">
      <div>
        <h2>{{ $t('users.title') }}</h2>
        <p>{{ $t('users.description') }}</p>
      </div>
      <el-button type="primary" @click="openCreate">
        <el-icon><Plus /></el-icon>
        {{ $t('users.addUser') }}
      </el-button>
    </div>

    <el-card shadow="never">
      <el-table v-loading="loading" :data="users" stripe size="small">
        <el-table-column prop="username" :label="$t('users.username')" min-width="140" />
        <el-table-column prop="display_name" :label="$t('users.displayName')" min-width="140">
          <template #default="{ row }">
            <span>{{ row.display_name || '-' }}</span>
            <el-tag v-if="row.username === currentUsername" size="small" type="success" class="self-tag">
              {{ $t('users.self') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="$t('users.role')" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="roleType(row.role)">{{ roleLabel(row.role) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="$t('common.status')" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.enabled ? 'success' : 'info'">
              {{ row.enabled ? $t('common.enabled') : $t('common.disabled') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="last_login_at" :label="$t('users.lastLogin')" width="170">
          <template #default="{ row }">{{ row.last_login_at || '-' }}</template>
        </el-table-column>
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="170" />
        <el-table-column :label="$t('common.action')" width="170" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" size="small" @click="openEdit(row)">{{ $t('common.edit') }}</el-button>
            <el-button
              link
              type="danger"
              size="small"
              :disabled="row.username === currentUsername"
              @click="confirmDelete(row)"
            >
              {{ $t('common.delete') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog
      v-model="dialogVisible"
      :title="editing ? $t('users.editUser') : $t('users.addUser')"
      width="440px"
      destroy-on-close
    >
      <el-form :model="form" label-width="110px" size="small">
        <el-form-item :label="$t('users.username')">
          <el-input v-model="form.username" :disabled="!!editing" :placeholder="$t('users.usernamePlaceholder')" />
        </el-form-item>
        <el-form-item :label="$t('users.displayName')">
          <el-input v-model="form.display_name" :placeholder="$t('users.displayNamePlaceholder')" />
        </el-form-item>
        <el-form-item :label="$t('users.role')">
          <el-select v-model="form.role" style="width: 100%">
            <el-option v-for="r in roles" :key="r" :label="roleLabel(r)" :value="r" />
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('common.status')">
          <el-switch v-model="form.enabled" active-value="1" inactive-value="0" />
        </el-form-item>
        <el-form-item :label="editing ? $t('users.resetPassword') : $t('users.password')">
          <el-input
            v-model="form.password"
            type="password"
            show-password
            :placeholder="editing ? $t('users.passwordPlaceholderEdit') : $t('users.passwordPlaceholder')"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button size="small" @click="dialogVisible = false">{{ $t('common.cancel') }}</el-button>
        <el-button size="small" type="primary" :loading="saving" @click="save">{{ $t('common.save') }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useI18n } from 'vue-i18n'
import { useAuthStore } from '@/stores/auth'
import { createUser, deleteUser, getUsers, updateUser, type UserRow } from '@/api/users'

withDefaults(defineProps<{ embedded?: boolean }>(), { embedded: false })

const { t } = useI18n()
const authStore = useAuthStore()
const users = ref<UserRow[]>([])
const loading = ref(false)
const saving = ref(false)
const dialogVisible = ref(false)
const editing = ref<UserRow | null>(null)
const currentUsername = ref('')
const roles = ['admin', 'operator', 'viewer']

const form = reactive({
  username: '',
  display_name: '',
  role: 'operator',
  enabled: '1',
  password: '',
})

function roleLabel(role: string) {
  return t(`users.role_${role}`)
}

function roleType(role: string): 'danger' | 'primary' | 'info' {
  if (role === 'admin') return 'danger'
  if (role === 'operator') return 'primary'
  return 'info'
}

async function refresh() {
  loading.value = true
  try {
    const res = await getUsers()
    users.value = res.data
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || t('users.loadFailed'))
  } finally {
    loading.value = false
  }
}

function openCreate() {
  editing.value = null
  Object.assign(form, { username: '', display_name: '', role: 'operator', enabled: '1', password: '' })
  dialogVisible.value = true
}

function openEdit(row: UserRow) {
  editing.value = row
  Object.assign(form, {
    username: row.username,
    display_name: row.display_name,
    role: row.role,
    enabled: String(row.enabled),
    password: '',
  })
  dialogVisible.value = true
}

async function save() {
  saving.value = true
  try {
    if (editing.value) {
      await updateUser(editing.value.id, {
        display_name: form.display_name,
        role: form.role,
        enabled: form.enabled === '1',
        ...(form.password ? { password: form.password } : {}),
      })
      ElMessage.success(t('users.updateSuccess'))
    } else {
      await createUser({
        username: form.username,
        display_name: form.display_name,
        role: form.role,
        enabled: form.enabled === '1',
        password: form.password,
      })
      ElMessage.success(t('users.createSuccess'))
    }
    dialogVisible.value = false
    await refresh()
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || t('common.saveFailed'))
  } finally {
    saving.value = false
  }
}

async function confirmDelete(row: UserRow) {
  try {
    await ElMessageBox.confirm(
      t('users.confirmDelete', { username: row.username }),
      t('common.confirm'),
      { confirmButtonText: t('common.delete'), cancelButtonText: t('common.cancel'), type: 'warning' }
    )
  } catch {
    return
  }
  try {
    await deleteUser(row.id)
    ElMessage.success(t('users.deleteSuccess'))
    await refresh()
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || t('common.saveFailed'))
  }
}

onMounted(() => {
  currentUsername.value = authStore.username
  refresh()
})
</script>

<style scoped>
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}
.page-header h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 600;
}
.page-header p {
  margin: 6px 0 0;
  color: var(--mc-text-secondary);
  font-size: 13px;
}
.self-tag {
  margin-left: 8px;
}
</style>
