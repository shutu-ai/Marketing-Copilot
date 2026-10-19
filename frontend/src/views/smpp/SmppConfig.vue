<template>
  <div class="smpp-config">
    <el-alert
      v-if="restartRequired"
      type="warning"
      :closable="false"
      show-icon
      :title="$t('smpp.restartRequired')"
      class="restart-alert"
    />
    <div class="page-header">
      <h2>{{ $t('smpp.title') }}</h2>
      <el-button type="primary" @click="openDialog()">
        <el-icon><Plus /></el-icon>
        {{ $t('smpp.addConfig') }}
      </el-button>
    </div>

    <el-card shadow="never">
      <el-table :data="store.configs" stripe v-loading="store.loading" style="width: 100%">
        <el-table-column prop="name" :label="$t('smpp.name')" min-width="150" />
        <el-table-column prop="host" :label="$t('smpp.host')" width="140" />
        <el-table-column prop="port" :label="$t('smpp.port')" width="80" />
        <el-table-column prop="system_id" :label="$t('smpp.systemId')" width="130" />
        <el-table-column prop="source_addr" :label="$t('smpp.sourceAddr')" width="130" />
        <el-table-column prop="bind_type" :label="$t('smpp.bindType')" width="120" />
        <el-table-column prop="priority" :label="$t('legacy.xee8ecb9e')" width="90" />
        <el-table-column prop="pool_size" :label="$t('remaining.x64cea34e')" width="80" />
        <el-table-column prop="window_size" :label="$t('smpp.windowSize')" width="100" />
        <el-table-column prop="tps" label="TPS" width="70" />
        <el-table-column :label="$t('common.status')" width="80">
          <template #default="{ row }">
            <StatusTag :status="row.enabled ? 'enabled' : 'disabled'" />
          </template>
        </el-table-column>
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="170" />
        <el-table-column :label="$t('common.action')" width="220" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="testConfig(row)">
              {{ $t('smpp.testConnection') }}
            </el-button>
            <el-button text type="primary" size="small" @click="openDialog(row)">
              {{ $t('common.edit') }}
            </el-button>
            <el-button text type="danger" size="small" @click="handleDelete(row.id)">
              {{ $t('common.delete') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog
      v-model="dialogVisible"
      :title="editingId ? $t('smpp.editConfig') : $t('smpp.addConfig')"
      width="680px"
      :close-on-click-modal="false"
    >
      <el-form :model="form" label-width="140px" size="small">
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.name')" required>
              <el-input v-model="form.name" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('smpp.host')" required>
              <el-input v-model="form.host" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.port')" required>
              <el-input-number v-model="form.port" :min="1" :max="65535" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('smpp.systemId')" required>
              <el-input v-model="form.system_id" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.password')" :required="!editingId">
              <el-input
                v-model="form.password"
                type="password"
                show-password
                :placeholder="editingId ? $t('smpp.passwordPlaceholder') : ''"
              />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('smpp.sourceAddr')">
              <el-input v-model="form.source_addr" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.bindType')">
              <el-select v-model="form.bind_type" style="width: 100%">
                <el-option label="transmitter" value="transmitter" />
                <el-option label="receiver" value="receiver" />
                <el-option label="transceiver" value="transceiver" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('smpp.dataCoding')">
              <el-select v-model="form.data_coding" style="width: 100%">
                <el-option :label="'0 - GSM 7-bit'" :value="0" />
                <el-option :label="'3 - Latin-1'" :value="3" />
                <el-option :label="'4 - UCS-2'" :value="4" />
                <el-option :label="'8 - UCS-2'" :value="8" />
              </el-select>
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.deliveryReceiptMode')">
              <el-select v-model="form.delivery_receipt_mode" style="width: 100%">
                <el-option :label="$t('smpp.deliveryReceiptRequired')" value="required" />
                <el-option :label="$t('smpp.deliveryReceiptOptional')" value="optional" />
                <el-option :label="$t('smpp.deliveryReceiptDisabled')" value="disabled" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('smpp.addressTon')">
              <el-input-number v-model="form.address_ton" :min="0" :max="255" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('smpp.addressNpi')">
              <el-input-number v-model="form.address_npi" :min="0" :max="255" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20" class="smpp-throughput-row">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.poolSize')">
              <el-input-number v-model="form.pool_size" :min="1" :max="20" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('smpp.windowSize')">
              <el-input-number v-model="form.window_size" :min="1" :max="100" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20" class="smpp-throughput-row">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.enquireInterval')">
              <el-input-number v-model="form.enquire_interval" :min="0" :max="300" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('smpp.timeout')">
              <el-input-number v-model="form.timeout" :min="1" :max="120" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('legacy.x4d8329b8')">
              <el-input-number v-model="form.priority" :min="1" :max="10000" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('legacy.x7c8fbe80')">
              <el-input-number v-model="form.tps" :min="0" :max="10000" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('legacy.x62072bea')">
              <el-input-number v-model="form.reconnect_max_seconds" :min="0" :max="300" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">{{ $t('common.cancel') }}</el-button>
        <el-button type="primary" @click="handleSave">{{ $t('common.save') }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import i18n from '@/i18n'
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useSmppStore } from '@/stores/smpp'
import type { SmppConfig, SmppConfigForm } from '@/types/smpp'
import StatusTag from '@/components/StatusTag.vue'

withDefaults(defineProps<{ embedded?: boolean }>(), { embedded: false })

const store = useSmppStore()
const dialogVisible = ref(false)
const editingId = ref<number | null>(null)

const defaultForm: SmppConfigForm = {
  name: '', host: '', port: 2775, system_id: '', password: '',
  source_addr: '', bind_type: 'transceiver', address_ton: 0, address_npi: 0,
  data_coding: 0, pool_size: 1, window_size: 10, enquire_interval: 30, timeout: 10,
  tps: 20, priority: 100, reconnect_max_seconds: 60, delivery_receipt_mode: 'optional',
}
const form = ref<SmppConfigForm>({ ...defaultForm })
const restartRequired = ref(false)

onMounted(() => store.fetchConfigs())

function openDialog(row?: SmppConfig) {
  if (row) {
    editingId.value = row.id
    form.value = {
      name: row.name, host: row.host, port: row.port,
      system_id: row.system_id, password: '',
      source_addr: row.source_addr, bind_type: row.bind_type,
      address_ton: row.address_ton, address_npi: row.address_npi,
      data_coding: row.data_coding, pool_size: row.pool_size, window_size: row.window_size || 10,
      enquire_interval: row.enquire_interval, timeout: row.timeout,
      tps: row.tps, priority: row.priority, reconnect_max_seconds: row.reconnect_max_seconds,
      delivery_receipt_mode: row.delivery_receipt_mode || 'optional',
    }
  } else {
    editingId.value = null
    form.value = { ...defaultForm }
  }
  dialogVisible.value = true
}

async function handleDelete(id: number) {
  try {
    await ElMessageBox.confirm(i18n.global.t('legacy.xb28efaa0'), i18n.global.t('legacy.x02d9819d'))
    const response = await store.removeConfig(id)
    restartRequired.value = Boolean(response?.restart_required) || restartRequired.value
    ElMessage.success(i18n.global.t('legacy.x0007d170'))
  } catch {}
}

async function handleSave() {
  try {
    if (editingId.value) {
      const response = await store.editConfig(editingId.value, form.value)
      restartRequired.value = Boolean((response as any)?.restart_required) || restartRequired.value
      ElMessage.success(i18n.global.t('legacy.x55aa6366'))
    } else {
      const response = await store.addConfig(form.value)
      restartRequired.value = Boolean((response as any)?.restart_required) || restartRequired.value
      ElMessage.success(i18n.global.t('legacy.xa5bfd70d'))
    }
    dialogVisible.value = false
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('common.saveFailed'))
  }
}

async function testConfig(row: SmppConfig) {
  try {
    const result = await store.testConnection({
      config_id: row.id,
      host: row.host, port: row.port,
      system_id: row.system_id, password: row.password,
      bind_type: row.bind_type,
    })
    if (result.success) {
      ElMessage.success(i18n.global.t('dbConfig.testSuccess'))
    } else {
      ElMessage.error(i18n.global.t('dynamic.connectionTestFailed',{message:result.message}))
    }
  } catch (e: any) {
    ElMessage.error(i18n.global.t('dynamic.testFailed',{message:e.response?.data?.detail || e.message}))
  }
}
</script>

<style scoped>
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}
.restart-alert { margin-bottom: 16px; }
.page-header.embedded {
  justify-content: flex-end;
}
.page-header h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 600;
}
</style>
