<template>
  <div class="rabbitmq-config">
    <div class="page-header">
      <h2>{{ $t('rabbitmq.title') }}</h2>
      <div>
        <el-tag :type="connected ? 'success' : 'danger'" size="default">
          {{ connected ? $t('rabbitmq.connected') : $t('rabbitmq.disconnected') }}
        </el-tag>
      </div>
    </div>

    <el-card shadow="never" style="margin-bottom: 20px">
      <el-form :model="form" label-width="140px" size="small">
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.host')" required>
              <el-input v-model="form.host" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.port')" required>
              <el-input-number v-model="form.port" :min="1" :max="65535" style="width: 100%" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.virtualHost')">
              <el-input v-model="form.virtual_host" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.exchangeName')">
              <el-input v-model="form.exchange_name" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.username')">
              <el-input v-model="form.username" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.password')">
              <el-input v-model="form.password" type="password" show-password />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="20">
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.queuePrefix')">
              <el-input v-model="form.queue_prefix" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item :label="$t('rabbitmq.enabled')">
              <el-switch v-model="form.enabled" :active-value="1" :inactive-value="0" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item>
          <el-button type="primary" @click="handleSave">
            <el-icon><Check /></el-icon>
            {{ $t('common.save') }}
          </el-button>
          <el-button @click="handleRefresh">
            <el-icon><Refresh /></el-icon>
            {{ $t('common.test') }}
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card shadow="never">
      <template #header>
        <span>{{ $t('rabbitmq.testPublish') }}</span>
      </template>
      <el-form :model="testForm" label-width="120px" size="small">
        <el-form-item :label="$t('rabbitmq.routingKey')">
          <el-input v-model="testForm.routing_key" placeholder="campaign.my_campaign" />
        </el-form-item>
        <el-form-item :label="$t('rabbitmq.messageContent')">
          <el-input v-model="testForm.message" type="textarea" :rows="4" placeholder='{"msisdn":"66812345678","name":"Test"}' />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="testing" @click="handleTestPublish">
            {{ $t('rabbitmq.publish') }}
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import i18n from '@/i18n'
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { getRabbitMQConfig, saveRabbitMQConfig, updateRabbitMQConfig, getRabbitMQStatus, testPublish } from '@/api/rabbitmq'

const connected = ref(false)
const testing = ref(false)

const form = ref<any>({
  host: 'localhost',
  port: 5672,
  virtual_host: '/',
  username: 'guest',
  password: 'guest',
  exchange_name: 'marketing_copilot',
  queue_prefix: 'mc_',
  enabled: 0,
})

const testForm = ref({
  routing_key: 'campaign.test_campaign',
  message: '{"msisdn":"66812345678","name":"TestUser"}',
})

onMounted(async () => {
  try {
    const res = await getRabbitMQConfig()
    if (res.data.id) {
      form.value = res.data
      form.value.enabled = res.data.enabled ?? 0
    }
    const statusRes = await getRabbitMQStatus()
    connected.value = statusRes.data.connected
  } catch {}
})

async function handleSave() {
  try {
    if (form.value.id) {
      await updateRabbitMQConfig(form.value)
    } else {
      await saveRabbitMQConfig(form.value)
    }
    ElMessage.success(i18n.global.t('common.saveSuccess'))
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('common.saveFailed'))
  }
}

async function handleRefresh() {
  try {
    const res = await getRabbitMQStatus()
    connected.value = res.data.connected
    ElMessage.info(res.data.message)
  } catch {}
}

async function handleTestPublish() {
  testing.value = true
  try {
    let msg: any
    try {
      msg = JSON.parse(testForm.value.message)
    } catch {
      msg = { raw: testForm.value.message }
    }
    const res = await testPublish(testForm.value.routing_key, msg)
    if (res.data.success) {
      ElMessage.success(i18n.global.t('monitor.publishSuccess'))
    } else {
      ElMessage.warning(i18n.global.t('dynamic.publishFailed',{message:res.data.message}))
    }
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('monitor.publishFailed'))
  } finally {
    testing.value = false
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
.page-header h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 600;
}
</style>
