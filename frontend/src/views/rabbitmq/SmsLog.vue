<template>
  <div class="sms-log">
    <div class="page-header">
      <h2>{{ $t('smsLog.title') }}</h2>
      <el-button @click="fetchData">
        <el-icon><Refresh /></el-icon>
        {{ $t('common.search') }}
      </el-button>
    </div>

    <el-card shadow="never" style="margin-bottom: 20px">
      <el-form :inline="true" size="small">
        <el-form-item :label="$t('smsLog.msisdn')">
          <el-input v-model="filters.msisdn" placeholder="668..." clearable style="width: 160px" @change="fetchData" />
        </el-form-item>
        <el-form-item :label="$t('common.status')">
          <el-select v-model="filters.status" clearable style="width: 130px" @change="fetchData">
            <el-option :label="$t('common.all')" value="" />
            <el-option :label="$t('smsLog.status_sent')" value="sent" />
            <el-option :label="$t('smsLog.status_failed')" value="failed" />
            <el-option :label="$t('smsLog.status_pending')" value="pending" />
          </el-select>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card shadow="never">
      <el-table :data="logs" stripe v-loading="loading" style="width: 100%">
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="msisdn" :label="$t('smsLog.msisdn')" width="150" />
        <el-table-column prop="campaign_name" :label="$t('smsLog.campaignName')" width="150" />
        <el-table-column prop="language" :label="$t('smsLog.language')" width="80" />
        <el-table-column prop="content" :label="$t('smsLog.content')" min-width="250" show-overflow-tooltip />
        <el-table-column prop="source_addr" :label="$t('smsLog.sourceAddr')" width="120" />
        <el-table-column :label="$t('common.status')" width="100">
          <template #default="{ row }">
            <StatusTag :status="row.status" />
          </template>
        </el-table-column>
        <el-table-column prop="error_msg" :label="$t('smsLog.errorMsg')" width="150" show-overflow-tooltip />
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="170" />
      </el-table>

      <div style="display: flex; justify-content: center; margin-top: 16px">
        <el-pagination
          v-model:current-page="page"
          :page-size="pageSize"
          :total="total"
          layout="prev, pager, next"
          @current-change="fetchData"
        />
      </div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { getSmsLog } from '@/api/rabbitmq'
import StatusTag from '@/components/StatusTag.vue'

const logs = ref<any[]>([])
const loading = ref(false)
const total = ref(0)
const page = ref(1)
const pageSize = ref(50)

const filters = ref({ msisdn: '', status: '' })

onMounted(() => fetchData())

async function fetchData() {
  loading.value = true
  try {
    const res = await getSmsLog({
      page: page.value,
      page_size: pageSize.value,
      status: filters.value.status || undefined,
      msisdn: filters.value.msisdn || undefined,
    })
    logs.value = res.data.items
    total.value = res.data.total
  } catch {
    logs.value = []
  } finally {
    loading.value = false
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
