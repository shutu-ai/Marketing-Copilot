<template>
  <div class="dashboard">
    <h2>{{ $t('dashboard.title') }}</h2>

    <h3 class="section-title">{{ $t('dashboard.batchSection') }}</h3>
    <el-row :gutter="20" style="margin-bottom: 24px">
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #ecf5ff">
              <el-icon :size="28" color="#409eff"><ChatDotSquare /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.total_batch_campaigns }}</div>
              <div class="stat-label">{{ $t('dashboard.totalCampaigns') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #f0f9eb">
              <el-icon :size="28" color="#67c23a"><Promotion /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.in_progress_campaigns }}</div>
              <div class="stat-label">{{ $t('dashboard.inProgress') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #fdf6ec">
              <el-icon :size="28" color="#e6a23c"><Message /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.today_sent_count }}</div>
              <div class="stat-label">{{ $t('dashboard.todaySent') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #fef0f0">
              <el-icon :size="28" color="#f56c6c"><Finished /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.ended_campaigns }}</div>
              <div class="stat-label">{{ $t('dashboard.ended') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <h3 class="section-title">{{ $t('dashboard.realtimeSection') }}</h3>
    <el-row :gutter="20">
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #ecf5ff">
              <el-icon :size="28" color="#409eff"><Connection /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.realtime_campaigns }}</div>
              <div class="stat-label">{{ $t('dashboard.realtimeCampaigns') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #f0f9eb">
              <el-icon :size="28" color="#67c23a"><UploadFilled /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.realtime_files }}</div>
              <div class="stat-label">{{ $t('dashboard.realtimeFiles') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #fdf6ec">
              <el-icon :size="28" color="#e6a23c"><User /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.realtime_event_users }}</div>
              <div class="stat-label">{{ $t('dashboard.realtimeEventUsers') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <div class="stat-card">
            <div class="stat-icon" style="background: #fef0f0">
              <el-icon :size="28" color="#f56c6c"><Select /></el-icon>
            </div>
            <div class="stat-info">
              <div class="stat-value">{{ stats.realtime_matched_users }}</div>
              <div class="stat-label">{{ $t('dashboard.realtimeMatchedUsers') }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { onMounted, reactive } from 'vue'
import { getDashboardStats } from '@/api/dashboard'

const stats = reactive({
  total_batch_campaigns: 0,
  in_progress_campaigns: 0,
  ended_campaigns: 0,
  today_sent_count: 0,
  total_sms_campaigns: 0,
  realtime_campaigns: 0,
  realtime_files: 0,
  realtime_event_users: 0,
  realtime_matched_users: 0,
})

onMounted(async () => {
  try {
    const res = await getDashboardStats()
    Object.assign(stats, res.data)
  } catch (e) {
    console.error(e)
  }
})
</script>

<style scoped>
.dashboard h2 {
  margin: 0 0 8px 0;
  font-size: 22px;
  font-weight: 600;
}
.section-title {
  margin: 0 0 12px 0;
  font-size: 16px;
  font-weight: 600;
  color: #606266;
}
.stat-card {
  display: flex;
  align-items: center;
  gap: 16px;
}
.stat-icon {
  width: 56px;
  height: 56px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
}
.stat-value {
  font-size: 28px;
  font-weight: bold;
  line-height: 1.2;
}
.stat-label {
  font-size: 13px;
  color: #909399;
  margin-top: 4px;
}
</style>
