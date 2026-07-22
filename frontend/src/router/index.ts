import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      component: () => import('@/views/layout/MainLayout.vue'),
      redirect: '/dashboard',
      children: [
        {
          path: 'dashboard',
          name: 'Dashboard',
          component: () => import('@/views/dashboard/Dashboard.vue'),
        },
        {
          path: 'data-monitor',
          name: 'DataMonitor',
          component: () => import('@/views/monitor/FileMonitor.vue'),
        },
        {
          path: 'campaign/batch',
          name: 'BatchCampaign',
          component: () => import('@/views/campaign/BatchCampaign.vue'),
        },
        {
          path: 'campaign/batch/:id',
          name: 'CampaignDetail',
          component: () => import('@/views/campaign/CampaignDetail.vue'),
        },
        {
          path: 'campaign/realtime',
          name: 'RealtimeCampaign',
          component: () => import('@/views/campaign/RealtimeCampaign.vue'),
        },
        {
          path: 'campaign/realtime/:id',
          name: 'RealtimeCampaignDetail',
          component: () => import('@/views/campaign/RealtimeCampaignDetail.vue'),
        },
        {
          path: 'smpp-config',
          name: 'SmppConfig',
          component: () => import('@/views/smpp/SmppConfig.vue'),
        },
        {
          path: 'whitelist/upload',
          name: 'WhitelistUpload',
          component: () => import('@/views/whitelist/WhitelistUpload.vue'),
        },
        {
          path: 'whitelist/history',
          name: 'UploadHistory',
          component: () => import('@/views/whitelist/UploadHistory.vue'),
        },
        {
          path: 'sms-content',
          name: 'SmsContent',
          component: () => import('@/views/sms-content/SmsContentConfig.vue'),
        },
        {
          path: 'sms-content/campaign/:campaignId',
          name: 'SmsContentCampaignPage',
          component: () => import('@/views/sms-content/SmsContentCampaignPage.vue'),
        },
        {
          path: 'rabbitmq',
          name: 'RabbitMQ',
          component: () => import('@/views/rabbitmq/RabbitMQConfig.vue'),
        },
        {
          path: 'sms-log',
          name: 'SmsLog',
          component: () => import('@/views/rabbitmq/SmsLog.vue'),
        },
        {
          path: 'sms-tasks',
          name: 'SmsTasks',
          component: () => import('@/views/sms-tasks/SmsTasks.vue'),
        },
        {
          path: 'operations',
          name: 'OperationsCenter',
          component: () => import('@/views/operations/OperationsCenter.vue'),
        },
        {
          path: 'capacity',
          name: 'CapacityCenter',
          component: () => import('@/views/operations/CapacityCenter.vue'),
        },
        {
          path: 'db-query',
          name: 'DbQuery',
          component: () => import('@/views/dbQuery/DbQuery.vue'),
        },
        {
          path: 'db-config',
          name: 'DBConfig',
          component: () => import('@/views/dbConfig/DbConfig.vue'),
        },
        {
          path: 'settings',
          name: 'AppSettings',
          component: () => import('@/views/settings/AppSettings.vue'),
        },
      ],
    },
  ],
})

export default router
