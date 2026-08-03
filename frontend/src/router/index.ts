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
          meta: { titleKey: 'dashboard.title' },
          component: () => import('@/views/dashboard/Dashboard.vue'),
        },
        {
          path: 'data-monitor',
          name: 'DataMonitor',
          meta: { titleKey: 'monitor.title' },
          component: () => import('@/views/monitor/FileMonitor.vue'),
        },
        {
          path: 'campaign/batch',
          name: 'BatchCampaign',
          meta: { titleKey: 'campaign.batchTitle' },
          component: () => import('@/views/campaign/BatchCampaign.vue'),
        },
        {
          path: 'campaign/batch/:id',
          name: 'CampaignDetail',
          meta: { titleKey: 'campaign.detailTitle' },
          component: () => import('@/views/campaign/CampaignDetail.vue'),
        },
        {
          path: 'campaign/realtime',
          name: 'RealtimeCampaign',
          meta: { titleKey: 'campaign.realtimeTitle' },
          component: () => import('@/views/campaign/RealtimeCampaign.vue'),
        },
        {
          path: 'campaign/realtime/:id',
          name: 'RealtimeCampaignDetail',
          meta: { titleKey: 'campaign.realtimeDetailTitle' },
          component: () => import('@/views/campaign/RealtimeCampaignDetail.vue'),
        },
        {
          path: 'smpp-config',
          redirect: { path: '/settings', query: { tab: 'smpp' } },
        },
        {
          path: 'sms-content',
          name: 'SmsContent',
          meta: { titleKey: 'smsContent.title' },
          component: () => import('@/views/sms-content/SmsContentConfig.vue'),
        },
        {
          path: 'sms-content/campaign/:campaignId',
          name: 'SmsContentCampaignPage',
          meta: { titleKey: 'smsContent.editorTitle' },
          component: () => import('@/views/sms-content/SmsContentCampaignPage.vue'),
        },
        {
          path: 'rabbitmq',
          name: 'RabbitMQ',
          meta: { titleKey: 'rabbitmq.title' },
          component: () => import('@/views/rabbitmq/RabbitMQConfig.vue'),
        },
        {
          path: 'sms-log',
          name: 'SmsLog',
          meta: { titleKey: 'smsLog.title' },
          component: () => import('@/views/rabbitmq/SmsLog.vue'),
        },
        {
          path: 'sms-tasks',
          redirect: to => ({
            path: '/operations',
            query: { ...to.query, tab: 'sms-tasks' },
          }),
        },
        {
          path: 'operations',
          name: 'OperationsCenter',
          meta: { titleKey: 'operations.title' },
          component: () => import('@/views/operations/OperationsCenter.vue'),
        },
        {
          path: 'capacity',
          redirect: to => ({
            path: '/operations',
            query: { ...to.query, tab: 'capacity' },
          }),
        },
        {
          path: 'db-query',
          name: 'DbQuery',
          meta: { titleKey: 'dbQuery.title' },
          component: () => import('@/views/dbQuery/DbQuery.vue'),
        },
        {
          path: 'db-config',
          redirect: { path: '/settings', query: { tab: 'database' } },
        },
        {
          path: 'settings',
          name: 'AppSettings',
          meta: { titleKey: 'settings.title' },
          component: () => import('@/views/settings/AppSettings.vue'),
        },
      ],
    },
  ],
})

export default router
