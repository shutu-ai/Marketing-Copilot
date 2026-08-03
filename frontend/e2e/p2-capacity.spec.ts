import { expect, test } from '@playwright/test'

test('P2 capacity dashboard renders worker, pool and alert state', async ({ page }) => {
  const errors: string[] = []
  page.on('console', message => { if (message.type() === 'error') errors.push(message.text()) })
  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const url = new URL(route.request().url())
    const body = url.pathname.endsWith('/alerts/evaluate') ? { alerts: [{ code: 'QUEUE_DEPTH_HIGH' }] } : {
      workers: { configured: 4, running: 4 },
      smpp: { enabled_configs: 2, active_config_id: 1, pools: [
        { config_id: 1, name: 'Primary', priority: 10, desired_pool_size: 2, healthy: true, cooldown_seconds: 0,
          connections: [{ connected: true, last_error: '' }, { connected: true, last_error: '' }] },
        { config_id: 2, name: 'Backup', priority: 20, desired_pool_size: 1, healthy: false, cooldown_seconds: 30,
          connections: [{ connected: false, last_error: 'connection refused' }] },
      ] },
      metrics: { gauges: [{ name: 'marketing_copilot_queue_depth', labels: {}, value: 12 }], counters: [] },
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
  })
  await page.goto('/capacity')
  await expect(page).toHaveURL(/\/operations\?tab=capacity/)
  await expect(page.getByRole('heading', { name: '容量与高可用' })).toBeVisible()
  await expect(page.getByText('Primary')).toBeVisible()
  await expect(page.getByText('Backup')).toBeVisible()
  await expect(page.getByText('connection refused')).toBeVisible()
  await expect(page.getByText('marketing_copilot_queue_depth')).toBeVisible()
  await page.getByRole('button', { name: '立即评估告警' }).click()
  await expect(page.getByText('告警评估完成：1 个活动告警')).toBeVisible()
  expect(errors).toEqual([])
})
