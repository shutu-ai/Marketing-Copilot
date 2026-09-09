import { expect, test } from '@playwright/test'

test('P2 capacity dashboard renders worker, pool and alert state', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
  })

  const errors: string[] = []
  page.on('console', message => { if (message.type() === 'error') errors.push(message.text()) })
  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const url = new URL(route.request().url())
    const body = url.pathname.endsWith('/alerts/evaluate') ? { alerts: [{ code: 'QUEUE_DEPTH_HIGH' }] } : url.pathname.endsWith('/smpp/stats') ? {
      summary: { queue_size: 12, submitted_1m: 2, submitted_60m: 8, today_submitted: 20, today_delivered: 18, today_failed: 2, unassigned: 0 },
      items: [],
    } : {
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
  const alertEvaluation = page.waitForResponse(response => response.url().endsWith('/api/metrics/alerts/evaluate') && response.ok())
  await page.getByRole('button', { name: /评估告警/ }).click()
  expect((await (await alertEvaluation).json()).alerts).toHaveLength(1)
  expect(errors).toEqual([])
})
