import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('P2 capacity dashboard renders worker, pool and alert state', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
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

test('VNext capacity metrics use aligned units and one lightweight snapshot', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
    localStorage.setItem('locale', 'en')
  })

  const errors: string[] = []
  const apiRequests: string[] = []
  page.on('console', message => { if (message.type() === 'error') errors.push(message.text()) })
  page.on('request', request => {
    if (request.url().includes('/api/')) apiRequests.push(request.url())
  })

  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/metrics/status') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          workers: { configured: 4, running: 4 },
          smpp: {
            pipeline: 'vnext',
            summary: {
              queue_size: 12,
              socket_written_parts_1m: 12000,
              socket_written_tps_1m: 200,
              submit_sm_resp_success_1m: 11800,
              submit_sm_resp_failed_1m: 200,
              today_socket_written: 120,
              today_smsc_accepted: 110,
              today_smsc_rejected: 8,
              today_unknown: 1,
              today_write_failed: 1,
              effective_total_limit_tps: 800,
              projection_lag_seconds: 0.7,
            },
            items: [{
              config_id: 1,
              name: 'Primary',
              host: 'smsc.example',
              port: 2775,
              healthy: true,
              enabled: 1,
              connected_count: 4,
              desired_pool_size: 4,
              socket_written_parts_1m: 12000,
              observed_parts_tps: 200,
              submit_sm_resp_success_1m: 11800,
              submit_sm_resp_failed_1m: 200,
              unknown_parts_1m: 1,
              effective_total_limit_tps: 800,
              connections: [],
            }],
          },
        }),
      })
    }
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({}) })
  })

  await page.goto('/capacity')
  await expect(page).toHaveURL(/\/operations\?tab=capacity/)
  await expect(page.getByRole('heading', { name: 'Capacity & Availability' })).toBeVisible()
  await expect(page.getByText('SMSC rejected / last minute (PDU parts)').first()).toBeVisible()
  await expect(page.getByText('SMSC rejected today (PDU parts)')).toBeVisible()
  await expect(page.getByText('Sent today (messages)')).toBeVisible()
  await expect(page.getByText('Projection lag')).toBeVisible()
  await expect(page.getByText('SMSC rejected / last minute (PDU parts)').locator('..').getByText('200')).toBeVisible()
  await expect(page.getByText('SMSC rejected today (PDU parts)').locator('..').getByText('8')).toBeVisible()
  await expect(page.getByText('Sent today (messages)').locator('..').getByText('120')).toBeVisible()
  await expect(page.locator('.el-statistic').filter({ hasText: 'Projection lag' })).toContainText('0.7')
  await expect(page.getByText('Primary').last()).toBeVisible()
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-capacity-vnext-desktop.png'),
    fullPage: false,
  })

  await page.setViewportSize({ width: 390, height: 844 })
  await expect(page.getByText('SMSC rejected today (PDU parts)')).toBeVisible()
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-capacity-vnext-mobile.png'),
    fullPage: false,
  })

  expect(apiRequests.filter(url => url.includes('/api/smpp/stats')).length).toBe(0)
  expect(errors).toEqual([])
})
