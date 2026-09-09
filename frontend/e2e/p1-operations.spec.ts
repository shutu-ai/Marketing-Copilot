import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('P1 operations center actions, failures, health and audits', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
  })

  let status = 'in_progress'
  let failed = true
  let healthRecorded = false
  const apiCalls: string[] = []
  page.on('console', message => {
    if (message.type() === 'error') throw new Error(`browser console error: ${message.text()}`)
  })
  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const request = route.request()
    const url = new URL(request.url())
    apiCalls.push(`${request.method()} ${url.pathname}${url.search}`)
    const json = (body: unknown) => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
    if (url.pathname === '/api/operations/progress') return json({ items: [{
      campaign_type: 'realtime', campaign_id: 7, name: 'P1 UI campaign', status,
      tasks: { total: 10, finished: status === 'paused' ? 4 : 3, percent: status === 'paused' ? 40 : 30 },
      files: { total: 1, finished: 1 }, batches: null,
    }], total: 1, page: 1, page_size: 50 })
    if (url.pathname === '/api/operations/campaigns/action') {
      status = JSON.parse(request.postData() || '{}').action === 'pause' ? 'paused' : status
      return json({ status })
    }
    if (url.pathname === '/api/sms-tasks') {
      return json({ items: failed && url.searchParams.get('status') === 'failed' ? [{ id: 91, campaign_name: 'P1 UI campaign', msisdn: '251900000091', status: 'failed', failure_class: 'transport', error_msg: 'timeout' }] : [] })
    }
    if (url.pathname === '/api/operations/tasks/failures') return json({ items: failed ? [{ id: 91, campaign_name: 'P1 UI campaign', msisdn: '251900000091', status: 'failed', failure_class: 'transport', error_msg: 'timeout' }] : [], total: failed ? 1 : 0, page: 1, page_size: 50 })
    if (url.pathname === '/api/operations/tasks/archive') return json({ items: [], total: 0, page: 1, page_size: 50 })
    if (url.pathname === '/api/operations/tasks/bulk') { failed = false; return json({ success: true, count: 1 }) }
    if (url.pathname === '/api/operations/configuration-health') {
      if (url.searchParams.get('record_events') === 'true') healthRecorded = true
      return json([{ code: 'SMPP_ENABLED', ok: false, severity: 'error', message: 'No enabled SMPP configuration' }])
    }
    if (url.pathname === '/api/operations/exceptions') return json({ items: healthRecorded ? [{ id: 3, severity: 'error', category: 'smpp', code: 'SMPP_ENABLED', message: 'No enabled SMPP configuration' }] : [], total: healthRecorded ? 1 : 0, page: 1, page_size: 50 })
    if (url.pathname === '/api/operations/audits') return json({ items: [{ id: 1, username: 'system', action: 'campaign.pause', resource: 'realtime_campaign', resource_id: '7', detail: '{}' }], total: 1, page: 1, page_size: 50 })
    if (url.pathname === '/api/sms-tasks/dlr-codes') return json({ DELIVRD: { label: 'Delivered', status: 'delivered', retryable: false } })
    if (/\/api\/operations\/exceptions\/\d+\/resolve/.test(url.pathname)) return json({ success: true })
    return json({})
  })

  await page.goto('/operations')
  await expect(page).toHaveURL(/\/operations$/)
  await expect(page.getByText('P1 UI campaign')).toBeVisible()
  await expect(page.getByText('30%')).toBeVisible()
  await page.getByRole('button', { name: '暂停' }).click()
  await page.getByRole('button', { name: '确定' }).click()
  await expect(page.getByRole('button', { name: '恢复' })).toBeVisible()
  expect(apiCalls.some(item => item === 'POST /api/operations/campaigns/action')).toBeTruthy()

  await page.getByRole('tab', { name: '短信任务' }).click()
  await expect(page).toHaveURL(/\/operations\?tab=sms-tasks/)
  await expect(page.getByRole('heading', { name: '短信任务实时状态' })).toBeVisible()

  await page.getByRole('tab', { name: '失败处理' }).click()
  await expect(page.getByText('251900000091')).toBeVisible()
  await page.locator('.el-table__body-wrapper .el-checkbox').first().click()
  await page.getByRole('button', { name: '批量重试' }).click()
  await expect(page.getByText('251900000091')).toHaveCount(0)

  await page.getByRole('tab', { name: '异常中心' }).click()
  await page.getByRole('button', { name: '检查配置并记录异常' }).click()
  await expect(page.getByText('No enabled SMPP configuration').first()).toBeVisible()
  expect(apiCalls.some(item => item.includes('record_events=true'))).toBeTruthy()

  await page.getByRole('tab', { name: '审计与报表' }).click()
  await expect(page.getByText('campaign.pause')).toBeVisible()
  const screenshot = path.join(os.tmpdir(), 'marketing-copilot-p1-operations.png')
  await page.screenshot({ path: screenshot, fullPage: false })
})
