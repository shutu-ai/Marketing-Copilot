import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('sms task timestamps are rendered as readable date and time', async ({ page }) => {
  const errors: string[] = []
  page.on('console', message => {
    if (message.type() === 'error') errors.push(message.text())
  })

  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
    localStorage.setItem('locale', 'en')
  })
  await page.setViewportSize({ width: 1440, height: 1100 })

  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const request = route.request()
    const url = new URL(request.url())
    const json = (body: unknown) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(body),
    })

    if (url.pathname === '/api/sms-tasks' && request.method() === 'GET') {
      return json({
        total: 1,
        page: 1,
        page_size: 50,
        items: [{
          id: 11,
          campaign_name: 'Batch QA',
          campaign_type: 'batch',
          smpp_config_id: 0,
          msisdn: '251900009911',
          language: 'English',
          content: 'Hello',
          status: 'socket_written',
          attempts: 0,
          error_msg: '',
          updated_at: '2026-10-20T13:14:15.123Z',
        }],
        next_cursor: null,
        has_more: false,
      })
    }
    if (url.pathname === '/api/sms-tasks/stats') {
      return json({ pipeline: 'vnext', socket_written: 1 })
    }
    if (url.pathname === '/api/smpp/configs') return json([])
    if (url.pathname === '/api/sms-tasks/exclusions/list') return json({ total: 0, items: [] })
    return json({})
  })

  await page.goto('/sms-tasks')
  await expect(page).toHaveURL(/\/sms-tasks$/)
  await expect(page.getByText('Batch QA', { exact: true })).toBeVisible()
  const readableTimestamp = page.getByText('2026-10-20 13:14:15', { exact: true })
  await expect(readableTimestamp).toBeVisible()
  await expect(page.getByText('2026-10-20T13:14:15.123Z', { exact: true })).toHaveCount(0)
  expect(errors).toEqual([])
  await page.locator('.el-table__body-wrapper, .el-table__header-wrapper').evaluateAll(elements => {
    elements.forEach(element => { element.scrollLeft = element.scrollWidth })
  })
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-sms-task-readable-time.png'),
    fullPage: true,
  })
  await readableTimestamp.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-sms-task-readable-time-cell.png'),
  })
})
