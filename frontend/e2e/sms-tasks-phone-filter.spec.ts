import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('sms task list filters by phone number', async ({ page }) => {
  const errors: string[] = []
  const taskQueries: string[] = []
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
      const msisdn = url.searchParams.get('msisdn') || ''
      taskQueries.push(url.search)
      const filtered = msisdn === '251900000922'
      return json({
        total: 1,
        page: 1,
        page_size: 50,
        items: [{
          id: filtered ? 22 : 11,
          campaign_name: 'Phone filter QA',
          campaign_type: 'batch',
          smpp_config_id: 0,
          msisdn: filtered ? '251900000922' : '251900000911',
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
    if (url.pathname === '/api/sms-tasks/stats') return json({ pipeline: 'vnext', socket_written: 1 })
    if (url.pathname === '/api/smpp/configs') return json([])
    if (url.pathname === '/api/sms-tasks/exclusions/list') return json({ total: 0, items: [] })
    return json({})
  })

  await page.goto('/sms-tasks')
  await expect(page.getByText('251900000911', { exact: true })).toBeVisible()

  const phoneFilter = page.getByLabel('Phone Number')
  await phoneFilter.fill('+251 900000922')
  await phoneFilter.press('Enter')

  await expect(page.getByText('251900000922', { exact: true })).toBeVisible()
  await expect(page.getByText('251900000911', { exact: true })).toHaveCount(0)
  await expect(page).toHaveURL(/msisdn=251900000922/)
  expect(taskQueries.some(query => query.includes('msisdn=251900000922'))).toBeTruthy()
  expect(errors).toEqual([])

  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-sms-task-phone-filter.png'),
    fullPage: true,
  })
})
