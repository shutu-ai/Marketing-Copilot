import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('realtime campaign detail shows current watch files in its own tab', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
    localStorage.setItem('locale', 'en')
  })

  const watchFiles = {
    total: 1,
    page: 1,
    page_size: 25,
    items: [{
      filename: 'Android_20260909120000.csv',
      file_size: 120,
      row_count: 2,
      status: 'pending',
      status_label: '待处理',
      match_prefix: true,
      match_schedule: true,
      skip_reason: '',
    }],
  }

  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/realtime-campaigns/7/detail') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
        id: 7, name: 'Realtime QA', status: 'in_progress', use_whitelist: false,
        start_time: '', end_time: '', send_start_time: '', send_end_time: '', send_days: [],
        watch_dir: 'C:/watch', watch_filename_prefix: 'Android_', campaign_dir: 'C:/campaign-7',
        customer_list_filename: '', row_count: 0, smpp_config_id: null,
      }) })
    }
    if (url.pathname === '/api/realtime-campaigns/7/stats') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
        file_count: 1, total_msisdn_sum: 2, total_matched: 1,
      }) })
    }
    if (url.pathname === '/api/realtime-campaigns/7/files') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ total: 0, items: [] }) })
    }
    if (url.pathname === '/api/realtime-campaigns/7/sent-records') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ total: 0, items: [] }) })
    }
    if (url.pathname === '/api/realtime-campaigns/7/watch-files') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(watchFiles) })
    }
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([]) })
  })

  await page.goto('/campaign/realtime/7')
  await expect(page).toHaveURL(/\/campaign\/realtime\/7$/)
  const watchTab = page.getByRole('tab', { name: 'Current Watch Files' })
  await expect(watchTab).toBeVisible()
  await watchTab.click()
  await expect(page.getByText('Android_20260909120000.csv')).toBeVisible()
  await expect(page.getByText('Pending', { exact: true })).toBeVisible()
  await page.screenshot({ path: path.join(os.tmpdir(), 'marketing-copilot-realtime-watch-files.png'), fullPage: false })
})
