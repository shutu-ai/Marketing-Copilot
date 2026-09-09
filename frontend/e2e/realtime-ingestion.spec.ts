import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('realtime ingestion queue shows progress and supports retry', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
  })

  let retryCalls = 0
  const state = {
    campaign_id: 7,
    watch_dir: 'C:/watch',
    watch_dirs: [],
    campaigns: [{ id: 7, name: 'Realtime QA', status: 'in_progress', watch_filename_prefix: 'Android_' }],
    exists: true,
    active_campaigns: 1,
    source_files: [],
    source_files_total: 0,
    source_files_page: 1,
    source_files_page_size: 50,
    processed_files: [{
      id: 11, campaign_id: 7, filename: 'Android_20260909120000.csv', file_path: 'C:/quarantine/file.csv',
      file_size: 120, total_rows: 10, created_tasks: 0, status: 'quarantined', processing_phase: 'quarantined',
      processed_rows: 10, matched_rows: 0, retry_count: 1, last_error_class: 'INVALID_INPUT',
      error_detail: 'CSV columns are missing', ingestion_file_id: 21, detected_at: '2026-09-09 12:00:00',
    }],
    processed_files_total: 1,
    processed_files_page: 1,
    processed_files_page_size: 50,
    ingestion_files: [{
      id: 21, campaign_id: 7, filename: 'Android_20260909120000.csv', status: 'processing',
      processing_phase: 'matching', processed_rows: 6, total_rows: 10, matched_rows: 4,
      retry_count: 0, last_error_class: '', error_detail: '', last_progress_at: '2026-09-09 12:01:00',
    }],
  }

  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/monitor/realtime') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(state) })
    }
    if (url.pathname === '/api/monitor/realtime/files/21/retry') {
      retryCalls += 1
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true }) })
    }
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({}) })
  })

  await page.goto('/data-monitor')
  await expect(page).toHaveURL(/\/data-monitor$/)
  await expect(page.getByText('Realtime ingestion queue')).toBeVisible()
  await expect(page.getByText('matching')).toBeVisible()
  await expect(page.getByText('6 / 10 rows')).toBeVisible()
  await expect(page.getByText('CSV columns are missing')).toBeVisible()
  await page.getByRole('button', { name: 'Retry', exact: true }).click()
  await expect.poll(() => retryCalls).toBe(1)
  await page.screenshot({ path: path.join(os.tmpdir(), 'marketing-copilot-realtime-ingestion.png'), fullPage: false })
})
