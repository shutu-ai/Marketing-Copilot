import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('realtime campaign editor keeps SSH/SFTP setup compact and navigable', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
    localStorage.setItem('locale', 'en')
  })

  const campaign = {
    id: 7,
    name: 'Realtime QA',
    use_whitelist: false,
    start_time: '2026-10-18 08:00:00',
    end_time: '2026-10-20 18:00:00',
    send_start_time: '08:00',
    send_end_time: '18:00',
    send_days: ['1', '2', '3', '4', '5'],
    status: 'not_started',
    customer_list_filename: '',
    customer_list_path: '',
    row_count: 0,
    campaign_dir: '',
    use_custom_directory: false,
    watch_source: 'local',
    watch_dir: 'C:/watch',
    sftp_host: '',
    sftp_port: 22,
    sftp_username: '',
    sftp_password: '',
    watch_filename_prefix: 'Android_',
    phone_prefix: '251',
    sms_template_id: null,
    smpp_config_id: 1,
    flash_sms: false,
    created_at: '2026-10-18 08:00:00',
  }

  await page.route(/^https?:\/\/[^/]+\/api\//, async (route) => {
    const url = new URL(route.request().url())
    const json = (body: unknown) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(body),
    })

    if (url.pathname === '/api/realtime-campaigns') {
      return json({ total: 1, page: 1, page_size: 50, items: [campaign] })
    }
    if (url.pathname === '/api/realtime-campaigns/7/stats') {
      return json({ file_count: 2, total_msisdn_sum: 100, total_matched: 80 })
    }
    if (url.pathname === '/api/smpp/configs') {
      return json([{ id: 1, name: 'QA SMPP', host: '127.0.0.1', port: 2775, enabled: 1 }])
    }
    return json([])
  })

  await page.goto('/campaign/realtime')
  await expect(page).toHaveURL(/\/campaign\/realtime$/)
  await expect(page.getByText('Realtime QA')).toBeVisible()

  await page.getByRole('button', { name: 'Add Campaign' }).click()
  const dialog = page.getByRole('dialog')
  await expect(dialog).toBeVisible()
  await expect(dialog.locator('.editor-intro')).toHaveCount(0)
  await expect(dialog.locator('.editor-footer__status')).toHaveCount(0)
  await expect(page.getByRole('tab', { name: 'Basic Info' })).toBeVisible()
  await expect(page.getByRole('tab', { name: 'Watch Source' })).toBeVisible()
  await expect(page.getByRole('tab', { name: 'Sending settings' })).toBeVisible()

  await page.getByRole('tab', { name: 'Watch Source' }).click()
  await page.locator('.source-switch .el-radio-button').nth(1).click()
  await expect(page.getByPlaceholder('Enter the absolute remote directory path')).toBeVisible()
  await expect(page.getByPlaceholder('sftp.example.com')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Test SSH/SFTP Connection' })).toBeVisible()
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-realtime-editor-sftp-desktop.png'),
    fullPage: false,
  })

  await page.setViewportSize({ width: 390, height: 844 })
  await expect(page.getByPlaceholder('sftp.example.com')).toBeVisible()
  const dialogBox = await dialog.boundingBox()
  expect(dialogBox).not.toBeNull()
  expect(dialogBox!.width).toBeLessThanOrEqual(390)
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-realtime-editor-sftp-mobile.png'),
    fullPage: false,
  })
})
