import { expect, test } from '@playwright/test'
import { readFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

test('batch prospect template uses an importer-compatible language value', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
    localStorage.setItem('locale', 'en')
  })

  const campaign = {
    id: 7,
    name: 'Batch QA',
    start_time: '2026-10-18 08:00:00',
    end_time: '2026-10-20 18:00:00',
    send_start_time: '08:00',
    send_end_time: '18:00',
    send_days: ['1', '2', '3', '4', '5'],
    status: 'not_started',
    batch_size: 500,
    customer_list_filename: '',
    customer_list_path: '',
    row_count: 0,
    campaign_dir: '',
    use_custom_directory: false,
    sms_template_id: null,
    smpp_config_id: null,
    flash_sms: false,
    phone_prefix: '251',
    created_at: '2026-10-18 08:00:00',
  }

  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const url = new URL(route.request().url())
    const body = url.pathname === '/api/batch-campaigns'
      ? { total: 1, page: 1, page_size: 50, items: [campaign] }
      : url.pathname === '/api/smpp/configs' ? [] : {}
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(body),
    })
  })

  await page.goto('/campaign/batch')
  await expect(page).toHaveURL(/\/campaign\/batch$/)
  await expect(page.getByText('Batch QA')).toBeVisible()

  await page.getByRole('button', { name: 'More' }).click()
  await page.getByRole('menuitem', { name: 'Upload Target List' }).click()
  const downloadPromise = page.waitForEvent('download')
  await page.getByRole('button', { name: 'Download Prospect Template' }).click()
  const download = await downloadPromise
  const downloadPath = await download.path()
  expect(downloadPath).not.toBeNull()
  const content = await readFile(downloadPath!, 'utf8')

  expect(content).toBe('msisdn,region,language\n66812345678,TH,English\n')
  expect(content).not.toContain(',th\n')
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-batch-template-download.png'),
    fullPage: false,
  })
})

test('batch campaign saves only the configured phone prefix', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
    localStorage.setItem('locale', 'en')
  })

  let updateBody: Record<string, unknown> | null = null
  let campaign: Record<string, any> = {
    id: 7,
    name: 'Batch QA',
    start_time: '2026-10-18 08:00:00',
    end_time: '2026-10-20 18:00:00',
    send_start_time: '08:00',
    send_end_time: '18:00',
    send_days: ['1', '2', '3', '4', '5'],
    status: 'not_started',
    batch_size: 500,
    customer_list_filename: '',
    customer_list_path: '',
    row_count: 0,
    campaign_dir: '',
    use_custom_directory: false,
    sms_template_id: null,
    smpp_config_id: null,
    flash_sms: false,
    phone_prefix: '251',
    created_at: '2026-10-18 08:00:00',
  }

  await page.route(/^https?:\/\/[^/]+\/api\//, async route => {
    const request = route.request()
    const url = new URL(request.url())
    const json = (body: unknown) => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(body),
    })

    if (url.pathname === '/api/batch-campaigns' && request.method() === 'GET') {
      return json({ total: 1, page: 1, page_size: 50, items: [campaign] })
    }
    if (url.pathname === '/api/smpp/configs') return json([])
    if (url.pathname === '/api/batch-campaigns/7' && request.method() === 'PUT') {
      updateBody = JSON.parse(request.postData() || '{}')
      campaign = { ...campaign, ...updateBody }
      return json(campaign)
    }
    return json({})
  })

  await page.goto('/campaign/batch')
  await expect(page.getByText('Batch QA')).toBeVisible()
  await page.getByRole('button', { name: 'More' }).click()
  await page.getByRole('menuitem', { name: 'Edit' }).click()

  const dialog = page.getByRole('dialog')
  const prefixInput = dialog.locator('input[placeholder="251"]')
  await expect(prefixInput).toHaveValue('251')
  await prefixInput.fill('251980665666')
  await expect(prefixInput).toHaveValue('251980665666')
  await prefixInput.fill('251')
  await expect(prefixInput).toHaveValue('251')
  await dialog.getByRole('button', { name: 'Save' }).click()

  expect(updateBody?.phone_prefix).toBe('251')
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-batch-phone-prefix.png'),
    fullPage: false,
  })
})
