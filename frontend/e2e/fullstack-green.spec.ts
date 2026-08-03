import { expect, test } from '@playwright/test'

test('frontend works with the real backend', async ({ page, request }) => {
  const api = 'http://127.0.0.1:8017/api', name = `E2E ${Date.now()}`
  await request.put(`${api}/settings/system_mode`, { data: { value: 'development' } })
  const preview = await (await request.post(`${api}/whitelist/preview`, { multipart: { file: { name: 'w.csv', mimeType: 'text/csv', buffer: Buffer.from('msisdn|region|language\n900009911|AA|English\n') } } })).json()
  await request.post(`${api}/whitelist/import`, { form: { file_path: preview.file_path, import_mode: 'append' } })
  const now = new Date(), iso = (d: number) => { const v = new Date(now); v.setDate(v.getDate() + d); return v.toISOString().slice(0, 19) }
  const campaign = await (await request.post(`${api}/batch-campaigns`, { data: { name, start_time: iso(-1), end_time: iso(1), send_start_time: '00:00', send_end_time: '23:59', send_days: [], batch_size: 10 } })).json()
  const contentCampaign = await (await request.post(`${api}/sms-content/campaigns`, { data: { name, marketing_type: 'batch', marketing_campaign_id: campaign.id } })).json()
  const template = await (await request.post(`${api}/sms-content/templates`, { data: { campaign_id: contentCampaign.id, language: 'English', content: 'E2E hello {msisdn}' } })).json()
  await request.post(`${api}/batch-campaigns/${campaign.id}/upload`, { multipart: { file: { name: 'c.csv', mimeType: 'text/csv', buffer: Buffer.from('msisdn\n900009911\n') } } })
  await request.put(`${api}/batch-campaigns/${campaign.id}`, { data: { sms_template_id: template.id } })
  await request.put(`${api}/batch-campaigns/${campaign.id}`, { data: { status: 'in_progress' } })
  await expect.poll(async () => {
    const data = await (await request.get(`${api}/sms-tasks`)).json()
    return data.items.find((item: any) => item.campaign_name === name)?.status
  }, { timeout: 30_000 }).toBe('delivered')
  await page.goto('/sms-tasks')
  const table = page.locator('.el-table')
  await expect(table.getByText(name, { exact: true })).toBeVisible()
  await expect(table.getByText('251900009911', { exact: true })).toBeVisible()
  await expect(table.getByText('delivered', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: '排除名单' }).click()
  const dialog = page.getByRole('dialog', { name: '排除名单' })
  await dialog.getByLabel('手机号').fill('251900009922')
  await dialog.getByLabel('原因').fill('e2e')
  await dialog.getByRole('button', { name: '添加' }).click()
  await expect(dialog.locator('.el-table').getByText('251900009922', { exact: true })).toBeVisible()
  await dialog.getByRole('button', { name: '删除' }).click()
  await expect(dialog.getByText('251900009922', { exact: true })).toHaveCount(0)
})
