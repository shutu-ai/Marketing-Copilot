import { expect, test } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

test('dashboard trend keeps bars proportional and moves values into the day tooltip', async ({ page }) => {
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

  await page.route(/^https?:\/\/[^/]+\/api\/dashboard\/stats/, route =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        pipeline: 'vnext',
        total_batch_campaigns: 1,
        in_progress_campaigns: 1,
        in_progress_batch_campaigns: 1,
        in_progress_realtime_campaigns: 0,
        today_completed_campaigns: 0,
        today_sent_count: 900,
        total_sms_campaigns: 0,
        realtime_campaigns: 0,
        today_realtime_files: 0,
        today_realtime_event_users: 0,
        today_realtime_matched_users: 0,
        today_open_exception_count: 0,
        today_recent_exceptions: [],
        daily_trend: [
          { stats_date: '2026-10-19', sent_count: 900, matched_count: 300, received_file_count: 30, conversion_count: 0 },
          { stats_date: '2026-10-20', sent_count: 450, matched_count: 150, received_file_count: 15, conversion_count: 0 },
        ],
      }),
    }),
  )

  await page.goto('/dashboard')
  await expect(page).toHaveURL(/\/dashboard$/)
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible()
  await expect(page.locator('.bar-chart__item')).toHaveCount(2)

  const firstBars = page.locator('.bar-chart__item').first().locator('.trend-series__fill')
  const secondBars = page.locator('.bar-chart__item').nth(1).locator('.trend-series__fill')
  const firstHeights = await firstBars.evaluateAll(items => items.map(item => item.getBoundingClientRect().height))
  const secondHeights = await secondBars.evaluateAll(items => items.map(item => item.getBoundingClientRect().height))

  expect(firstHeights[0]).toBeLessThan(firstHeights[1])
  expect(firstHeights[1]).toBeLessThan(firstHeights[2])
  expect(secondHeights[0]).toBeLessThan(secondHeights[1])
  expect(secondHeights[1]).toBeLessThan(secondHeights[2])
  expect(secondHeights.every((height, index) => height < firstHeights[index])).toBe(true)
  await expect(page.locator('.trend-series__value')).toHaveCount(0)

  await page.locator('.bar-chart__item').first().hover()
  await expect(page.locator('.trend-tooltip').first()).toContainText('900')
  await expect(page.locator('.trend-tooltip').first()).toContainText('300')
  await expect(page.locator('.trend-tooltip').first()).toContainText('30')
  expect(errors).toEqual([])
  await page.screenshot({
    path: path.join(os.tmpdir(), 'marketing-copilot-dashboard-trend.png'),
    fullPage: false,
  })
})
