import { expect, test } from '@playwright/test'

test('sidebar campaign group expands and shows current campaign routes', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('mc_token', 'e2e-token')
    localStorage.setItem('mc_user', 'admin')
    localStorage.setItem('mc_role', 'admin')
  })

  await page.route(/^https?:\/\/[^/]+\/api\/dashboard\/stats/, route =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        total_batch_campaigns: 0,
        in_progress_campaigns: 0,
        today_sent_count: 0,
        realtime_files: 0,
        realtime_matched_users: 0,
        daily_trend: [],
        open_exception_count: 0,
        recent_exceptions: [],
      }),
    }),
  )

  await page.goto('/dashboard')

  await expect(page.locator('.el-menu-item[index="/data-monitor"]')).toHaveCount(0)
  const campaign = page.locator('.el-sub-menu').first()
  await campaign.locator(':scope > .el-sub-menu__title').click()
  await expect(campaign).toHaveClass(/is-opened/)
  await expect(campaign.locator('.el-menu-item')).toHaveCount(2)
  await expect(campaign.locator('.el-menu-item').nth(0)).toBeVisible()
  await expect(campaign.locator('.el-menu-item').nth(1)).toBeVisible()
})
