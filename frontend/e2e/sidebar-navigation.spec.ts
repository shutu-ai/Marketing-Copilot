import { expect, test } from '@playwright/test'

test('sidebar navigation groups expand exclusively', async ({ page }) => {
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

  const campaign = page.getByRole('menuitem', { name: '营销活动' })
  const whitelist = page.getByRole('menuitem', { name: '白名单管理' })
  const queue = page.getByRole('menuitem', { name: '消息队列' })

  await campaign.click()
  await expect(campaign).toHaveAttribute('aria-expanded', 'true')

  await whitelist.click()
  await expect(whitelist).toHaveAttribute('aria-expanded', 'true')
  await expect(campaign).toHaveAttribute('aria-expanded', 'false')

  await queue.click()
  await expect(queue).toHaveAttribute('aria-expanded', 'true')
  await expect(whitelist).toHaveAttribute('aria-expanded', 'false')
})
