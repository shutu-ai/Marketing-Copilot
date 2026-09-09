import { defineConfig } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

const port = process.env.PLAYWRIGHT_PORT ?? '4176'

export default defineConfig({
  testDir: './e2e',
  testIgnore: '**/fullstack-green.spec.ts',
  timeout: 30_000,
  workers: 1,
  reporter: 'line',
  outputDir: path.join(os.tmpdir(), 'marketing-copilot-playwright-results'),
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    headless: true,
    viewport: { width: 1440, height: 900 },
    launchOptions: process.env.PLAYWRIGHT_EXECUTABLE_PATH
      ? { executablePath: process.env.PLAYWRIGHT_EXECUTABLE_PATH }
      : {},
  },
  webServer: {
    command: `npm run dev -- --host 127.0.0.1 --port ${port}`,
    url: `http://127.0.0.1:${port}/operations`,
    reuseExistingServer: true,
  },
})
