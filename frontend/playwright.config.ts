import { defineConfig } from '@playwright/test'
import os from 'node:os'
import path from 'node:path'

export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  workers: 1,
  reporter: 'line',
  outputDir: path.join(os.tmpdir(), 'marketing-copilot-playwright-results'),
  use: {
    baseURL: 'http://127.0.0.1:4174',
    headless: true,
    viewport: { width: 1440, height: 900 },
    launchOptions: process.env.PLAYWRIGHT_EXECUTABLE_PATH
      ? { executablePath: process.env.PLAYWRIGHT_EXECUTABLE_PATH }
      : {},
  },
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 4174',
    url: 'http://127.0.0.1:4174/operations',
    reuseExistingServer: true,
  },
})
