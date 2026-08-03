import { defineConfig } from '@playwright/test'
export default defineConfig({
  testDir: './e2e', testMatch: 'fullstack-green.spec.ts', timeout: 60_000, workers: 1, reporter: 'line',
  use: { baseURL: 'http://127.0.0.1:4182', headless: true, viewport: { width: 1440, height: 900 } },
  webServer: [
    { command: '..\\.p0-test-venv\\Scripts\\python.exe -m tools.e2e_fullstack_server --port 8017', cwd: '../backend', url: 'http://127.0.0.1:8017/api/ready', reuseExistingServer: false },
    { command: 'set VITE_API_BASE=http://127.0.0.1:8017/api&& npm run dev -- --host 127.0.0.1 --port 4182', url: 'http://127.0.0.1:4182/sms-tasks', reuseExistingServer: false },
  ],
})
