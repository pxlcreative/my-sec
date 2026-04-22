import { defineConfig, devices } from '@playwright/test'

/**
 * Playwright E2E smoke tests.
 * Starts a Vite dev server (proxied to http://localhost:8000 for the API)
 * and runs one happy-path flow. The dev server + API must be running (via
 * `make up && cd frontend && npm run dev`) before you invoke `npm run test:e2e`.
 */
export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  expect: { timeout: 5_000 },
  fullyParallel: false,
  retries: 0,
  reporter: 'list',
  use: {
    baseURL: 'http://localhost:5173',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
})
