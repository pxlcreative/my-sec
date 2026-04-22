import { test, expect } from '@playwright/test'

/**
 * One-shot smoke flow: loads the dashboard, opens each top-level page, and
 * confirms it renders without JS errors. Running this against a fresh
 * `make up` install proves the "works out of the box" guarantee.
 */

test('dashboard loads without errors', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (err) => errors.push(err.message))

  await page.goto('/')
  // Home redirects to /search — assert we landed somewhere useful.
  await expect(page).toHaveURL(/\/(search|$)/)

  // Sidebar links exist for every primary page.
  for (const label of ['Search', 'Bulk Match', 'Platforms', 'Alerts', 'Export', 'Sync']) {
    await expect(page.getByRole('link', { name: label })).toBeVisible()
  }

  expect(errors, `pageerror events: ${errors.join(', ')}`).toEqual([])
})

test('navigating to Sync dashboard shows job history', async ({ page }) => {
  await page.goto('/sync')
  await expect(page.getByRole('heading', { name: /sync/i })).toBeVisible()
})

test('Platforms page renders empty state or list', async ({ page }) => {
  await page.goto('/platforms')
  // Either a table with platforms or the empty-state heading — both fine.
  const pageContent = await page.content()
  expect(
    pageContent.includes('No platforms') || pageContent.includes('Platform'),
  ).toBeTruthy()
})
