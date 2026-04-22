/// <reference types="vitest" />
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: './src/test/setup.ts',
    css: false,
    // Vitest owns unit/component tests under src/. Playwright owns e2e/.
    include: ['src/**/*.test.{ts,tsx}'],
    exclude: ['node_modules', 'e2e', 'dist'],
  },
})
