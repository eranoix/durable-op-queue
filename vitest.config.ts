import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Each test opens its own in-memory database, so they cannot interfere and
    // there is nothing to clean up between runs.
    environment: 'node',
    include: ['tests/**/*.test.ts'],
  },
});
