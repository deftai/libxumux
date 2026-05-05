import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/*.test.ts', 'src/**/*.spec.ts'],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: [
        'src/index.ts',
        'src/transports/index.ts',
        // Type-only files (no executable code)
        'src/codec/frame.ts',
        'src/control/messages.ts',
        // Re-export barrels (no logic)
        'src/codec/index.ts',
        'src/control/index.ts',
        '**/*.test.ts',
        '**/*.spec.ts',
      ],
      thresholds: {
        lines: 95,
        functions: 95,
        branches: 85,
        statements: 95,
      },
    },
  },
});
