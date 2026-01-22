import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/integration/**/*.test.ts"],
    environment: "node",
    // Integration tests expect a running MySQL.
    testTimeout: 60_000,
  },
});
