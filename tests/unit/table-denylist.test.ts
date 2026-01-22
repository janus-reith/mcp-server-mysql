import { describe, it, expect } from "vitest";
import { isQueryBlockedByDenylist } from "../../src/db/utils.js";

describe("MYSQL_TABLE_DENYLIST enforcement", () => {
  it("blocks a direct SELECT from a denylisted fully-qualified table", () => {
    const res = isQueryBlockedByDenylist({
      sql: "SELECT * FROM prod.users",
      denylist: [{ schema: "prod", table: "users" }],
      defaultSchema: null,
      multiDbMode: true,
    });
    expect(res.blocked).toBe(true);
  });

  it("blocks JOINs that reference a denylisted table", () => {
    const res = isQueryBlockedByDenylist({
      sql: "SELECT * FROM app.orders o JOIN app.users u ON u.id = o.user_id",
      denylist: [{ schema: "app", table: "users" }],
      defaultSchema: null,
      multiDbMode: false,
    });
    expect(res.blocked).toBe(true);
  });

  it("blocks unqualified table references in multi-DB mode when denylist is set", () => {
    const res = isQueryBlockedByDenylist({
      sql: "SELECT * FROM users",
      denylist: [{ schema: "prod", table: "users" }],
      defaultSchema: null,
      multiDbMode: true,
    });
    expect(res.blocked).toBe(true);
    expect(res.reason).toContain("Unqualified table references");
  });

  it("allows unqualified table references in single-DB mode when not denylisted", () => {
    const res = isQueryBlockedByDenylist({
      sql: "SELECT * FROM orders",
      denylist: [{ schema: "prod", table: "users" }],
      defaultSchema: "prod",
      multiDbMode: false,
    });
    expect(res.blocked).toBe(false);
  });
});
