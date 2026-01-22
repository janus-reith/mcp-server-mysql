import { isMultiDbMode } from "./../config/index.js";
import { log } from "./../utils/index.js";
import SqlParser, { AST } from "node-sql-parser";
import { DenylistedTable } from "../config/index.js";

const { Parser } = SqlParser;
const parser = new Parser();

function normalizeIdent(part: string | undefined | null): string | undefined {
  if (!part) return undefined;
  return String(part).replace(/`/g, "").trim().toLowerCase();
}

export type ReferencedTable = { schema?: string; table: string };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/**
 * Extract referenced tables from a SQL query using `node-sql-parser`.
 *
 * Security note: this is best-effort and should be paired with DB-side least privilege.
 *
 * We fail-closed by throwing if parsing fails.
 */
function getReferencedTablesFromAst(ast: unknown): ReferencedTable[] {
  const out: ReferencedTable[] = [];

  const visit = (node: unknown) => {
    if (!node) return;

    // Arrays
    if (Array.isArray(node)) {
      for (const n of node) visit(n);
      return;
    }

    // Table nodes commonly look like:
    // { db: 'schema', table: 'name', as: 'alias' }
    if (isRecord(node)) {
      const maybeTable = node["table"];
      if (typeof maybeTable === "string") {
        const table = normalizeIdent(maybeTable);
        if (table) {
          const maybeDb = node["db"];
          const schema =
            typeof maybeDb === "string" ? normalizeIdent(maybeDb) : undefined;
          out.push({ schema, table });
        }
      }

      // Recurse
      for (const value of Object.values(node)) {
        visit(value);
      }
    }
  };

  visit(ast);
  // de-dupe
  const seen = new Set<string>();
  return out.filter((t) => {
    const key = `${t.schema || ""}.${t.table}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function getReferencedTables(sql: string): ReferencedTable[] {
  const astOrArray: AST | AST[] = parser.astify(sql, { database: "mysql" });
  const statements = Array.isArray(astOrArray) ? astOrArray : [astOrArray];
  // Enforce single-statement to reduce bypass surface.
  if (statements.length !== 1) {
    throw new Error("Only single-statement SQL is allowed");
  }
  return getReferencedTablesFromAst(statements[0]);
}

export function isQueryBlockedByDenylist(params: {
  sql: string;
  denylist: DenylistedTable[];
  defaultSchema?: string | null;
  multiDbMode: boolean;
}): { blocked: boolean; reason?: string } {
  const { sql, denylist, defaultSchema, multiDbMode } = params;
  if (!denylist.length) return { blocked: false };

  const referenced = getReferencedTables(sql);
  if (!referenced.length) return { blocked: false };

  // In multi-DB mode, unqualified tables are ambiguous if the query doesn't have a USE.
  // We do NOT track session state, so we fail closed for unqualified references.
  if (multiDbMode) {
    const anyUnqualified = referenced.some((t) => !t.schema);
    if (anyUnqualified) {
      return {
        blocked: true,
        reason:
          "Unqualified table references are not allowed in multi-DB mode when MYSQL_TABLE_DENYLIST is set. Use fully-qualified schema.table names.",
      };
    }
  }

  for (const t of referenced) {
    const schema = t.schema || normalizeIdent(defaultSchema) || undefined;
    const table = t.table;

    for (const deny of denylist) {
      // deny entry may be schema-specific or schema-agnostic
      if (deny.schema) {
        if (schema && deny.schema === schema && deny.table === table) {
          return {
            blocked: true,
            reason: `Access to table '${deny.schema}.${deny.table}' is blocked by MYSQL_TABLE_DENYLIST`,
          };
        }
      } else {
        if (deny.table === table) {
          return {
            blocked: true,
            reason: `Access to table '${table}' is blocked by MYSQL_TABLE_DENYLIST`,
          };
        }
      }
    }
  }

  return { blocked: false };
}

// Extract schema from SQL query
function extractSchemaFromQuery(sql: string): string | null {
  // Default schema from environment
  const defaultSchema = process.env.MYSQL_DB || null;

  // If we have a default schema and not in multi-DB mode, return it
  if (defaultSchema && !isMultiDbMode) {
    return defaultSchema;
  }

  // Try to extract schema from query

  // Case 1: USE database statement
  const useMatch = sql.match(/USE\s+`?([a-zA-Z0-9_]+)`?/i);
  if (useMatch && useMatch[1]) {
    return useMatch[1];
  }

  // Case 2: database.table notation
  const dbTableMatch = sql.match(/`?([a-zA-Z0-9_]+)`?\.`?[a-zA-Z0-9_]+`?/i);
  if (dbTableMatch && dbTableMatch[1]) {
    return dbTableMatch[1];
  }

  // Return default if we couldn't find a schema in the query
  return defaultSchema;
}

async function getQueryTypes(query: string): Promise<string[]> {
  try {
    log("info", "Parsing SQL query: ", query);
    // Parse into AST or array of ASTs - only specify the database type
    const astOrArray: AST | AST[] = parser.astify(query, { database: "mysql" });
    const statements = Array.isArray(astOrArray) ? astOrArray : [astOrArray];

    // Enforce single-statement at the parser layer as well.
    if (statements.length !== 1) {
      throw new Error("Only single-statement SQL is allowed");
    }

    // Map each statement to its lowercased type (e.g., 'select', 'update', 'insert', 'delete', etc.)
    return statements.map((stmt) => stmt.type?.toLowerCase() ?? "unknown");
  } catch (err: any) {
    log("error", "sqlParser error, query: ", query);
    log("error", "Error parsing SQL query:", err);
    throw new Error(`Parsing failed: ${err.message}`);
  }
}

export { extractSchemaFromQuery, getQueryTypes };
