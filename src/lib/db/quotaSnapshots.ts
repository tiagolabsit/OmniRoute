import { getDbInstance, rowToCamel } from "./core";
import type { QuotaSnapshotRow, ProviderUtilizationPoint } from "@/shared/types/utilization";
import {
  hasCodexScopeCooldown,
  liftCodexScopeCooldownOnHeadroom,
} from "./providers/codexAccountState";
import { isCodexSparkQuotaKey } from "@omniroute/open-sse/config/codexQuotaScopes";

type JsonRecord = Record<string, unknown>;

interface StatementLike<TRow = unknown> {
  all: (...params: unknown[]) => TRow[];
  get: (...params: unknown[]) => TRow | undefined;
  run: (...params: unknown[]) => { changes: number };
}

interface DbLike {
  prepare: <TRow = unknown>(sql: string) => StatementLike<TRow>;
}

let lastCleanupAt = 0;

export function saveQuotaSnapshot(snapshot: Omit<QuotaSnapshotRow, "id" | "created_at">): void {
  const db = getDbInstance() as unknown as DbLike;
  const now = new Date().toISOString();

  try {
    db.prepare(
      `INSERT INTO quota_snapshots
       (provider, connection_id, window_key, remaining_percentage, is_exhausted,
        next_reset_at, window_duration_ms, raw_data, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      snapshot.provider,
      snapshot.connection_id,
      snapshot.window_key,
      snapshot.remaining_percentage,
      snapshot.is_exhausted,
      snapshot.next_reset_at,
      snapshot.window_duration_ms,
      snapshot.raw_data,
      now
    );
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      console.warn(
        "[QuotaSnapshots] Skipping save: quota_snapshots table not found. Awaiting migration."
      );
      return;
    }
    throw err;
  }

  // #12860: When a new snapshot demonstrates headroom on a Codex connection,
  // lift any fallback-sourced scope cooldown parked by quota preflight.
  maybeLiftCodexCooldownOnHeadroom(snapshot);
}

/**
 * `rowToCamel` leaves snake_case keys intact on some driver paths, so accept
 * either casing rather than trusting one shape.
 */
type SnapshotShape = Partial<QuotaSnapshotRow> & {
  windowKey?: string;
  remainingPercentage?: number;
  isExhausted?: number;
};

function snapshotHasHeadroom(s: SnapshotShape): boolean {
  const pct = s.remainingPercentage ?? s.remaining_percentage ?? 0;
  const exhausted = s.isExhausted ?? s.is_exhausted ?? 0;
  return pct > 0 && exhausted !== 1;
}

function maybeLiftCodexCooldownOnHeadroom(
  snapshot: Omit<QuotaSnapshotRow, "id" | "created_at">
): void {
  if (
    snapshot.provider?.toLowerCase() !== "codex" ||
    typeof snapshot.connection_id !== "string" ||
    snapshot.connection_id.length === 0 ||
    (snapshot.remaining_percentage ?? 0) <= 0 ||
    snapshot.is_exhausted === 1
  ) {
    return;
  }

  try {
    const scope = isCodexSparkQuotaKey(snapshot.window_key) ? "spark" : "codex";
    // The scope-wide read is only worth paying for when a cooldown is
    // actually parked on this connection+scope; the common case is clean.
    if (!hasCodexScopeCooldown(snapshot.connection_id, scope)) return;

    const scopeWindows = getLatestQuotaSnapshotsForConnection(snapshot.connection_id).filter(
      (s: SnapshotShape) => {
        const key = s.windowKey ?? s.window_key;
        return scope === "spark" ? isCodexSparkQuotaKey(key) : !isCodexSparkQuotaKey(key);
      }
    );
    // Every window in the scope must be healthy: one exhausted window still
    // justifies the cooldown even when a sibling reports full headroom.
    if (scopeWindows.length > 0 && scopeWindows.every(snapshotHasHeadroom)) {
      liftCodexScopeCooldownOnHeadroom(snapshot.connection_id, scope);
    }
  } catch (err) {
    console.debug("[QuotaSnapshots] Headroom evaluation skipped:", err);
  }
}

export function getQuotaSnapshots(opts: {
  provider?: string;
  connectionId?: string;
  since: string;
  until?: string;
}): QuotaSnapshotRow[] {
  const db = getDbInstance() as unknown as DbLike;
  const conditions: string[] = ["created_at >= ?"];
  const params: unknown[] = [opts.since];

  if (opts.provider) {
    conditions.push("provider = ?");
    params.push(opts.provider);
  }

  if (opts.connectionId) {
    conditions.push("connection_id = ?");
    params.push(opts.connectionId);
  }

  if (opts.until) {
    conditions.push("created_at <= ?");
    params.push(opts.until);
  }

  try {
    const sql = `SELECT * FROM quota_snapshots WHERE ${conditions.join(" AND ")} ORDER BY created_at ASC`;
    const rows = db.prepare(sql).all(...params);
    return rows.map((r) => rowToCamel(r) as unknown as QuotaSnapshotRow);
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return [];
    }
    throw err;
  }
}

/**
 * Returns the single latest snapshot row for each distinct `window_key`
 * ever observed for this connection.
 *
 * Deliberately NOT a "most recent N rows across all windows" query: a
 * connection with many quota windows where only a subset actively churn
 * (frequent writes as they drain) and the rest stay idle/healthy (a single
 * old row each, thanks to the #4438 no-op-write dedup) would otherwise have
 * its recent-rows slice flooded entirely by the hot windows, silently
 * evicting the idle windows from rehydration (#8431). Scoping "latest" PER
 * window_key via a window function keeps every window visible regardless of
 * how skewed the write frequency is across windows.
 */
export function getLatestQuotaSnapshotsForConnection(connectionId: string): QuotaSnapshotRow[] {
  const db = getDbInstance() as unknown as DbLike;

  try {
    const rows = db
      .prepare(
        `SELECT * FROM (
           SELECT *, ROW_NUMBER() OVER (
             PARTITION BY window_key ORDER BY created_at DESC, id DESC
           ) AS rn
           FROM quota_snapshots
           WHERE connection_id = ?
         )
         WHERE rn = 1`
      )
      .all(connectionId);

    return rows.map((row) => rowToCamel(row) as unknown as QuotaSnapshotRow);
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return [];
    }
    throw err;
  }
}

/**
 * Utilization series for the dashboard: ONE point per (bucket, provider-or-connection).
 *
 * A provider can report several quota windows at once (a 5h session plus a weekly
 * window, per-model pools, ...). The point therefore carries the TIGHTEST window of the
 * bucket (lowest remaining percentage, `windowKey` names it) and is exhausted when ANY
 * window is. Returning one row per window made consumers pick an arbitrary one, so a
 * provider whose weekly window was exhausted could be drawn from its half-empty session
 * window instead. Samples of the same window inside a bucket are still averaged first.
 *
 * Provider aggregation with several accounts: an account is a fallback for the others, so
 * the provider is as usable as its BEST account. Each account is reduced to its own
 * tightest window first; the point then takes the highest of those and is exhausted only
 * when every account is. Snapshots are persisted only when a value changes, so a bucket
 * with no row for an account means "unchanged", not "unknown": the account's last known
 * value is carried into the bucket (seeded from the last row before `since`).
 */
export function getAggregatedSnapshots(opts: {
  provider?: string;
  since: string;
  until?: string;
  bucketMinutes: number;
  aggregateBy?: "provider" | "connection";
}): ProviderUtilizationPoint[] {
  const db = getDbInstance() as unknown as DbLike;
  const conditions: string[] = ["created_at >= ?"];
  const params: unknown[] = [opts.since];

  if (opts.provider) {
    conditions.push("provider = ?");
    params.push(opts.provider);
  }

  if (opts.until) {
    conditions.push("created_at <= ?");
    params.push(opts.until);
  }

  const bucketSeconds = Number(opts.bucketMinutes) * 60;
  if (!Number.isFinite(bucketSeconds) || bucketSeconds <= 0) {
    throw new Error("Invalid bucket size");
  }

  const bucketExpr = `datetime((strftime('%s', created_at) / ${bucketSeconds}) * ${bucketSeconds}, 'unixepoch')`;

  const connectionSql = `
      WITH per_window AS (
        SELECT
          ${bucketExpr} as bucket,
          provider || ':' || connection_id as provider,
          AVG(remaining_percentage) as remainingPct,
          MAX(is_exhausted) as isExhausted,
          window_key
        FROM quota_snapshots
        WHERE ${conditions.join(" AND ")}
        GROUP BY bucket, provider, connection_id, window_key
      )
      SELECT bucket, provider, remainingPct, isExhausted, window_key AS windowKey
      FROM (
        SELECT
          bucket,
          provider,
          remainingPct,
          MAX(isExhausted) OVER (PARTITION BY bucket, provider) AS isExhausted,
          window_key,
          ROW_NUMBER() OVER (
            PARTITION BY bucket, provider
            ORDER BY remainingPct IS NULL, remainingPct ASC, window_key ASC
          ) AS rn
        FROM per_window
      )
      WHERE rn = 1
      ORDER BY bucket ASC
    `;

  const seedProviderFilter = opts.provider ? "AND provider = ?" : "";
  const providerSql = `
      WITH in_range AS (
        SELECT
          ${bucketExpr} as bucket,
          provider,
          connection_id,
          window_key,
          AVG(remaining_percentage) as remainingPct,
          MAX(is_exhausted) as isExhausted
        FROM quota_snapshots
        WHERE ${conditions.join(" AND ")}
        GROUP BY bucket, provider, connection_id, window_key
      ),
      seed AS (
        SELECT '0000-00-00 00:00:00' AS bucket, provider, connection_id, window_key,
               remaining_percentage AS remainingPct, is_exhausted AS isExhausted
        FROM (
          SELECT provider, connection_id, window_key, remaining_percentage, is_exhausted,
                 ROW_NUMBER() OVER (
                   PARTITION BY provider, connection_id, window_key ORDER BY created_at DESC
                 ) AS rn
          FROM quota_snapshots
          WHERE created_at < ? ${seedProviderFilter}
        )
        WHERE rn = 1
      ),
      observations AS (
        SELECT * FROM in_range
        UNION ALL
        SELECT * FROM seed
      ),
      buckets AS (SELECT DISTINCT bucket, provider FROM in_range),
      carried AS (
        SELECT
          b.bucket, b.provider, o.connection_id, o.window_key, o.remainingPct, o.isExhausted,
          ROW_NUMBER() OVER (
            PARTITION BY b.bucket, b.provider, o.connection_id, o.window_key
            ORDER BY o.bucket DESC
          ) AS rn
        FROM buckets b
        JOIN observations o ON o.provider = b.provider AND o.bucket <= b.bucket
      ),
      per_connection AS (
        SELECT
          bucket, provider, connection_id, remainingPct, window_key,
          MAX(isExhausted) OVER (PARTITION BY bucket, provider, connection_id) AS isExhausted,
          ROW_NUMBER() OVER (
            PARTITION BY bucket, provider, connection_id
            ORDER BY remainingPct IS NULL, remainingPct ASC, window_key ASC
          ) AS rn
        FROM carried
        WHERE rn = 1
      )
      SELECT bucket, provider, remainingPct, isExhausted, window_key AS windowKey
      FROM (
        SELECT
          bucket, provider, remainingPct, window_key,
          MIN(isExhausted) OVER (PARTITION BY bucket, provider) AS isExhausted,
          ROW_NUMBER() OVER (
            PARTITION BY bucket, provider
            ORDER BY remainingPct IS NULL, remainingPct DESC, connection_id ASC
          ) AS rn
        FROM per_connection
        WHERE rn = 1
      )
      WHERE rn = 1
      ORDER BY bucket ASC
    `;

  // The seed only needs the range start and the provider filter: rows after `until` never
  // contribute to a bucket anyway, and rows before `since` are reduced to one per window.
  const providerParams = [...params, opts.since, ...(opts.provider ? [opts.provider] : [])];

  try {
    const aggregateByConnection = opts.aggregateBy === "connection";
    const rows = db
      .prepare(aggregateByConnection ? connectionSql : providerSql)
      .all(...(aggregateByConnection ? params : providerParams)) as Array<{
      bucket: string;
      provider: string;
      remainingPct: number | null;
      isExhausted: number;
      windowKey: string;
    }>;

    return rows.map((r) => ({
      timestamp: r.bucket,
      provider: r.provider,
      remainingPct: r.remainingPct ?? 0,
      isExhausted: r.isExhausted === 1,
      windowKey: r.windowKey,
    }));
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return [];
    }
    throw err;
  }
}

export function cleanupOldSnapshots(retentionDays = 90): number {
  const now = Date.now();
  const cleanupThresholdMs = 6 * 60 * 60 * 1000;

  if (now - lastCleanupAt < cleanupThresholdMs) {
    return 0;
  }

  const db = getDbInstance() as unknown as DbLike;
  const cutoffDate = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();

  try {
    const result = db.prepare("DELETE FROM quota_snapshots WHERE created_at < ?").run(cutoffDate);
    lastCleanupAt = now;
    return result.changes;
  } catch (err: any) {
    if (err?.message?.includes("no such table")) {
      return 0;
    }
    throw err;
  }
}
