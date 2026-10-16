import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omni-db-quota-snapshots-"));
process.env.DATA_DIR = TEST_DATA_DIR;

const coreDb = await import("../../src/lib/db/core.ts");
const quotaSnapshotsDb = await import("../../src/lib/db/quotaSnapshots.ts");

async function resetStorage() {
  coreDb.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  fs.mkdirSync(TEST_DATA_DIR, { recursive: true });
}

test.beforeEach(async () => {
  await resetStorage();
});

test.after(() => {
  coreDb.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

test("quotaSnapshots save and query rows with provider and connection filters", () => {
  quotaSnapshotsDb.saveQuotaSnapshot({
    provider: "openai",
    connection_id: "conn-1",
    window_key: "hourly",
    remaining_percentage: 60,
    is_exhausted: 0,
    next_reset_at: "2026-01-01T01:00:00.000Z",
    window_duration_ms: 3600000,
    raw_data: JSON.stringify({ source: "first" }),
  });
  quotaSnapshotsDb.saveQuotaSnapshot({
    provider: "anthropic",
    connection_id: "conn-2",
    window_key: "daily",
    remaining_percentage: 30,
    is_exhausted: 1,
    next_reset_at: "2026-01-02T00:00:00.000Z",
    window_duration_ms: 86400000,
    raw_data: JSON.stringify({ source: "second" }),
  });

  const openaiRows = quotaSnapshotsDb.getQuotaSnapshots({
    provider: "openai",
    connectionId: "conn-1",
    since: "2000-01-01T00:00:00.000Z",
  });

  assert.equal(openaiRows.length, 1);
  assert.equal(openaiRows[0].provider, "openai");
  assert.equal(openaiRows[0].connectionId, "conn-1");
});

test("quotaSnapshots aggregates by provider or connection and rejects invalid buckets", () => {
  quotaSnapshotsDb.saveQuotaSnapshot({
    provider: "openai",
    connection_id: "conn-a",
    window_key: "hourly",
    remaining_percentage: 50,
    is_exhausted: 0,
    next_reset_at: "2026-01-01T01:00:00.000Z",
    window_duration_ms: 3600000,
    raw_data: "{}",
  });
  quotaSnapshotsDb.saveQuotaSnapshot({
    provider: "openai",
    connection_id: "conn-a",
    window_key: "hourly",
    remaining_percentage: 70,
    is_exhausted: 0,
    next_reset_at: "2026-01-01T01:10:00.000Z",
    window_duration_ms: 3600000,
    raw_data: "{}",
  });

  const providerAgg = quotaSnapshotsDb.getAggregatedSnapshots({
    provider: "openai",
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
  });
  const connectionAgg = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
    aggregateBy: "connection",
  });

  assert.equal(providerAgg.length, 1);
  assert.equal(providerAgg[0].remainingPct, 60);
  assert.equal(connectionAgg[0].provider, "openai:conn-a");

  assert.throws(
    () =>
      quotaSnapshotsDb.getAggregatedSnapshots({
        since: "2000-01-01T00:00:00.000Z",
        bucketMinutes: 0,
      }),
    /Invalid bucket size/
  );
});

test("quotaSnapshots cleanup removes old rows and throttles repeated execution", () => {
  const db = coreDb.getDbInstance();
  db.prepare(
    `
    INSERT INTO quota_snapshots
    (provider, connection_id, window_key, remaining_percentage, is_exhausted, next_reset_at, window_duration_ms, raw_data, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `
  ).run(
    "openai",
    "old-conn",
    "hourly",
    20,
    1,
    "2000-01-01T01:00:00.000Z",
    3600000,
    "{}",
    "2000-01-01T00:00:00.000Z"
  );

  const deleted = quotaSnapshotsDb.cleanupOldSnapshots(1);
  const throttled = quotaSnapshotsDb.cleanupOldSnapshots(1);

  assert.equal(deleted, 1);
  assert.equal(throttled, 0);
});

test("quotaSnapshots aggregation returns one row per bucket with the tightest window", () => {
  const db = coreDb.getDbInstance();
  const insert = db.prepare(
    `INSERT INTO quota_snapshots
     (provider, connection_id, window_key, remaining_percentage, is_exhausted, next_reset_at, window_duration_ms, raw_data, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );
  const row = (windowKey: string, remaining: number, exhausted: number, createdAt: string) =>
    insert.run("claude", "conn-1", windowKey, remaining, exhausted, null, null, "{}", createdAt);

  // 10:00 bucket: the 5h session is almost full but the weekly window is exhausted.
  row("session (5h)", 97, 0, "2026-01-01T10:05:00.000Z");
  row("weekly (7d)", 20, 0, "2026-01-01T10:10:00.000Z");
  row("weekly (7d)", 0, 1, "2026-01-01T10:40:00.000Z");
  // 11:00 bucket: both windows have headroom, the weekly one is still the tightest.
  row("session (5h)", 90, 0, "2026-01-01T11:05:00.000Z");
  row("weekly (7d)", 40, 0, "2026-01-01T11:10:00.000Z");

  const byProvider = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
  });

  assert.equal(byProvider.length, 2, "one row per bucket, not one per quota window");
  assert.deepEqual(
    byProvider.map((point) => point.timestamp),
    ["2026-01-01 10:00:00", "2026-01-01 11:00:00"]
  );
  // Window samples are still averaged inside a bucket (weekly: (20 + 0) / 2 = 10), then the tightest wins.
  assert.equal(byProvider[0].remainingPct, 10);
  assert.equal(byProvider[0].isExhausted, true);
  assert.equal(byProvider[0].windowKey, "weekly (7d)");
  assert.equal(byProvider[1].remainingPct, 40);
  assert.equal(byProvider[1].isExhausted, false);
  assert.equal(byProvider[1].windowKey, "weekly (7d)");

  const byConnection = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
    aggregateBy: "connection",
  });
  assert.equal(byConnection.length, 2);
  assert.equal(byConnection[0].provider, "claude:conn-1");
  assert.equal(byConnection[0].remainingPct, 10);
});

function insertSnapshotRow(
  provider: string,
  connectionId: string,
  windowKey: string,
  remaining: number,
  exhausted: number,
  createdAt: string
) {
  coreDb
    .getDbInstance()
    .prepare(
      `INSERT INTO quota_snapshots
       (provider, connection_id, window_key, remaining_percentage, is_exhausted, next_reset_at, window_duration_ms, raw_data, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .run(provider, connectionId, windowKey, remaining, exhausted, null, null, "{}", createdAt);
}

test("provider aggregation is not exhausted while another account of the provider has headroom", () => {
  // Account A is spent (weekly window at 0) and re-snapshots in every bucket. Account B is healthy but
  // snapshots are only persisted when a value changes, so B has a row in the first bucket only.
  insertSnapshotRow("claude", "conn-a", "weekly (7d)", 0, 1, "2026-01-01T10:05:00.000Z");
  insertSnapshotRow("claude", "conn-b", "session (5h)", 96, 0, "2026-01-01T10:10:00.000Z");
  insertSnapshotRow("claude", "conn-b", "weekly (7d)", 100, 0, "2026-01-01T10:10:00.000Z");
  insertSnapshotRow("claude", "conn-a", "weekly (7d)", 0, 1, "2026-01-01T11:05:00.000Z");
  insertSnapshotRow("claude", "conn-a", "weekly (7d)", 0, 1, "2026-01-01T12:05:00.000Z");

  const points = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
  });

  assert.deepEqual(
    points.map((point) => point.timestamp),
    ["2026-01-01 10:00:00", "2026-01-01 11:00:00", "2026-01-01 12:00:00"]
  );
  for (const point of points) {
    assert.equal(point.isExhausted, false, `${point.timestamp}: the healthy account keeps it usable`);
    assert.equal(point.remainingPct, 96, `${point.timestamp}: tightest window of the best account`);
    assert.equal(point.windowKey, "session (5h)");
  }
});

test("provider aggregation is exhausted only when every account of the provider is", () => {
  insertSnapshotRow("claude", "conn-a", "weekly (7d)", 0, 1, "2026-01-01T10:05:00.000Z");
  insertSnapshotRow("claude", "conn-b", "session (5h)", 0, 1, "2026-01-01T10:10:00.000Z");
  insertSnapshotRow("claude", "conn-b", "weekly (7d)", 40, 0, "2026-01-01T10:10:00.000Z");

  const [point] = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
  });

  assert.equal(point.isExhausted, true);
  assert.equal(point.remainingPct, 0);
});

test("provider aggregation carries an unchanged account in from before the requested range", () => {
  // B was last observed two days before the range starts and has not changed since.
  insertSnapshotRow("claude", "conn-b", "weekly (7d)", 88, 0, "2025-12-30T08:00:00.000Z");
  insertSnapshotRow("claude", "conn-a", "weekly (7d)", 0, 1, "2026-01-01T10:05:00.000Z");

  const [point] = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2026-01-01T00:00:00.000Z",
    bucketMinutes: 60,
  });

  assert.equal(point.isExhausted, false);
  assert.equal(point.remainingPct, 88);
});

test("provider aggregation keeps providers separate and leaves connection aggregation untouched", () => {
  insertSnapshotRow("claude", "conn-a", "weekly (7d)", 0, 1, "2026-01-01T10:05:00.000Z");
  insertSnapshotRow("claude", "conn-b", "weekly (7d)", 70, 0, "2026-01-01T10:10:00.000Z");
  insertSnapshotRow("codex", "conn-c", "weekly (7d)", 10, 0, "2026-01-01T10:15:00.000Z");

  const byProvider = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
  });
  assert.deepEqual(
    byProvider.map((point) => [point.provider, point.remainingPct, point.isExhausted]),
    [
      ["claude", 70, false],
      ["codex", 10, false],
    ]
  );

  const byConnection = quotaSnapshotsDb.getAggregatedSnapshots({
    since: "2000-01-01T00:00:00.000Z",
    bucketMinutes: 60,
    aggregateBy: "connection",
  });
  assert.deepEqual(
    byConnection.map((point) => [point.provider, point.remainingPct, point.isExhausted]),
    [
      ["claude:conn-a", 0, true],
      ["claude:conn-b", 70, false],
      ["codex:conn-c", 10, false],
    ]
  );
});
