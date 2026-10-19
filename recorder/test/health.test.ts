import assert from "node:assert/strict";
import { test } from "node:test";
import { type Outcome, heartbeats } from "../src/health.ts";

const o = (feed: string, result: Outcome["result"], detail?: string): Outcome => ({ city: "sofia", feed, result, detail });

test("one ping per feed: success if anything was saved, /log if nothing was", () => {
  const pings = heartbeats("KEY", [
    o("vehicle-positions", "saved"),
    o("vehicle-positions", "error", "Error: HTTP 503"),
    o("vehicle-positions", "saved"),
    o("trip-updates", "error", "TimeoutError: timed out"),
    o("alerts", "lost", "R2 put failed"),
  ]);
  assert.deepEqual(
    pings.map((p) => p.url),
    [
      "https://hc-ping.com/KEY/sofia-vehicle-positions?create=1",
      "https://hc-ping.com/KEY/sofia-trip-updates/log?create=1",
      "https://hc-ping.com/KEY/sofia-alerts/log?create=1",
    ],
  );
  assert.equal(pings[0].body, "2/3 snapshots saved\nerror: Error: HTTP 503");
  assert.equal(pings[2].body, "0/1 snapshots saved\nlost: R2 put failed");
});

test("repeated errors are listed once and the body stays small", () => {
  const [p] = heartbeats("K", Array.from({ length: 50 }, () => o("f", "error", "HTTP 503")));
  assert.equal(p.body, "0/50 snapshots saved\nerror: HTTP 503");
  const [long] = heartbeats("K", [o("f", "error", "x".repeat(5000))]);
  assert.equal(long.body.length, 1000);
});
