import assert from "node:assert/strict";
import { test } from "node:test";
import { type City, errorKey, jobsFor, rawKey } from "../src/schedule.ts";

const city: City = {
  id: "testcity",
  realtime: [
    { feed: "vehicle-positions", url: "https://example/vp", every_minutes: 1, offsets_s: [0, 15, 30, 45] },
    { feed: "trip-updates", url: "https://example/tu", every_minutes: 2, offsets_s: [5] },
    { feed: "alerts", url: "https://example/al", every_minutes: 5, offsets_s: [10] },
  ],
};

const at = (iso: string) => Date.parse(iso);

test("every minute fetches positions four times, 15 s apart", () => {
  const jobs = jobsFor(at("2026-10-14T19:01:00.250Z"), [city]);
  assert.deepEqual(
    jobs.map((j) => [j.feed.feed, new Date(j.at).toISOString()]),
    [
      ["vehicle-positions", "2026-10-14T19:01:00.000Z"],
      ["vehicle-positions", "2026-10-14T19:01:15.000Z"],
      ["vehicle-positions", "2026-10-14T19:01:30.000Z"],
      ["vehicle-positions", "2026-10-14T19:01:45.000Z"],
    ],
  );
});

test("trip updates on even minutes, alerts every five", () => {
  const feeds = (iso: string) => [...new Set(jobsFor(at(iso), [city]).map((j) => j.feed.feed))];
  assert.deepEqual(feeds("2026-10-14T19:02:00Z"), ["vehicle-positions", "trip-updates"]);
  assert.deepEqual(feeds("2026-10-14T19:05:00Z"), ["vehicle-positions", "alerts"]);
  assert.deepEqual(feeds("2026-10-14T19:10:00Z"), ["vehicle-positions", "trip-updates", "alerts"]);
});

test("keys are UTC date and hour partitioned", () => {
  const t = at("2026-10-14T23:59:59.123Z");
  assert.equal(rawKey("sofia", "alerts", t), `raw/sofia/alerts/2026-10-14/23/${t}.pb`);
  assert.equal(errorKey("sofia", "alerts", t), `errors/sofia/2026-10-14/${t}-alerts.json`);
});
