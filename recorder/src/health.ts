// Per-feed heartbeats for healthchecks.io, kept pure so it can be unit tested.

export type Result = "saved" | "error" | "lost";

export interface Outcome {
  city: string;
  feed: string;
  // saved: snapshot stored. error: fetch failed, error record stored. lost: nothing stored.
  result: Result;
  detail?: string;
}

export interface Ping {
  url: string;
  body: string;
}

const PING_BASE = "https://hc-ping.com";

// One ping per city and feed that ran in this invocation: a success ping if at least one snapshot
// of it was stored in R2, otherwise a /log ping, which records the errors without changing the
// check's state. So a single bad minute is visible but quiet, and the check goes down only when
// no snapshot has been stored for its whole period plus grace. Slugs are "<city>-<feed>";
// create=1 makes healthchecks.io add a check the first time a slug pings, so a new city or feed
// is never silently unmonitored.
export function heartbeats(pingKey: string, outcomes: Outcome[]): Ping[] {
  const groups = new Map<string, Outcome[]>();
  for (const o of outcomes) {
    const slug = `${o.city}-${o.feed}`;
    groups.set(slug, [...(groups.get(slug) ?? []), o]);
  }
  return [...groups].map(([slug, os]) => {
    const saved = os.filter((o) => o.result === "saved").length;
    const problems = [...new Set(os.filter((o) => o.detail).map((o) => `${o.result}: ${o.detail}`))];
    return {
      url: `${PING_BASE}/${pingKey}/${slug}${saved ? "" : "/log"}?create=1`,
      body: [`${saved}/${os.length} snapshots saved`, ...problems].join("\n").slice(0, 1000),
    };
  });
}
