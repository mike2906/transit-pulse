// Records GTFS-Realtime snapshots into R2. Deliberately does no parsing: the free plan allows
// 10 ms CPU per invocation, so each snapshot is fetched and stored as raw bytes, and all
// decoding and deduplication happens in the daily pipeline.
import sofia from "../../cities/sofia.json";
import { type City, type Job, errorKey, jobsFor, rawKey } from "./schedule.ts";

export interface Env {
  RAW: R2Bucket;
}

const CITIES: City[] = [sofia];
const USER_AGENT = "transit-pulse (+https://github.com/mike2906/transit-pulse)";
const FETCH_TIMEOUT_MS = 10_000;

async function record(env: Env, job: Job): Promise<void> {
  const wait = job.at - Date.now();
  if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));

  const started = Date.now();
  let status = 0;
  let error = "";
  try {
    const res = await fetch(job.feed.url, {
      headers: { "User-Agent": USER_AGENT },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    status = res.status;
    const body = await res.arrayBuffer();
    if (res.ok && body.byteLength > 0) {
      await env.RAW.put(rawKey(job.city, job.feed.feed, started), body, {
        httpMetadata: { contentType: "application/x-protobuf" },
        customMetadata: {
          status: String(status),
          fetch_ms: String(Date.now() - started),
          date: res.headers.get("date") ?? "",
        },
      });
      return;
    }
    error = res.ok ? "empty body" : `HTTP ${status}`;
  } catch (e) {
    error = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
  }
  await env.RAW.put(
    errorKey(job.city, job.feed.feed, started),
    JSON.stringify({ city: job.city, feed: job.feed.feed, url: job.feed.url, at: started, status, error }),
    { httpMetadata: { contentType: "application/json" } },
  );
}

export default {
  // Awaited rather than passed to waitUntil: a cron invocation may run for up to 15 minutes of
  // wall time, and the sleeps between fetches cost no CPU.
  async scheduled(controller: ScheduledController, env: Env): Promise<void> {
    const jobs = jobsFor(controller.scheduledTime, CITIES);
    await Promise.allSettled(jobs.map((job) => record(env, job)));
  },
} satisfies ExportedHandler<Env>;
