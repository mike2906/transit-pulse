// Pure scheduling and key-naming logic, kept apart from the Worker so it can be unit tested.

export interface RealtimeFeed {
  feed: string;
  url: string;
  every_minutes: number;
  offsets_s: number[];
}

export interface City {
  id: string;
  realtime: RealtimeFeed[];
}

export interface Job {
  city: string;
  feed: RealtimeFeed;
  at: number; // epoch ms the fetch should start
}

// Which fetches belong to the cron firing at `scheduledTime`.
export function jobsFor(scheduledTime: number, cities: City[]): Job[] {
  const minuteStart = scheduledTime - (scheduledTime % 60_000);
  const minuteOfDay = Math.floor((minuteStart % 86_400_000) / 60_000);
  const jobs: Job[] = [];
  for (const city of cities) {
    for (const feed of city.realtime) {
      if (minuteOfDay % feed.every_minutes !== 0) continue;
      for (const offset of feed.offsets_s) {
        jobs.push({ city: city.id, feed, at: minuteStart + offset * 1000 });
      }
    }
  }
  return jobs;
}

export function rawKey(city: string, feed: string, fetchedAt: number): string {
  const iso = new Date(fetchedAt).toISOString(); // 2026-10-14T19:46:28.057Z
  return `raw/${city}/${feed}/${iso.slice(0, 10)}/${iso.slice(11, 13)}/${fetchedAt}.pb`;
}

export function errorKey(city: string, feed: string, fetchedAt: number): string {
  const iso = new Date(fetchedAt).toISOString();
  return `errors/${city}/${iso.slice(0, 10)}/${fetchedAt}-${feed}.json`;
}
