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
  // Minutes since the epoch, so cadences that don't divide a day stay even across midnight.
  const minute = minuteStart / 60_000;
  const jobs: Job[] = [];
  for (const city of cities) {
    for (const feed of city.realtime) {
      if (!(feed.every_minutes >= 1)) throw new Error(`${city.id}/${feed.feed}: every_minutes must be >= 1`);
      if (minute % feed.every_minutes !== 0) continue;
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

// Keyed by the scheduled slot, which is unique per city and feed.
export function errorKey(city: string, feed: string, slot: number): string {
  const iso = new Date(slot).toISOString();
  return `errors/${city}/${iso.slice(0, 10)}/${slot}-${feed}.json`;
}
