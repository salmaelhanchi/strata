// The console API (docs/api.md), typed after the server's pydantic models.

export type Change = "added" | "removed" | "changed" | "same";

export interface Crawl {
  id: string;
  name: string;
}

export interface ShapeProfile {
  shape: string;
  urls: number;
  example: string;
  allowed: number | null;
}

export interface CrawlProfile {
  crawl: string;
  captures: number;
  eligible: number;
  urls: number;
  by_status: Record<string, number>;
  by_mime: Record<string, number>;
  by_language: Record<string, number>;
  hosts: Record<string, number>;
  depth: Record<string, number>;
  shapes: ShapeProfile[];
  query_string_urls: number;
  first_capture: string | null;
  last_capture: string | null;
  from_cache: boolean;
  requests: number;
  bytes: number;
}

export interface RobotsGroup {
  agents: string[];
  rules: [string, string][];
  crawl_delay: string | null;
}

export interface RobotsProfile {
  host: string;
  basis: string;
  robots_url: string | null;
  capture_id: string | null;
  archived_status: number | null;
  crawl_delay_s: number | null;
  sitemaps: string[];
  group: RobotsGroup | null;
  groups: number;
  verdicts: Record<string, number>;
  error: string | null;
}

export interface SiteProfile {
  site: string;
  crawls: CrawlProfile[];
  robots: RobotsProfile[];
  requests: number;
  footer_reads: number;
}

export interface ShapeDiff {
  shape: string;
  counts: Record<Change, number>;
}

export interface DiffSummary {
  site: string;
  from_crawl: string;
  to_crawl: string;
  counts: Record<Change, number>;
  shapes: ShapeDiff[];
  requests: number;
  page_fetches: number;
}

export interface UrlChange {
  url: string;
  change: Change;
}

export interface ShapeRate {
  shape: string;
  urls: number;
  compared: number;
  changed: number;
  example: string;
}

export interface ChangeRates {
  site: string;
  crawls: string[];
  shapes: ShapeRate[];
  compared: number;
  changed: number;
  requests: number;
}

export interface Page<T> {
  items: T[];
  next_cursor?: string | null;
}

export interface JobItem {
  job_id: string;
  state: string;
  stop_reason: string | null;
  domain: string | null;
  crawl: string | null;
  budget_cap: number;
  budget_spent: number;
  created_at: string;
}

export interface PageItem {
  url: string;
  crawl: string;
  title: string | null;
  lang: string | null;
  chars: number | null;
  links: number;
  robots_verdict: string;
  captured_at: string | null;
  capture_id: string;
}

export interface LineageReport {
  capture: {
    capture_id: string;
    job_id: string;
    url: string;
    crawl: string | null;
    upstream_fetch_time: string | null;
    fetched_at: string;
    raw_uri: string;
    raw_offset: number;
    raw_length: number;
    raw_sha256: string;
    http_status: number | null;
    policy_verdict: string;
    robots_capture_id: string | null;
  };
  upstream_url: string | null;
  upstream_range: string | null;
  raw_bytes_intact: boolean;
  index_digest: string | null;
  warc_header_digest: string | null;
  recomputed_digest: string | null;
  index_matches_warc_header: boolean | null;
  recompute_matches_header: boolean | null;
  http_status_line: string | null;
}

export interface Lineage {
  lineage: LineageReport;
  page: { title: string | null; main_text: string | null; out_links: string[] } | null;
}

export interface PullRequest {
  site: string;
  pattern: string | null;
  limit: number;
  budget: number;
  crawl: string;
}

export interface PullRun {
  run_id: string;
  state: string;
  job_id: string | null;
}

/** An RFC 9457 problem the API answered with. */
export class Problem extends Error {
  constructor(
    readonly status: number,
    readonly title: string,
    readonly detail?: string,
  ) {
    super(detail ? `${title}: ${detail}` : title);
  }
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(path, {
    ...init,
    headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    throw new Problem(resp.status, body.title ?? resp.statusText, body.detail);
  }
  return (await resp.json()) as T;
}

const q = (params: Record<string, string | number | null | undefined>): string => {
  const s = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v != null && v !== "") s.set(k, String(v));
  const text = s.toString();
  return text ? `?${text}` : "";
};

const site = (name: string) => `/v1/sites/${encodeURIComponent(name)}`;

export const api = {
  crawls: () => call<Crawl[]>("/v1/crawls"),
  profile: (name: string, crawls = 2) => call<SiteProfile>(`${site(name)}/profile${q({ crawls })}`),
  diff: (name: string) => call<DiffSummary>(`${site(name)}/diff`),
  diffChanges: (name: string, change: Change | null, cursor?: string | null) =>
    call<Page<UrlChange>>(`${site(name)}/diff/changes${q({ change, cursor, limit: 25 })}`),
  rates: (name: string, crawls = 3) => call<ChangeRates>(`${site(name)}/changes${q({ crawls })}`),
  jobs: (cursor?: string | null) => call<Page<JobItem>>(`/v1/jobs${q({ cursor, limit: 15 })}`),
  jobPages: (jobId: string, cursor?: string | null) =>
    call<Page<PageItem>>(`/v1/jobs/${jobId}/pages${q({ cursor, limit: 50 })}`),
  lineage: (captureId: string) => call<Lineage>(`/v1/captures/${captureId}/lineage`),
  startPull: (body: PullRequest) =>
    call<PullRun>("/v1/pulls", { method: "POST", body: JSON.stringify(body) }),
  pullEvents: (runId: string) => new EventSource(`/v1/pulls/${runId}/events`),
};
