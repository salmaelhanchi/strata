import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { api } from "../api";
import { useConsole } from "../store";
import { Failure, Loading, Pill, fmt } from "./ui";

/** A pulled row, back to its bytes: Common Crawl's copy, ours, and the checks that tie them. */
export function LineageDrawer() {
  const id = useConsole((s) => s.lineageId);
  const close = useConsole((s) => s.closeLineage);
  const trace = useQuery({
    queryKey: ["lineage", id],
    queryFn: () => api.lineage(id!),
    enabled: id !== null,
    staleTime: Infinity,
  });
  if (id === null) return null;

  return (
    <div className="fixed inset-0 z-20 flex justify-end bg-black/20" onClick={close}>
      <aside
        onClick={(e) => e.stopPropagation()}
        className="h-full w-full max-w-xl overflow-y-auto bg-paper p-6 shadow-2xl dark:bg-[#161614]"
      >
        <div className="flex items-start justify-between gap-4">
          <h2 className="text-lg font-semibold">Lineage</h2>
          <button onClick={close} className="muted text-sm" aria-label="Close">
            Close
          </button>
        </div>
        {trace.isPending ? (
          <Loading what="Re-reading the stored bytes…" />
        ) : trace.error ? (
          <Failure error={trace.error} />
        ) : (
          <Trace data={trace.data} />
        )}
      </aside>
    </div>
  );
}

function Check({ ok, label }: { ok: boolean | null; label: string }) {
  return (
    <div className="flex items-center justify-between py-1.5 text-sm">
      <span>{label}</span>
      <Pill tone={ok ? "good" : ok === null ? "plain" : "bad"}>{ok ? "yes" : ok === null ? "n/a" : "no"}</Pill>
    </div>
  );
}

function Row({ k, children }: { k: string; children: ReactNode }) {
  return (
    <div className="grid grid-cols-[8rem_1fr] gap-3 py-1 text-xs">
      <span className="muted">{k}</span>
      <span className="break-all font-mono">{children}</span>
    </div>
  );
}

function Trace({ data }: { data: NonNullable<Awaited<ReturnType<typeof api.lineage>>> }) {
  const l = data.lineage;
  const c = l.capture;
  return (
    <div className="mt-4 space-y-5">
      <div>
        <div className="font-medium">{data.page?.title ?? c.url}</div>
        <div className="muted break-all font-mono text-xs">{c.url}</div>
      </div>
      <div className="card divide-y divide-paper-line px-4 dark:divide-[#2d2c28]">
        <Check ok={l.raw_bytes_intact} label="Our copy still hashes to its recorded SHA-256" />
        <Check ok={l.index_matches_warc_header} label="Index digest matches the WARC header" />
        <Check ok={l.recompute_matches_header} label="Payload re-hashed matches the WARC header" />
        <Check ok={c.policy_verdict === "allowed"} label={`robots.txt verdict: ${c.policy_verdict}`} />
      </div>
      <div className="card px-4 py-2">
        <h3 className="py-1 text-sm font-semibold">Common Crawl's copy</h3>
        <Row k="crawl">{c.crawl}</Row>
        <Row k="captured">{c.upstream_fetch_time ?? "–"}</Row>
        <Row k="WARC file">{l.upstream_url}</Row>
        <Row k="range">{l.upstream_range}</Row>
        <Row k="digest">{l.index_digest}</Row>
        <Row k="archived reply">{l.http_status_line}</Row>
      </div>
      <div className="card px-4 py-2">
        <h3 className="py-1 text-sm font-semibold">Strata's copy</h3>
        <Row k="stored at">
          {c.raw_uri} @ {fmt(c.raw_offset)} (+{fmt(c.raw_length)} B)
        </Row>
        <Row k="sha256">{c.raw_sha256}</Row>
        <Row k="fetched">{c.fetched_at}</Row>
        <Row k="job">{c.job_id}</Row>
        <Row k="robots.txt">{c.robots_capture_id ?? "–"}</Row>
        <Row k="capture id">{c.capture_id}</Row>
      </div>
      {data.page?.main_text && (
        <div className="card p-4">
          <h3 className="mb-2 text-sm font-semibold">Main text</h3>
          <p className="muted line-clamp-[10] whitespace-pre-line text-xs leading-relaxed">{data.page.main_text}</p>
        </div>
      )}
    </div>
  );
}
