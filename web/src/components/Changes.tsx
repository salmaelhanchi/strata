import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api, type Change } from "../api";
import { Bar, Failure, Loading, Pill, Section, Stat, fmt } from "./ui";

const KINDS: Change[] = ["added", "removed", "changed"];

export function Changes({ site }: { site: string }) {
  const diff = useQuery({ queryKey: ["diff", site], queryFn: () => api.diff(site), staleTime: Infinity });
  const rates = useQuery({ queryKey: ["rates", site], queryFn: () => api.rates(site), staleTime: Infinity });
  const [kind, setKind] = useState<Change>("changed");

  if (diff.isPending) return <Loading what={`Comparing ${site} across crawls…`} />;
  if (diff.error) return <Failure error={diff.error} />;
  const d = diff.data;
  const total = Object.values(d.counts).reduce((a, b) => a + b, 0);

  return (
    <div>
      <p className="muted mb-4 text-sm">
        <span className="font-mono">{d.from_crawl}</span> → <span className="font-mono">{d.to_crawl}</span>, from the
        payload digests in the index: {d.requests} index requests, {d.page_fetches} page fetches.
      </p>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Stat label="added" value={fmt(d.counts.added)} />
        <Stat label="removed (not re-captured)" value={fmt(d.counts.removed)} />
        <Stat label="changed" value={fmt(d.counts.changed)} />
        <Stat label="same" value={fmt(d.counts.same)} />
      </div>

      <Section title="By URL shape" aside={<Legend />}>
        <div className="card divide-y divide-paper-line dark:divide-[#2d2c28]">
          {d.shapes.slice(0, 8).map((s) => {
            const n = s.counts.added + s.counts.removed + s.counts.changed + s.counts.same;
            return (
              <div key={s.shape} className="grid grid-cols-[1fr_16rem] items-center gap-4 px-4 py-2 text-sm">
                <span className="truncate font-mono text-xs">{s.shape}</span>
                <div className="flex h-2.5 overflow-hidden rounded-full">
                  <div className="bg-moss" style={{ width: `${(100 * s.counts.added) / n}%` }} />
                  <div className="bg-rust/70" style={{ width: `${(100 * s.counts.removed) / n}%` }} />
                  <div className="bg-sand" style={{ width: `${(100 * s.counts.changed) / n}%` }} />
                  <div className="bg-paper-line dark:bg-[#2d2c28]" style={{ width: `${(100 * s.counts.same) / n}%` }} />
                </div>
              </div>
            );
          })}
        </div>
        <p className="muted mt-2 text-xs">{fmt(total)} URLs in either crawl.</p>
      </Section>

      <Section title="What changes, crawl to crawl" aside={rates.data ? `${rates.data.crawls.length} crawls` : undefined}>
        {rates.isPending ? (
          <Loading what="Reading one more crawl…" />
        ) : rates.error ? (
          <Failure error={rates.error} />
        ) : (
          <div className="card divide-y divide-paper-line dark:divide-[#2d2c28]">
            {[
              ...rates.data.shapes.filter((s) => s.changed > 0).slice(0, 5),
              ...rates.data.shapes
                .filter((s) => s.compared > 0 && s.changed === 0)
                .sort((a, b) => b.compared - a.compared)
                .slice(0, 3),
            ].map((s) => (
              <div key={s.shape} className="grid grid-cols-[1fr_5rem_6rem_10rem] items-center gap-4 px-4 py-2 text-sm">
                <span className="truncate font-mono text-xs">{s.shape}</span>
                <Pill tone={s.changed ? "plain" : "good"}>{s.changed ? "changes" : "stable"}</Pill>
                <span className="num muted text-right text-xs">
                  {s.changed}/{s.compared} re-captures
                </span>
                <Bar part={s.changed} whole={s.compared} />
              </div>
            ))}
          </div>
        )}
      </Section>

      <Section title="Changed URLs">
        <div className="mb-3 flex gap-2">
          {KINDS.map((k) => (
            <button
              key={k}
              onClick={() => setKind(k)}
              className={`rounded-full px-3 py-1 text-xs ${k === kind ? "bg-ink text-paper dark:bg-[#ecebe6] dark:text-[#161614]" : "card"}`}
            >
              {k} · {fmt(d.counts[k])}
            </button>
          ))}
        </div>
        <ChangedUrls site={site} kind={kind} />
      </Section>
    </div>
  );
}

function ChangedUrls({ site, kind }: { site: string; kind: Change }) {
  const pages = useInfiniteQuery({
    queryKey: ["changes", site, kind],
    queryFn: ({ pageParam }) => api.diffChanges(site, kind, pageParam),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor ?? null,
  });
  if (pages.isPending) return <Loading what="Listing…" />;
  if (pages.error) return <Failure error={pages.error} />;
  const rows = pages.data.pages.flatMap((p) => p.items);
  return (
    <div className="card overflow-hidden text-sm">
      {rows.map((r) => (
        <div key={r.url} className="flex items-center gap-3 border-t border-paper-line px-4 py-1.5 first:border-0 dark:border-[#2d2c28]">
          <Pill tone={r.change === "added" ? "good" : r.change === "removed" ? "bad" : "plain"}>{r.change}</Pill>
          <span className="truncate font-mono text-xs">{r.url}</span>
        </div>
      ))}
      {pages.hasNextPage && (
        <button onClick={() => pages.fetchNextPage()} className="w-full border-t border-paper-line py-2 text-xs dark:border-[#2d2c28]">
          {pages.isFetchingNextPage ? "Loading…" : "More"}
        </button>
      )}
    </div>
  );
}

function Legend() {
  const items: [string, string][] = [
    ["bg-moss", "added"],
    ["bg-rust/70", "removed"],
    ["bg-sand", "changed"],
    ["bg-paper-line dark:bg-[#2d2c28]", "same"],
  ];
  return (
    <span className="flex gap-3">
      {items.map(([color, label]) => (
        <span key={label} className="flex items-center gap-1">
          <i className={`inline-block h-2 w-2 rounded-sm ${color}`} />
          {label}
        </span>
      ))}
    </span>
  );
}
