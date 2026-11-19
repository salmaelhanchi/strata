import { useQuery } from "@tanstack/react-query";

import { api, type RobotsProfile } from "../api";
import { Bar, Counts, Failure, Loading, Pill, Section, Stat, fmt, pct } from "./ui";

export function Profile({ site }: { site: string }) {
  const { data, error, isPending } = useQuery({
    queryKey: ["profile", site],
    queryFn: () => api.profile(site),
    staleTime: Infinity, // a released crawl never changes
  });
  if (isPending) return <Loading what={`Reading ${site} from the URL index…`} />;
  if (error) return <Failure error={error} />;

  const c = data.crawls[0]!;
  const peak = Math.max(...data.crawls.map((x) => x.urls), 1);
  const known = c.shapes.every((s) => s.allowed !== null);
  return (
    <div>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <Stat label={`pages in ${c.crawl}`} value={fmt(c.urls)} />
        <Stat label="captures in the index" value={fmt(c.captures)} />
        <Stat label="hosts" value={fmt(Object.keys(c.hosts).length)} />
        <Stat label="requests for this profile" value={fmt(data.requests)} />
        <Stat label="page fetches" value="0" />
      </div>

      <Section title="Pages per crawl" aside="distinct URLs answered 200 with text/html">
        <div className="card divide-y divide-paper-line dark:divide-[#2d2c28]">
          {data.crawls.map((x) => (
            <div key={x.crawl} className="grid grid-cols-[10rem_5rem_1fr_6rem] items-center gap-4 px-4 py-2 text-sm">
              <span className="font-mono text-xs">{x.crawl}</span>
              <span className="num text-right">{fmt(x.urls)}</span>
              <Bar part={x.urls} whole={peak} />
              <span className="muted text-right text-xs">{x.from_cache ? "cached" : `${x.requests} req`}</span>
            </div>
          ))}
        </div>
      </Section>

      <Section title={`URL shapes in ${c.crawl}`} aside={known ? "green: allowed by robots.txt" : undefined}>
        <div className="card overflow-hidden">
          <table className="w-full text-sm">
            <thead className="muted text-left text-xs uppercase tracking-wide">
              <tr>
                <th className="px-4 py-2 font-medium">shape</th>
                <th className="px-2 py-2 text-right font-medium">pages</th>
                <th className="px-2 py-2 text-right font-medium">share</th>
                <th className="w-1/4 px-4 py-2 font-medium">{known ? "allowed" : ""}</th>
              </tr>
            </thead>
            <tbody>
              {c.shapes.slice(0, 12).map((s) => (
                <tr key={s.shape} className="border-t border-paper-line dark:border-[#2d2c28]">
                  <td className="px-4 py-1.5">
                    <div className="font-mono text-xs">{s.shape}</div>
                    <div className="muted max-w-md truncate text-xs">{s.example}</div>
                  </td>
                  <td className="num px-2 text-right">{fmt(s.urls)}</td>
                  <td className="num px-2 text-right">{pct(s.urls, c.urls)}</td>
                  <td className="px-4">
                    {s.allowed !== null ? <Bar part={s.allowed} whole={s.urls} split /> : <Bar part={s.urls} whole={c.urls} />}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section title="What the crawl holds">
        <div className="grid gap-4 md:grid-cols-3">
          <Counts title="HTTP status (captures)" counts={c.by_status} total={c.captures} />
          <Counts title="Language (pages)" counts={c.by_language} total={c.urls} />
          <Counts title="Hosts (pages)" counts={c.hosts} total={c.urls} />
        </div>
      </Section>

      <Section title="robots.txt, from the same crawl">
        <div className="grid gap-4 md:grid-cols-2">
          {data.robots.map((r) => (
            <Robots key={r.host} r={r} />
          ))}
        </div>
      </Section>
    </div>
  );
}

function Robots({ r }: { r: RobotsProfile }) {
  const allowed = r.verdicts["allowed"] ?? 0;
  const total = Object.values(r.verdicts).reduce((a, b) => a + b, 0);
  return (
    <div className="card p-4 text-sm">
      <div className="flex items-center justify-between gap-2">
        <span className="font-semibold">{r.host}</span>
        <Pill tone={r.basis === "parsed" || r.basis === "unavailable" ? "good" : "bad"}>{r.basis}</Pill>
      </div>
      <div className="muted mt-1 text-xs">
        {r.archived_status != null && `archived HTTP ${r.archived_status} · `}
        {fmt(allowed)} of {fmt(total)} pages allowed
      </div>
      {r.group && (
        <pre className="mt-3 max-h-36 overflow-auto rounded-lg bg-paper p-2 font-mono text-xs dark:bg-[#161614]">
          {`User-agent: ${r.group.agents.join(", ")}\n`}
          {r.group.rules.map(([k, v]) => `${k[0]!.toUpperCase()}${k.slice(1)}: ${v}`).join("\n") || "(no rules)"}
        </pre>
      )}
      {r.sitemaps.length > 0 && <div className="muted mt-2 truncate text-xs">Sitemap: {r.sitemaps[0]}</div>}
    </div>
  );
}
