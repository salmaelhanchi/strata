import { useInfiniteQuery } from "@tanstack/react-query";

import { api } from "../api";
import { useConsole } from "../store";
import { Bar, Failure, Loading, Pill, Section, fmt } from "./ui";

export { Failure };

export function Jobs() {
  const jobId = useConsole((s) => s.jobId);
  const showJob = useConsole((s) => s.showJob);
  const jobs = useInfiniteQuery({
    queryKey: ["jobs"],
    queryFn: ({ pageParam }) => api.jobs(pageParam),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor ?? null,
  });
  if (jobs.isPending) return <Loading what="Loading jobs…" />;
  if (jobs.error) return <Failure error={jobs.error} />;
  const rows = jobs.data.pages.flatMap((p) => p.items);
  return (
    <div className="grid gap-6 lg:grid-cols-[22rem_1fr]">
      <div className="card h-fit overflow-hidden">
        {rows.map((j) => (
          <button
            key={j.job_id}
            onClick={() => showJob(j.job_id)}
            className={`block w-full border-t border-paper-line px-4 py-2 text-left first:border-0 dark:border-[#2d2c28] ${j.job_id === jobId ? "bg-moss/10" : ""}`}
          >
            <div className="flex items-center justify-between gap-2">
              <span className="truncate text-sm font-medium">{j.domain}</span>
              <Pill tone={j.state === "succeeded" ? "good" : j.state === "running" ? "plain" : "bad"}>{j.state}</Pill>
            </div>
            <div className="muted mt-0.5 font-mono text-[11px]">{j.job_id}</div>
            <div className="mt-1.5">
              <Bar part={j.budget_spent} whole={j.budget_cap} />
            </div>
          </button>
        ))}
        {jobs.hasNextPage && (
          <button onClick={() => jobs.fetchNextPage()} className="w-full border-t border-paper-line py-2 text-xs dark:border-[#2d2c28]">
            More
          </button>
        )}
      </div>
      <div>{jobId ? <Pages jobId={jobId} /> : <p className="muted text-sm">Choose a job.</p>}</div>
    </div>
  );
}

/** Pages a pull wrote; a row opens its lineage. */
export function Pages({ jobId }: { jobId: string }) {
  const openLineage = useConsole((s) => s.openLineage);
  const pages = useInfiniteQuery({
    queryKey: ["pages", jobId],
    queryFn: ({ pageParam }) => api.jobPages(jobId, pageParam),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor ?? null,
  });
  if (pages.isPending) return <Loading what="Loading pages…" />;
  if (pages.error) return <Failure error={pages.error} />;
  const rows = pages.data.pages.flatMap((p) => p.items);
  if (!rows.length)
    return <p className="muted text-sm">No pages pulled from the console for this job (CLI pulls write to their own target).</p>;
  return (
    <Section title="Pages" aside="click a row for its lineage">
      <div className="card overflow-hidden">
        <table className="w-full text-sm">
          <thead className="muted text-left text-xs uppercase tracking-wide">
            <tr>
              <th className="px-4 py-2 font-medium">page</th>
              <th className="w-24 px-2 py-2 text-right font-medium">text</th>
              <th className="w-20 px-2 py-2 text-right font-medium">links</th>
              <th className="w-28 px-4 py-2 font-medium">robots</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr
                key={r.capture_id}
                onClick={() => openLineage(r.capture_id)}
                className="cursor-pointer border-t border-paper-line hover:bg-moss/5 dark:border-[#2d2c28]"
              >
                <td className="max-w-0 px-4 py-1.5">
                  <div className="truncate font-medium">{r.title ?? "(no title)"}</div>
                  <div className="muted truncate font-mono text-xs">{r.url}</div>
                </td>
                <td className="num px-2 text-right text-xs">{fmt(r.chars)}</td>
                <td className="num px-2 text-right text-xs">{fmt(r.links)}</td>
                <td className="px-4">
                  <Pill tone={r.robots_verdict === "allowed" ? "good" : "bad"}>{r.robots_verdict}</Pill>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Section>
  );
}
