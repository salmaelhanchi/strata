import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { api } from "../api";
import { useConsole } from "../store";
import { Failure, Pages } from "./Jobs";
import { Bar, Pill, Section, fmt } from "./ui";

interface Progress {
  job_id: string;
  state: string;
  attempts: number;
  budget_spent: number;
  budget_cap: number;
  outcomes: Record<string, number>;
}

export function Pull({ site }: { site: string }) {
  const [pattern, setPattern] = useState("");
  const [limit, setLimit] = useState(10);
  const [budget, setBudget] = useState(150);
  const [runId, setRunId] = useState<string | null>(null);
  const start = useMutation({
    mutationFn: () => api.startPull({ site, pattern: pattern || null, limit, budget, crawl: "latest" }),
    onSuccess: (run) => setRunId(run.run_id),
  });

  return (
    <div>
      <p className="muted mb-4 max-w-2xl text-sm">
        A pull is a job: the budget is spent before each request, robots.txt from the same crawl is checked
        first, every record is verified against the index, and a pull that dies continues where it stopped.
        Then each page's title, main text, language and links land in a table, with their lineage.
      </p>
      <form
        className="card grid gap-3 p-4 md:grid-cols-[1fr_7rem_7rem_auto]"
        onSubmit={(e) => {
          e.preventDefault();
          setRunId(null);
          start.mutate();
        }}
      >
        <label className="text-xs">
          <span className="muted">URL pattern (RE2, whole URL)</span>
          <input
            value={pattern}
            onChange={(e) => setPattern(e.target.value)}
            placeholder={`^https://${site}/.*$`}
            className="mt-1 w-full rounded-lg border border-paper-line bg-paper px-3 py-2 font-mono text-xs dark:border-[#2d2c28] dark:bg-[#161614]"
          />
        </label>
        <Num label="pages" value={limit} onChange={setLimit} max={500} />
        <Num label="budget" value={budget} onChange={setBudget} max={5000} />
        <button
          type="submit"
          disabled={start.isPending}
          className="self-end rounded-lg bg-moss px-5 py-2 text-sm font-medium text-white disabled:opacity-50"
        >
          Pull
        </button>
      </form>
      {start.error && <Failure error={start.error} />}
      {runId && <Run key={runId} runId={runId} />}
    </div>
  );
}

function Num({ label, value, onChange, max }: { label: string; value: number; onChange: (n: number) => void; max: number }) {
  return (
    <label className="text-xs">
      <span className="muted">{label}</span>
      <input
        type="number"
        min={1}
        max={max}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="num mt-1 w-full rounded-lg border border-paper-line bg-paper px-3 py-2 text-sm dark:border-[#2d2c28] dark:bg-[#161614]"
      />
    </label>
  );
}

/** One pull, followed over Server-Sent Events until it is done. */
function Run({ runId }: { runId: string }) {
  const [progress, setProgress] = useState<Progress | null>(null);
  const [done, setDone] = useState<{
    job_id: string;
    state: string;
    written: number;
    selected: number;
    run: { stop_reason: string | null } | null;
  } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const client = useQueryClient();
  const showJob = useConsole((s) => s.showJob);

  useEffect(() => {
    const events = api.pullEvents(runId);
    events.addEventListener("progress", (e) => setProgress(JSON.parse((e as MessageEvent).data)));
    events.addEventListener("done", (e) => {
      setDone(JSON.parse((e as MessageEvent).data));
      client.invalidateQueries({ queryKey: ["jobs"] });
      events.close();
    });
    events.addEventListener("error", (e) => {
      const data = (e as MessageEvent).data;
      if (data) setError(JSON.parse(data).detail);
      events.close();
    });
    return () => events.close();
  }, [runId, client]);

  const p = progress;
  return (
    <Section title="Pull" aside={<span className="font-mono">{done?.job_id ?? p?.job_id ?? runId}</span>}>
      <div className="card grid gap-4 p-4 md:grid-cols-[1fr_auto]">
        <div>
          <div className="mb-1 flex justify-between text-xs">
            <span className="muted">budget</span>
            <span className="num">
              {fmt(p?.budget_spent ?? 0)} / {fmt(p?.budget_cap ?? 0)}
            </span>
          </div>
          <Bar part={p?.budget_spent ?? 0} whole={p?.budget_cap ?? 1} />
          <div className="muted mt-2 flex flex-wrap gap-3 text-xs">
            {Object.entries(p?.outcomes ?? {}).map(([k, v]) => (
              <span key={k}>
                {k} <span className="num">{fmt(v)}</span>
              </span>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {error ? (
            <Pill tone="bad">error</Pill>
          ) : done ? (
            <Pill tone={done.state === "succeeded" ? "good" : "bad"}>{done.state}</Pill>
          ) : (
            <Pill tone="plain">{!p ? "starting…" : p.state === "running" ? "fetching…" : "writing pages…"}</Pill>
          )}
        </div>
      </div>
      {error && <Failure error={error} />}
      {done?.run?.stop_reason && <Failure error={`Stopped: ${done.run.stop_reason}`} />}
      {done && (
        <div className="mt-4">
          <p className="mb-2 text-sm">
            {fmt(done.written)} of {fmt(done.selected)} pages in <span className="font-mono">strata_pages</span>.{" "}
            <button className="underline" onClick={() => showJob(done.job_id)}>
              Open the job
            </button>
          </p>
          <Pages jobId={done.job_id} />
        </div>
      )}
    </Section>
  );
}
