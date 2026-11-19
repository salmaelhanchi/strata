import type { ReactNode } from "react";

export const fmt = (n: number | null | undefined): string => (n == null ? "–" : n.toLocaleString("en"));

export const pct = (part: number, whole: number): string =>
  whole ? `${((100 * part) / whole).toFixed(1)}%` : "–";

export function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="card px-4 py-3">
      <div className="num text-2xl font-semibold">{value}</div>
      <div className="muted text-xs">{label}</div>
    </div>
  );
}

/** A horizontal bar: one share, or two (green part, sand rest). */
export function Bar({ part, whole, split = false }: { part: number; whole: number; split?: boolean }) {
  const w = whole ? (100 * part) / whole : 0;
  return (
    <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-paper-line dark:bg-[#2d2c28]">
      <div className="bg-moss dark:bg-moss-light" style={{ width: `${w}%` }} />
      {split && <div className="bg-sand" style={{ width: `${100 - w}%` }} />}
    </div>
  );
}

export function Section({ title, aside, children }: { title: string; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="mt-8">
      <div className="mb-3 flex items-baseline justify-between gap-4">
        <h2 className="text-base font-semibold">{title}</h2>
        {aside && <div className="muted text-xs">{aside}</div>}
      </div>
      {children}
    </section>
  );
}

export function Loading({ what }: { what: string }) {
  return (
    <div className="muted flex items-center gap-3 py-10 text-sm">
      <span className="h-3 w-3 animate-ping rounded-full bg-moss" />
      {what}
    </div>
  );
}

export function Failure({ error }: { error: unknown }) {
  return (
    <div className="card mt-6 border-rust/40 px-4 py-3 text-sm text-rust">
      {error instanceof Error ? error.message : String(error)}
    </div>
  );
}

export function Counts({ title, counts, total }: { title: string; counts: Record<string, number>; total: number }) {
  return (
    <div className="card p-4">
      <h3 className="mb-2 text-sm font-semibold">{title}</h3>
      <table className="w-full text-sm">
        <tbody>
          {Object.entries(counts).map(([k, v]) => (
            <tr key={k}>
              <td className="max-w-0 truncate py-1 pr-2 font-mono text-xs" title={k}>
                {k}
              </td>
              <td className="num w-16 py-1 text-right">{fmt(v)}</td>
              <td className="w-24 py-1 pl-3">
                <Bar part={v} whole={total} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function Pill({ tone, children }: { tone: "good" | "bad" | "plain"; children: ReactNode }) {
  const colors = {
    good: "bg-moss/15 text-moss dark:text-moss-light",
    bad: "bg-rust/15 text-rust",
    plain: "bg-paper-line text-ink-soft dark:bg-[#2d2c28] dark:text-[#9c9a92]",
  }[tone];
  return <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${colors}`}>{children}</span>;
}
