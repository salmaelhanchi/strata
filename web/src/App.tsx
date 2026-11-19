import { useState } from "react";

import { Changes } from "./components/Changes";
import { Jobs } from "./components/Jobs";
import { LineageDrawer } from "./components/LineageDrawer";
import { Profile } from "./components/Profile";
import { Pull } from "./components/Pull";
import { useConsole, type View } from "./store";

const VIEWS: { id: View; label: string; needsSite: boolean }[] = [
  { id: "profile", label: "Profile", needsSite: true },
  { id: "changes", label: "Changes", needsSite: true },
  { id: "pull", label: "Pull", needsSite: true },
  { id: "jobs", label: "Jobs", needsSite: false },
];

export function App() {
  const { site, view, chooseSite, setView } = useConsole();
  const [draft, setDraft] = useState(site);

  return (
    <div className="min-h-screen">
      <header className="border-b border-paper-line dark:border-[#2d2c28]">
        <div className="mx-auto flex max-w-6xl flex-wrap items-center gap-4 px-5 py-4">
          <div className="mr-4">
            <div className="text-lg font-semibold tracking-tight">Strata</div>
            <div className="muted text-xs">Before you scrape a site, ask Strata.</div>
          </div>
          <form
            className="flex flex-1 gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              chooseSite(draft);
              if (view === "jobs") setView("profile");
            }}
          >
            <input
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="a site, e.g. rfc-editor.org"
              aria-label="Site"
              className="w-full max-w-sm rounded-lg border border-paper-line bg-paper-card px-3 py-2 text-sm dark:border-[#2d2c28] dark:bg-[#1e1e1b]"
            />
            <button type="submit" className="rounded-lg bg-ink px-4 py-2 text-sm text-paper dark:bg-[#ecebe6] dark:text-[#161614]">
              Look
            </button>
          </form>
          <nav className="flex gap-1">
            {VIEWS.map((v) => (
              <button
                key={v.id}
                disabled={v.needsSite && !site}
                onClick={() => setView(v.id)}
                className={`rounded-lg px-3 py-1.5 text-sm disabled:opacity-40 ${
                  view === v.id ? "bg-moss text-white" : "hover:bg-moss/10"
                }`}
              >
                {v.label}
              </button>
            ))}
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-5 py-6">
        {view === "jobs" ? (
          <Jobs />
        ) : !site ? (
          <Intro />
        ) : view === "profile" ? (
          <Profile key={site} site={site} />
        ) : view === "changes" ? (
          <Changes key={site} site={site} />
        ) : (
          <Pull key={site} site={site} />
        )}
      </main>
      <LineageDrawer />
    </div>
  );
}

function Intro() {
  return (
    <div className="max-w-2xl py-10">
      <h1 className="text-2xl font-semibold">What does a site hold, what changes, and what may you take?</h1>
      <p className="muted mt-3">
        Strata answers from Common Crawl's index without fetching a single page of the site, then pulls the
        pages you choose from Common Crawl's archive, politely and within a budget, with every row traceable
        to the bytes it came from. Type a site above.
      </p>
    </div>
  );
}
