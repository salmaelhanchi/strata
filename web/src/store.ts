import { create } from "zustand";

export type View = "profile" | "changes" | "pull" | "jobs";

interface ConsoleState {
  site: string;
  view: View;
  jobId: string | null; // the job whose pages are shown
  lineageId: string | null; // the capture whose lineage drawer is open
  chooseSite: (site: string) => void;
  setView: (view: View) => void;
  showJob: (jobId: string) => void;
  openLineage: (captureId: string) => void;
  closeLineage: () => void;
}

// Client state only: what the person is looking at. Server data lives in TanStack Query.
export const useConsole = create<ConsoleState>((set) => ({
  site: "",
  view: "profile",
  jobId: null,
  lineageId: null,
  chooseSite: (site) => set({ site: site.trim().toLowerCase() }),
  setView: (view) => set({ view }),
  showJob: (jobId) => set({ jobId, view: "jobs" }),
  openLineage: (lineageId) => set({ lineageId }),
  closeLineage: () => set({ lineageId: null }),
}));
