import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Built into the console package, which FastAPI serves; `npm run dev` proxies the API.
export default defineConfig({
  plugins: [react()],
  build: { outDir: "../packages/strata-console/src/strata_console/static", emptyOutDir: true },
  server: { proxy: { "/v1": "http://127.0.0.1:8040" } },
});
