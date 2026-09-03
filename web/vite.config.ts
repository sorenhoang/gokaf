import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Build output goes straight into the Go package so it can be go:embed-ed.
// `npm run dev` proxies /api to a locally running broker.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/httpapi/dist",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://localhost:8080",
    },
  },
});
