// vite.config.js — front-end build for the coagent UI.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T7.
//
// Layout:
//   ui/index.html       — vite entry HTML (root)
//   ui/src/             — JS sources imported from index.html
//   ui/public/          — static assets copied verbatim to dist/
//   ui/dist/            — `vite build` output (consumed by cmd/server's
//                          static file handler in production)
//
// Dev proxy: `/api/*` and `/ws` are forwarded to the Go server so the
// SPA can run in vite dev mode without CORS plumbing. The server URL
// is configurable via VITE_SERVER_URL.
//
// M1.6-T7 phase-3 — build-time env propagation:
//   COAGENT_EXTENSION_ID  → import.meta.env.VITE_COAGENT_EXTENSION_ID
//   COAGENT_SERVER_URL    → vite dev proxy target
//
// The COAGENT_* names mirror the env knobs used by cmd/server / cmd/daemon
// / wxt.config.ts so a single .env file at the deploy root configures
// the whole stack. Vite refuses to expose env vars to the client unless
// they're VITE_*-prefixed, so we explicitly bridge COAGENT_EXTENSION_ID
// into VITE_COAGENT_EXTENSION_ID via the `define` plugin below.

import { defineConfig } from 'vite';

const serverURL =
  process.env.COAGENT_SERVER_URL || process.env.VITE_SERVER_URL || 'http://localhost:8080';
const wsURL = serverURL.replace(/^http/, 'ws');

// COAGENT_EXTENSION_ID is the project-wide canonical name; VITE_COAGENT_
// EXTENSION_ID is the vite-required prefix that lands in the client
// bundle. When the unprefixed env var is set, mirror it so the client
// code (ui/src/extension.js) stays on the historical VITE_* lookup.
const extensionID = (
  process.env.COAGENT_EXTENSION_ID ||
  process.env.VITE_COAGENT_EXTENSION_ID ||
  ''
).trim();
if (extensionID) {
  process.env.VITE_COAGENT_EXTENSION_ID = extensionID;
}

export default defineConfig({
  root: '.',
  publicDir: 'public',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': { target: serverURL, changeOrigin: true },
      '/healthz': { target: serverURL, changeOrigin: true },
      '/ws': { target: wsURL, ws: true, changeOrigin: true },
    },
  },
});
