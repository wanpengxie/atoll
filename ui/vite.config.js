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

import { defineConfig } from 'vite';

const serverURL = process.env.VITE_SERVER_URL || 'http://localhost:8080';
const wsURL = serverURL.replace(/^http/, 'ws');

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
