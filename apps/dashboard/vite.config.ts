import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react-swc';
import * as path from 'node:path';

// https://vite.dev/config/
// Relative base in builds: the server injects <base href> with the real mount
// path, unknown at build time. Dev keeps the absolute path the router expects.
// A statically hosted build has no server to inject it, so it sets the mount
// path through VITE_DASHBOARD_BASENAME (e.g. "/"), keeping asset URLs
// absolute so deep links still load them.
export default defineConfig(({ command }) => ({
  plugins: [react()],
  base: command === 'build' ? (process.env.VITE_DASHBOARD_BASENAME ?? './') : '/dashboard',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
}));
