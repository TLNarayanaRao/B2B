import { defineConfig } from 'vite';
const apiTarget = process.env.RELAY_API_URL || 'http://127.0.0.1:8010';
export default defineConfig({ server: { proxy: { '/api': apiTarget, '/http': apiTarget, '/as2': apiTarget } } });
