// electron-app/preload.js
//
// Runs in an isolated context (contextIsolation: true, nodeIntegration:
// false - unchanged from before, these security settings were already
// correct and are preserved as-is). Exposes ONLY the two config values the
// dashboard needs, via contextBridge, rather than giving the renderer
// direct Node/fs access. This is what lets dashboard.html auto-fill and
// auto-connect with zero manual steps when running inside the actual
// desktop app, while still falling back to manual entry if this file is
// ever absent or .env can't be read.
const { contextBridge } = require('electron');
const { loadKenjinEnv } = require('./envConfig');

const env = loadKenjinEnv();

contextBridge.exposeInMainWorld('kenjinConfig', {
  apiKey: env.apiKey,
  baseUrl: env.baseUrl,
});
