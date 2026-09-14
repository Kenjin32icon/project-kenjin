// electron-app/envConfig.js
//
// Tiny, dependency-free .env reader shared by main.js and preload.js.
// Deliberately NOT using the `dotenv` npm package: start_kenjin.sh only
// runs `npm install` when node_modules is missing (see the venv-staleness
// fix applied elsewhere in this project) - adding a new dependency here
// would silently fail to install on any machine that already has
// node_modules from before this change, reproducing the exact class of bug
// that was just fixed on the Python side. A ~20-line manual parser avoids
// that risk entirely.
const fs = require('fs');
const path = require('path');

function parseEnvFile(filePath) {
  const config = {};
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    raw.split('\n').forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) return;
      const eqIdx = trimmed.indexOf('=');
      if (eqIdx === -1) return;
      const key = trimmed.slice(0, eqIdx).trim();
      let value = trimmed.slice(eqIdx + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      config[key] = value;
    });
  } catch (err) {
    // .env missing/unreadable - callers fall back to defaults, same as
    // opening dashboard.html directly in a browser with nothing saved.
  }
  return config;
}

function loadKenjinEnv() {
  // electron-app/ is one level below the project root, same layout
  // start_kenjin.sh assumes (ELECTRON_DIR="$ROOT_DIR/electron-app").
  const envPath = path.join(__dirname, '..', '.env');
  const env = parseEnvFile(envPath);
  const host = env.KENJIN_HOST || '127.0.0.1';
  const port = env.KENJIN_PORT || '8000';
  return {
    apiKey: env.ORCH_API_KEY || '',
    host,
    port,
    baseUrl: `http://${host}:${port}`,
  };
}

module.exports = { loadKenjinEnv };
