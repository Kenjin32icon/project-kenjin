const { app, BrowserWindow } = require('electron');
const path = require('path');
const { loadKenjinEnv } = require('./envConfig');

function createWindow() {
  const env = loadKenjinEnv();

  const win = new BrowserWindow({
    width: 1300,
    height: 900,
    title: "Project Kenjin — Quant Matrix Desktop Monitor",
    backgroundColor: "#0f1115",
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      // v11.07: exposes .env's ORCH_API_KEY/KENJIN_HOST/KENJIN_PORT to the
      // dashboard page via contextBridge (see preload.js) - this is what
      // lets the desktop app auto-connect without the operator typing the
      // API key in by hand every time.
      preload: path.join(__dirname, 'preload.js'),
    }
  });

  // v11.07: read from .env instead of a hardcoded 127.0.0.1:8000, so this
  // still points at the right place if KENJIN_HOST/KENJIN_PORT are changed.
  const dashboardUrl = `${env.baseUrl}/static/dashboard.html`;
  win.loadURL(dashboardUrl);

  win.webContents.on('did-fail-load', () => {
    setTimeout(() => {
      win.loadURL(dashboardUrl);
    }, 2000);
  });
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
