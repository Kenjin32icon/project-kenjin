const { app, BrowserWindow } = require('electron');
const path = require('path');

function createWindow() {
  const win = new BrowserWindow({
    width: 1300,
    height: 900,
    title: "Project Kenjin — Quant Matrix Desktop Monitor",
    backgroundColor: "#0f1115",
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true
    }
  });

  // Load the static dashboard from orchestrator server
  win.loadURL('http://127.0.0.1:8000/static/dashboard.html');

  // Fallback: If orchestrator is starting up, retry connection
  win.webContents.on('did-fail-load', () => {
    setTimeout(() => {
      win.loadURL('http://127.0.0.1:8000/static/dashboard.html');
    }, 2000);
  });
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
