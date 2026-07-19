const { app, BrowserWindow, Menu, dialog, ipcMain } = require("electron");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");

const port = Number(process.env.GOLAZO_PORT || 8765);
const backendUrl = `http://127.0.0.1:${port}`;
const appName = "Golazo";
const iconPath = path.join(__dirname, "assets", "icon.png");
let backendProcess;
let mainWindow;

app.setName(appName);
if (process.platform === "darwin") app.setAboutPanelOptions({ applicationName: appName });

function sendMenuCommand(command) {
  if (!mainWindow || mainWindow.isDestroyed()) return;
  mainWindow.webContents.send("menu:command", command);
}

function createMenu() {
  const appMenu = process.platform === "darwin" ? [{
    label: appName,
    submenu: [
      { role: "about" },
      { type: "separator" },
      { role: "services" },
      { type: "separator" },
      { role: "hide" },
      { role: "hideOthers" },
      { role: "unhide" },
      { type: "separator" },
      { role: "quit" },
    ],
  }] : [];
  const template = [
    ...appMenu,
    {
      label: "Project",
      submenu: [
        { label: "Add Project...", accelerator: "CmdOrCtrl+O", click: () => sendMenuCommand("add-project") },
        { label: "Rename Active Project...", accelerator: "CmdOrCtrl+Shift+R", click: () => sendMenuCommand("rename-project") },
      ],
    },
    {
      label: "Goal",
      submenu: [
        { label: "New Goal...", accelerator: "CmdOrCtrl+N", click: () => sendMenuCommand("new-goal") },
      ],
    },
    {
      label: "Edit",
      submenu: [
        { role: "undo" },
        { role: "redo" },
        { type: "separator" },
        { role: "cut" },
        { role: "copy" },
        { role: "paste" },
        ...(process.platform === "darwin" ? [
          { role: "pasteAndMatchStyle" },
          { role: "delete" },
          { role: "selectAll" },
          { type: "separator" },
          {
            label: "Speech",
            submenu: [{ role: "startSpeaking" }, { role: "stopSpeaking" }],
          },
        ] : [
          { role: "delete" },
          { type: "separator" },
          { role: "selectAll" },
        ]),
      ],
    },
    {
      label: "Chat",
      submenu: [
        { label: "New Chat", accelerator: "CmdOrCtrl+Shift+N", click: () => sendMenuCommand("new-chat") },
        { label: "Focus Prompt", accelerator: "CmdOrCtrl+L", click: () => sendMenuCommand("focus-prompt") },
      ],
    },
    {
      label: "View",
      submenu: [
        { role: "reload" },
        { role: "forceReload" },
        { role: "toggleDevTools" },
        { type: "separator" },
        { role: "resetZoom" },
        { role: "zoomIn" },
        { role: "zoomOut" },
        { type: "separator" },
        { role: "togglefullscreen" },
      ],
    },
    {
      label: "Window",
      submenu: [
        { role: "minimize" },
        { role: "zoom" },
        ...(process.platform === "darwin" ? [
          { type: "separator" },
          { role: "front" },
        ] : [
          { role: "close" },
        ]),
      ],
    },
    {
      label: "Help",
      submenu: [
        { label: "Focus Prompt", click: () => sendMenuCommand("focus-prompt") },
      ],
    },
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

function backendCommand() {
  const filename = process.platform === "win32" ? "golazo-backend.exe" : "golazo-backend";
  if (app.isPackaged) {
    const executable = path.join(process.resourcesPath, "backend", filename);
    return { executable, args: [] };
  }
  const executable = process.env.GOLAZO_BACKEND_EXECUTABLE || path.join(app.getAppPath(), "target", "debug", filename);
  return { executable, args: [] };
}

function codexExecutable() {
  if (process.env.CODEX_EXECUTABLE) return process.env.CODEX_EXECUTABLE;
  const macAppBinary = "/Applications/Codex.app/Contents/Resources/codex";
  return fs.existsSync(macAppBinary) ? macAppBinary : "codex";
}

function startBackend() {
  const command = backendCommand();
  backendProcess = spawn(command.executable, command.args, {
    cwd: process.env.CODEX_WORKSPACE_ROOT || app.getPath("home"),
    env: {
      ...process.env,
      CODEX_EXECUTABLE: codexExecutable(),
      GOLAZO_PORT: String(port),
      GOAL_MANAGER_BROWSE_ROOT: process.env.GOAL_MANAGER_BROWSE_ROOT || app.getPath("home"),
      GOAL_MANAGER_APP_DATA_DIR: process.env.GOAL_MANAGER_APP_DATA_DIR || app.getPath("userData"),
    },
    stdio: app.isPackaged ? "ignore" : "inherit",
  });
  backendProcess.once("exit", (code) => {
    if (!app.isQuitting && code !== 0) dialog.showErrorBox("Backend stopped", "The local Golazo service exited unexpectedly.");
  });
}

function waitForBackend(timeoutMs = 20000) {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const attempt = () => {
      const request = http.get(`${backendUrl}/health`, (response) => {
        response.resume();
        if (response.statusCode === 200) resolve();
        else retry();
      });
      request.on("error", retry);
      request.setTimeout(1000, () => request.destroy());
    };
    const retry = () => {
      if (Date.now() - started > timeoutMs) reject(new Error("Backend did not become ready"));
      else setTimeout(attempt, 200);
    };
    attempt();
  });
}

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1440,
    height: 900,
    minWidth: 940,
    minHeight: 650,
    title: "Golazo",
    icon: iconPath,
    backgroundColor: "#111311",
    titleBarStyle: process.platform === "darwin" ? "hiddenInset" : "default",
    webPreferences: {
      preload: path.join(__dirname, "preload.cjs"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });
  mainWindow.webContents.setWindowOpenHandler(() => ({ action: "deny" }));
  mainWindow.webContents.on("context-menu", (_event, params) => {
    const flags = params.editFlags || {};
    let template = [];
    if (params.isEditable) {
      template = [
        { role: "undo", enabled: Boolean(flags.canUndo) },
        { role: "redo", enabled: Boolean(flags.canRedo) },
        { type: "separator" },
        { role: "cut", enabled: Boolean(flags.canCut) },
        { role: "copy", enabled: Boolean(flags.canCopy) },
        { role: "paste", enabled: Boolean(flags.canPaste) },
        { type: "separator" },
        { role: "selectAll", enabled: Boolean(flags.canSelectAll) },
      ];
    } else if (params.selectionText) {
      template = [{ role: "copy", enabled: Boolean(flags.canCopy) }];
    }
    if (template.length) Menu.buildFromTemplate(template).popup({ window: mainWindow });
  });
  const rendererUrl = process.env.ELECTRON_RENDERER_URL || backendUrl;
  await mainWindow.loadURL(rendererUrl);
}

ipcMain.handle("dialog:select-directory", async (event, defaultPath) => {
  const owner = BrowserWindow.fromWebContents(event.sender);
  if (!owner || owner !== mainWindow) return null;
  const result = await dialog.showOpenDialog(owner, {
    title: "Add a Golazo project",
    defaultPath: typeof defaultPath === "string" ? defaultPath : app.getPath("home"),
    properties: ["openDirectory", "createDirectory"],
  });
  return result.canceled ? null : result.filePaths[0];
});

app.whenReady().then(async () => {
  try {
    if (process.platform === "darwin" && fs.existsSync(iconPath)) app.dock.setIcon(iconPath);
    startBackend();
    await waitForBackend();
    await createWindow();
    createMenu();
  } catch (error) {
    dialog.showErrorBox("Unable to start", String(error));
    app.quit();
  }
  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on("before-quit", () => {
  app.isQuitting = true;
  if (backendProcess && !backendProcess.killed) backendProcess.kill("SIGTERM");
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});
