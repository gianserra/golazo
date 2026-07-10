const { contextBridge, ipcRenderer } = require("electron");

const menuCommands = new Set(["add-project", "rename-project", "new-goal", "new-chat", "focus-prompt"]);

contextBridge.exposeInMainWorld("desktop", {
  platform: process.platform,
  selectDirectory: (defaultPath) => ipcRenderer.invoke("dialog:select-directory", defaultPath),
  onMenuCommand: (callback) => {
    const listener = (_event, command) => {
      if (menuCommands.has(command)) callback(command);
    };
    ipcRenderer.on("menu:command", listener);
    return () => ipcRenderer.removeListener("menu:command", listener);
  },
});
