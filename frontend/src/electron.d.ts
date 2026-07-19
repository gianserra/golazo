export {};

type DesktopMenuCommand = "add-project" | "rename-project" | "new-goal" | "new-chat" | "focus-prompt";

declare global {
  interface Window {
    desktop?: {
      platform: string;
      selectDirectory: (defaultPath?: string) => Promise<string | null>;
      onMenuCommand: (callback: (command: DesktopMenuCommand) => void) => () => void;
    };
  }
}
