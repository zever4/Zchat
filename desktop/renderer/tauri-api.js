"use strict";

(function () {
  function invoke(command, args) {
    const tauri = window.__TAURI__;
    if (!tauri || !tauri.core || typeof tauri.core.invoke !== "function") {
      return Promise.reject(new Error("Tauri API недоступен"));
    }
    return tauri.core.invoke(command, args);
  }

  window.zchatDesktop = {
    isDesktop: true,
    getConfig: () => invoke("get_config"),
    setServerUrl: (serverUrl) => invoke("set_server_url", { serverUrl }),
    resetServerUrl: () => invoke("reset_server_url"),
    retryLoad: () => invoke("retry_load"),
    openSettingsWindow: () => invoke("open_settings_window"),
    closeSettingsWindow: () => invoke("close_settings_window"),
    openExternal: (targetUrl) => invoke("open_external", { targetUrl }),
  };
})();
