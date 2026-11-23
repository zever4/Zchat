"use strict";

// Independent of chat/network startup: window controls also work while offline.
(function () {
  const bar = document.getElementById("desktopTitlebar");
  const content = document.getElementById("desktop-content");
  const maximize = document.getElementById("desktopMaximize");
  const status = document.getElementById("desktopWindowStatus");
  const invoke = window.__TAURI__.core.invoke;
  const displayState = state => {
    if (!state) return;
    bar.classList.toggle("is-maximized", state.maximized);
    const label = state.maximized ? "Восстановить размер" : "Развернуть";
    maximize.setAttribute("aria-label", label);
    maximize.title = label;
  };
  const control = async action => {
    try {
      status.textContent = "";
      displayState(await invoke("window_control", { action }));
    } catch { status.textContent = "Не удалось выполнить действие окна"; }
  };
  document.getElementById("desktopMinimize").addEventListener("click", () => { void control("minimize"); });
  maximize.addEventListener("click", () => { void control("toggle_maximize"); });
  document.getElementById("desktopClose").addEventListener("click", () => { void control("close"); });
  document.getElementById("desktopTitlebarDrag").addEventListener("mousedown", event => {
    if (event.button !== 0) return;
    event.preventDefault();
    void control(event.detail === 2 ? "toggle_maximize" : "drag");
  });
  const openSettings = async () => {
    try { await invoke("open_settings_window"); }
    catch { status.textContent = "Не удалось открыть настройки сервера"; }
  };
  document.getElementById("desktopServerSettings").addEventListener("click", openSettings);
  document.addEventListener("keydown", event => {
    if ((event.ctrlKey || event.metaKey) && !event.altKey && !event.shiftKey && event.key === ",") {
      event.preventDefault(); void openSettings();
    }
  });
  // Maximize/restore may also happen through Windows snapping or keyboard keys.
  let pendingState = false;
  const refresh = () => {
    if (pendingState) return;
    pendingState = true;
    invoke("window_control", { action: "state" }).then(displayState).catch(() => {}).finally(() => { pendingState = false; });
  };
  window.addEventListener("resize", refresh);
  window.addEventListener("focus", refresh);
  refresh();
  // showModal makes every node outside the dialog inert. Move this same header
  // into its top layer so minimize/close stay usable during image preparation.
  const syncModalHost = () => {
    const host = document.querySelector("dialog:modal") || document.body;
    if (bar.parentElement !== host) host.prepend(bar);
  };
  new MutationObserver(syncModalHost).observe(content, { attributes: true, attributeFilter: ["open"], subtree: true });
})();
