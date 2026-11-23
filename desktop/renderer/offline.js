"use strict";

const serverUrlEl = document.getElementById("serverUrl");
const errorDescriptionEl = document.getElementById("errorDescription");
const statusEl = document.getElementById("status");
const retryBtn = document.getElementById("retryBtn");
const settingsBtn = document.getElementById("settingsBtn");
const browserBtn = document.getElementById("browserBtn");

let currentServerUrl = "";

function setStatus(message, type) {
  statusEl.textContent = message || "";
  statusEl.className = "status" + (type ? " status-" + type : "");
}

async function bootstrap() {
  const config = await window.zchatDesktop.getConfig();
  currentServerUrl = config.serverUrl;
  serverUrlEl.textContent = config.serverUrl;

  const error = config.lastLoadError;
  if (error && error.description) {
    errorDescriptionEl.textContent = error.description + (error.code ? " (" + error.code + ")" : "");
  } else {
    errorDescriptionEl.textContent = "Сервер не ответил";
  }
}

retryBtn.addEventListener("click", async () => {
  setStatus("Пробуем подключиться снова...", "");
  try {
    await window.zchatDesktop.retryLoad();
  } catch (error) {
    setStatus(error instanceof Error ? error.message : "Повторное подключение не удалось", "error");
  }
});

settingsBtn.addEventListener("click", async () => {
  await window.zchatDesktop.openSettingsWindow();
});

browserBtn.addEventListener("click", async () => {
  if (!currentServerUrl) return;
  await window.zchatDesktop.openExternal(currentServerUrl + "/login.html");
});

bootstrap().catch((error) => {
  setStatus(error instanceof Error ? error.message : "Не удалось загрузить состояние приложения", "error");
});
