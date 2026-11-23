"use strict";

const serverUrlInput = document.getElementById("serverUrl");
const defaultServerUrlEl = document.getElementById("defaultServerUrl");
const appVersionEl = document.getElementById("appVersion");
const statusEl = document.getElementById("status");
const saveBtn = document.getElementById("saveBtn");
const resetBtn = document.getElementById("resetBtn");
const cancelBtn = document.getElementById("cancelBtn");

function setStatus(message, type) {
  statusEl.textContent = message || "";
  statusEl.className = "status" + (type ? " status-" + type : "");
}

async function bootstrap() {
  const config = await window.zchatDesktop.getConfig();
  serverUrlInput.value = config.serverUrl;
  defaultServerUrlEl.textContent = config.defaultServerUrl;
  appVersionEl.textContent = config.appVersion;
}

async function saveServerUrl() {
  setStatus("Подключаем desktop-клиент к серверу...", "");
  saveBtn.disabled = true;

  try {
    await window.zchatDesktop.setServerUrl(serverUrlInput.value);
    setStatus("Адрес сохранён. Приложение переподключается.", "success");
  } catch (error) {
    setStatus(error instanceof Error ? error.message : "Не удалось сохранить адрес сервера", "error");
    saveBtn.disabled = false;
  }
}

saveBtn.addEventListener("click", saveServerUrl);

resetBtn.addEventListener("click", async () => {
  try {
    const response = await window.zchatDesktop.resetServerUrl();
    serverUrlInput.value = response.serverUrl;
    setStatus("Адрес сброшен к значению по умолчанию.", "success");
  } catch (error) {
    setStatus(error instanceof Error ? error.message : "Не удалось сбросить настройки", "error");
  }
});

cancelBtn.addEventListener("click", async () => {
  await window.zchatDesktop.closeSettingsWindow();
});

serverUrlInput.addEventListener("keydown", async (event) => {
  if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
    await saveServerUrl();
  }
});

bootstrap().catch((error) => {
  setStatus(error instanceof Error ? error.message : "Не удалось загрузить настройки", "error");
});
