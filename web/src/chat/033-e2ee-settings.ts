// Explicit local-device setup. Status polling has no network/cryptographic authority.
let e2eeSettingsRevision = 0;
let e2eeSettingsTimer: ReturnType<typeof setTimeout> | null = null;
let e2eeSettingsBusy = false;
let e2eeReplacementContext: number | null = null;
function clearE2eeReplacement(): void {
    byId<HTMLInputElement>("desktopE2eePassword").value = "";
    byId<HTMLInputElement>("desktopE2eeConfirm").checked = false;
    byId<HTMLButtonElement>("desktopE2eeReplace").disabled = true;
}
function stopE2eeSettings(clear = true): void {
    if (clear) clearE2eeReplacement();
    e2eeSettingsRevision++;
    if (e2eeSettingsTimer !== null) clearTimeout(e2eeSettingsTimer);
    e2eeSettingsTimer = null;
}
function e2eeSettingsVisible(): boolean {
    return !!window.zchatDesktop?.e2ee?.compiled && !authExpiredHandled && localStorage.getItem("token") === token &&
        byId<HTMLDivElement>("settingsModal").style.display !== "none" && byId<HTMLDivElement>("settingsTabSecurity").classList.contains("active");
}
function e2eeSettingsMessage(status: DesktopE2eeStatus): string {
    const messages: Record<string, string> = {
        starting: "Проверяем ключи устройства…", stopping: "Завершаем предыдущую операцию…",
        setup_required: "Ключи для этого аккаунта ещё не настроены на этом ПК.",
        active: "Ключи устройства настроены.",
        replacement_required: "Этот аккаунт уже настроен в другой установке приложения.",
        replacement_pending: "Замена устройства не завершена. Для продолжения потребуется повторный ввод пароля.",
        device_unavailable: "Эти ключи больше не активны. Локальная история сохранена.",
        idle: "Войдите в аккаунт, чтобы настроить ключи.", closed: "Сессия завершена.", disabled: "Настройка E2EE недоступна.",
    };
    if (status.phase !== "failed") return messages[status.phase] || "Не удалось определить состояние ключей.";
    if (status.error === "password_invalid") return "Неверный пароль. Обновите состояние и введите пароль снова: сохранённая операция замены не потеряна.";
    if (status.error === "generation_changed") return "Активная установка изменилась. Обновите состояние и заново подтвердите замену.";
    if (status.error === "authentication") return "Войдите в аккаунт заново, чтобы продолжить.";
    if (status.error === "configuration" || status.error === "not_configured") return "Сквозное шифрование недоступно для этого сервера.";
    if (status.error === "unsupported_platform") return "Сквозное шифрование доступно в приложении для Windows.";
    if (status.error === "busy") return "Ключи заняты другой операцией или копией приложения. Повторите проверку позже.";
    if (status.error === "storage") return "Не удалось открыть сохранённые ключи. Повторите проверку.";
    if (status.error === "local_deadline" || status.error === "native_wait") return "Время ожидания истекло. Повторная проверка уточнит состояние сохранённой операции.";
    return "Не удалось завершить проверку ключей. Можно повторить попытку.";
}
function renderE2eeSettings(status: DesktopE2eeStatus): void {
    if (typeof observeE2eeArchiveStatus === "function") observeE2eeArchiveStatus(status);
    if (status.phase === "active") byId<HTMLDivElement>("desktopE2eeError").hidden = true;
    byId<HTMLParagraphElement>("desktopE2eeStatus").textContent = e2eeSettingsMessage(status);
    const setup = byId<HTMLButtonElement>("desktopE2eeSetup");
    const retry = byId<HTMLButtonElement>("desktopE2eeRetry");
    setup.hidden = status.phase !== "setup_required";
    const blocked = ["replacement_required", "replacement_pending", "device_unavailable"].includes(status.phase);
    retry.hidden = !blocked && (status.phase !== "failed" || ["authentication", "configuration", "not_configured", "unsupported_platform"].includes(status.error || ""));
    const replacement = status.replacement;
    byId<HTMLDivElement>("desktopE2eeReplacement").hidden = !blocked || !replacement;
    if (e2eeReplacementContext !== status.context || !blocked || !replacement) clearE2eeReplacement();
    e2eeReplacementContext = status.context;
    byId<HTMLParagraphElement>("desktopE2eeTarget").textContent = replacement ?
        `Заменяемая установка: ${replacement.expectedGeneration}. ` + (replacement.continuing ? "Продолжаем сохранённую операцию; повторная замена не создаётся." : replacement.retargeting ? "Предыдущая попытка относилась к другой установке. Это новая замена с новым подтверждением." : "") : "";
    byId<HTMLButtonElement>("desktopE2eeReplace").textContent = replacement?.continuing ? "Продолжить замену" : "Заменить E2EE-установку";
    updateE2eeReplaceButton();
    setup.disabled = retry.disabled = e2eeSettingsBusy;
}
async function refreshE2eeSettings(): Promise<void> {
    stopE2eeSettings(false);
    if (!e2eeSettingsVisible()) { clearE2eeReplacement(); return; }
    const revision = e2eeSettingsRevision;
    try {
        const status = await window.zchatDesktop!.e2ee!.status();
        if (revision !== e2eeSettingsRevision || !e2eeSettingsVisible()) return;
        renderE2eeSettings(status);
        // One chained local status read, only while this settings tab is visible.
        e2eeSettingsTimer = setTimeout(() => { void refreshE2eeSettings(); }, ["starting", "stopping"].includes(status.phase) ? 500 : 2000);
    } catch (error) {
        if (revision === e2eeSettingsRevision && e2eeSettingsVisible()) {
            byId<HTMLParagraphElement>("desktopE2eeStatus").textContent = "Не удалось обновить состояние ключей. Откройте раздел ещё раз.";
        }
    }
}
async function runE2eeSettingsAction(action: "setupDevice" | "retry" | "replaceDevice"): Promise<void> {
    const button = byId<HTMLButtonElement>(action === "setupDevice" ? "desktopE2eeSetup" : action === "replaceDevice" ? "desktopE2eeReplace" : "desktopE2eeRetry");
    if (e2eeSettingsBusy || !e2eeSettingsVisible() || button.hidden || button.disabled) return;
    if (action === "replaceDevice" && byId<HTMLDivElement>("desktopE2eeReplacement").hidden) return;
    let password = action === "replaceDevice" ? byId<HTMLInputElement>("desktopE2eePassword").value : "";
    const confirmed = byId<HTMLInputElement>("desktopE2eeConfirm").checked;
    clearE2eeReplacement();
    e2eeSettingsBusy = true;
    stopE2eeSettings();
    const revision = e2eeSettingsRevision;
    const error = byId<HTMLDivElement>("desktopE2eeError");
    error.hidden = true;
    byId<HTMLButtonElement>("desktopE2eeSetup").disabled = true;
    byId<HTMLButtonElement>("desktopE2eeRetry").disabled = true;
    try {
        const api = window.zchatDesktop!.e2ee!;
        const pending = action === "replaceDevice" ? api.replaceDevice(password, confirmed) : api[action]();
        password = ""; // DOM/native allocations are cleared; JS strings cannot be reliably wiped.
        const status = await pending;
        if (revision === e2eeSettingsRevision && e2eeSettingsVisible()) renderE2eeSettings(status);
    } catch (cause) {
        if (revision === e2eeSettingsRevision && e2eeSettingsVisible()) {
            error.textContent = "Не удалось получить результат. Проверьте состояние ещё раз.";
            error.hidden = false;
        }
    } finally {
        password = "";
        e2eeSettingsBusy = false;
        if (e2eeSettingsVisible()) void refreshE2eeSettings();
    }
}
const e2eeSettingsCard = document.getElementById("desktopE2eeSettings");
if (e2eeSettingsCard) e2eeSettingsCard.hidden = !window.zchatDesktop?.e2ee?.compiled;
document.getElementById("desktopE2eeSetup")?.addEventListener("click", () => { void runE2eeSettingsAction("setupDevice"); });
document.getElementById("desktopE2eeRetry")?.addEventListener("click", () => { void runE2eeSettingsAction("retry"); });

function updateE2eeReplaceButton(): void {
    byId<HTMLButtonElement>("desktopE2eeReplace").disabled = e2eeSettingsBusy ||
        !byId<HTMLInputElement>("desktopE2eeConfirm").checked || !byId<HTMLInputElement>("desktopE2eePassword").value;
}
document.getElementById("desktopE2eeReplace")?.addEventListener("click", () => { void runE2eeSettingsAction("replaceDevice"); });
document.getElementById("desktopE2eeConfirm")?.addEventListener("change", updateE2eeReplaceButton);
document.getElementById("desktopE2eePassword")?.addEventListener("input", updateE2eeReplaceButton);

window.addEventListener?.("pagehide", () => { stopE2eeSettings(); });
window.addEventListener?.("storage", event => {
    if ((event.key === "token" || event.key === null) && localStorage.getItem("token") !== token) stopE2eeSettings();
});
