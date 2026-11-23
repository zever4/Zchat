const soundSettingRows = Array.from(document.querySelectorAll<HTMLElement>("[data-notification-sound]"));

async function refreshNotificationSoundSettings(): Promise<void> {
    await loadSoundPreferences();
    if (soundSessionCurrent()) for (const row of soundSettingRows) renderNotificationSoundSetting(row);
}

function renderNotificationSoundSetting(row: HTMLElement): void {
    const kind = row.dataset.notificationSound as AppSound;
    const file = customSounds[kind];
    row.querySelector<HTMLElement>("[data-sound-name]")!.textContent = file?.name || "Стандартный";
    row.querySelector<HTMLButtonElement>("[data-sound-reset]")!.disabled = !file;
}

for (const row of soundSettingRows) {
    const kind = row.dataset.notificationSound as AppSound;
    const input = row.querySelector<HTMLInputElement>("[data-sound-file]")!;
    const pick = row.querySelector<HTMLButtonElement>("[data-sound-pick]")!;
    const preview = row.querySelector<HTMLButtonElement>("[data-sound-preview]")!;
    const reset = row.querySelector<HTMLButtonElement>("[data-sound-reset]")!;
    const notice = row.querySelector<HTMLElement>("[data-sound-status]")!;
    const save = async (file: File | null): Promise<void> => {
        if (!soundSessionCurrent() || pick.disabled) return;
        pick.disabled = preview.disabled = reset.disabled = true;
        notice.textContent = "";
        try {
            await loadSoundPreferences();
            if (file) await decodeAppSound(getAppAudio(), kind, file);
            if (!soundSessionCurrent()) return;
            await soundFile(kind, file);
            if (!soundSessionCurrent()) return;
            customSounds[kind] = file;
            stopAppSound("preview");
            if (kind === "ring") { stopAppSound("ring"); syncIncomingCallSound(); }
            notice.textContent = file ? "Звук сохранён на этом устройстве." : "Восстановлен стандартный звук.";
        } catch (error) {
            if (!soundSessionCurrent()) return;
            notice.textContent = error instanceof Error && error.message === "sound_size" ? "Выбери аудиофайл размером до 2 МБ."
                : error instanceof Error && error.message === "sound_duration" ? "Длительность записи до " + soundLimits[kind] + " с."
                : "Не удалось сохранить звук. Попробуй поддерживаемый аудиофайл.";
        } finally {
            if (soundSessionCurrent()) {
                pick.disabled = preview.disabled = false;
                renderNotificationSoundSetting(row);
            }
        }
    };
    pick.addEventListener("click", () => input.click());
    input.addEventListener("change", () => {
        const file = input.files?.[0];
        input.value = "";
        if (file) void save(file);
    });
    reset.addEventListener("click", () => { void save(null); });
    preview.addEventListener("click", async () => {
        notice.textContent = "";
        stopAppSound("preview");
        const visible = () => row.closest(".settings-tab-content")?.classList.contains("active") &&
            byId<HTMLElement>("settingsModal").style.display === "flex";
        try {
            const ctx = getAppAudio();
            if (ctx.state !== "running") await ctx.resume();
            if (await playAppSound("preview", kind, visible)) return;
        } catch { /* Keep the settings usable with audio disabled by the system. */ }
        if (visible() && soundSessionCurrent()) notice.textContent = "Не удалось воспроизвести звук.";
    });
}
