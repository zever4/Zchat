type AppSound = "message" | "ring";
type AppSoundSlot = AppSound | "preview";

const maxSoundBytes = 2_000_000;
const soundLimits: Record<AppSound, number> = { message: 10, ring: 30 };
const customSounds: Record<AppSound, File | null> = { message: null, ring: null };
const defaultSoundBuffers = new Map<AppSound, AudioBuffer>();
const decodedSounds = new WeakMap<File, Promise<AudioBuffer>>();
interface AppSoundPlayback {
    source?: AudioBufferSourceNode;
    volume?: GainNode;
    release?: () => void;
}
const activeSounds = new Map<AppSoundSlot, AppSoundPlayback>();
let appAudio: AudioContext | null = null;
let soundPreferences: Promise<void> | null = null;
let protectedSoundBaseline: { context: number; revision: bigint } | null = null;

function soundSessionCurrent(): boolean {
    return !!token && !authExpiredHandled && localStorage.getItem("token") === token;
}

function soundStorageKey(kind: AppSound): string {
    const origin = new URL(window.zchatDesktop?.serverUrl || window.location.origin).origin;
    return accountStorageKey("zchat_sound:" + encodeURIComponent(origin) + ":" + kind);
}

async function soundFile(kind: AppSound, replacement?: File | null): Promise<File | null> {
    const key = soundStorageKey(kind);
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
        const request = indexedDB.open("zchat_notification_sounds", 1);
        request.onupgradeneeded = () => request.result.createObjectStore("sounds");
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
    try {
        return await new Promise<File | null>((resolve, reject) => {
            const tx = db.transaction("sounds", replacement === undefined ? "readonly" : "readwrite");
            const store = tx.objectStore("sounds");
            const request = replacement === undefined ? store.get(key) : replacement === null ? store.delete(key) : store.put(replacement, key);
            tx.oncomplete = () => resolve(request.result instanceof File ? request.result : null);
            tx.onabort = () => reject(tx.error);
        });
    } finally {
        db.close();
    }
}

function loadSoundPreferences(): Promise<void> {
    return soundPreferences ??= Promise.all((Object.keys(customSounds) as AppSound[]).map(async kind => {
        const file = await soundFile(kind).catch(() => null);
        if (soundSessionCurrent() && file && file.size > 0 && file.size <= maxSoundBytes) customSounds[kind] = file;
    })).then(() => {});
}

function getAppAudio(): AudioContext {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) throw new Error("audio_unavailable");
    return appAudio ??= new AudioCtx();
}

function defaultAppSound(ctx: AudioContext, kind: AppSound): AudioBuffer {
    const cached = defaultSoundBuffers.get(kind);
    if (cached) return cached;
    const rate = ctx.sampleRate;
    const ring = kind === "ring";
    const buffer = ctx.createBuffer(1, Math.ceil(rate * (ring ? 2.15 : .27)), rate);
    const samples = buffer.getChannelData(0);
    const notes = ring ? [[0, .23, 520], [.32, .24, 660]] : [[0, .11, 610], [.09, .15, 820]];
    for (const [start, duration, hz] of notes) {
        for (let i = 0; i < Math.floor(rate * duration); i++) {
            const t = i / rate;
            const envelope = Math.min(t / .013, (duration - t) / .052, 1);
            samples[Math.floor(start * rate) + i] += Math.sin(2 * Math.PI * hz * t) * envelope * .3;
        }
    }
    defaultSoundBuffers.set(kind, buffer);
    return buffer;
}

async function decodeAppSound(ctx: AudioContext, kind: AppSound, file: File): Promise<AudioBuffer> {
    if (!file.size || file.size > maxSoundBytes) throw new Error("sound_size");
    let decoded = decodedSounds.get(file);
    if (!decoded) {
        decoded = file.arrayBuffer().then(bytes => ctx.decodeAudioData(bytes));
        decodedSounds.set(file, decoded);
    }
    const buffer = await decoded;
    if (!(buffer.duration > 0) || buffer.duration > soundLimits[kind]) throw new Error("sound_duration");
    return buffer;
}

function stopAppSound(slot: AppSoundSlot): void {
    const playback = activeSounds.get(slot);
    if (!playback) return;
    activeSounds.delete(slot);
    if (playback.source) {
        playback.source.onended = null;
        try { playback.source.stop(); } catch { /* Already stopped. */ }
        playback.source.disconnect();
    }
    playback.volume?.disconnect();
    playback.release?.();
}

async function playAppSound(slot: AppSoundSlot, kind: AppSound, allowed: () => boolean): Promise<boolean> {
    if (!soundSessionCurrent() || !allowed() || activeSounds.has(slot)) return false;
    const playback: AppSoundPlayback = {};
    activeSounds.set(slot, playback);
    const valid = () => activeSounds.get(slot) === playback && soundSessionCurrent() && allowed();
    try {
        const ctx = getAppAudio();
        if (ctx.state !== "running") return false;
        await loadSoundPreferences();
        if (!valid()) return false;
        const file = customSounds[kind];
        const buffer = file ? await decodeAppSound(ctx, kind, file).catch(() => defaultAppSound(ctx, kind)) : defaultAppSound(ctx, kind);
        if (!valid()) return false;
        const start = () => {
            if (!valid()) return false;
            const source = ctx.createBufferSource();
            source.buffer = buffer;
            source.loop = slot === "ring";
            const volume = ctx.createGain();
            volume.gain.value = slot === "ring" ? .45 : .35;
            volume.connect(ctx.destination);
            source.connect(volume);
            source.onended = () => {
                if (activeSounds.get(slot) === playback) stopAppSound(slot);
            };
            playback.source = source;
            playback.volume = volume;
            try { source.start(); } catch (error) { stopAppSound(slot); throw error; }
            return true;
        };
        if (navigator.locks && slot === "ring") {
            // One tab rings for this account. Release the lock when the call stops.
            return await new Promise<boolean>((resolve, reject) => {
                void navigator.locks.request(soundStorageKey(kind), { ifAvailable: true }, async lock => {
                    if (!lock || !start()) { resolve(false); return; }
                    await new Promise<void>(release => { playback.release = release; resolve(true); });
                }).catch(reject);
            });
        }
        return start();
    } catch {
        return false;
    } finally {
        if (activeSounds.get(slot) === playback && !playback.source) stopAppSound(slot);
    }
}

function notificationAudioAllowed(): boolean {
    return userSettings.notification_sound && canShowAppNotifications();
}

function playNotificationSound(): void {
    void playAppSound("message", "message", notificationAudioAllowed);
}

function observeProtectedMessageSound(state: DesktopE2eeStatus): void {
    const revisionText = state.incomingMessageRevision;
    if (state.phase !== "active" || state.runtimePhase !== "Running" ||
        typeof revisionText !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(revisionText)) {
        protectedSoundBaseline = null;
        return;
    }
    const revision = BigInt(revisionText);
    const previous = protectedSoundBaseline;
    protectedSoundBaseline = { context: state.context, revision };
    if (previous?.context === state.context && revision > previous.revision) playNotificationSound();
}

function syncIncomingCallSound(): void {
    const allowed = () => callState === "ringing" && !!incomingCallData &&
        notificationAudioAllowed() && !isChatMutedByID(incomingCallData.chat_id);
    if (!allowed()) stopAppSound("ring");
    else void playAppSound("ring", "ring", allowed);
}

function syncNotificationSoundPolicy(): void {
    if (!notificationAudioAllowed()) stopAppSound("message");
    syncIncomingCallSound();
}

function unlockAppAudio(event: Event): void {
    if (!event.isTrusted || !soundSessionCurrent() || !notificationAudioAllowed()) return;
    try {
        const ctx = getAppAudio();
        if (ctx.state === "suspended") void ctx.resume().then(syncIncomingCallSound).catch(() => {});
    } catch { /* Playback is optional on platforms without Web Audio. */ }
}

function stopAllAppSounds(): void {
    for (const slot of activeSounds.keys()) stopAppSound(slot);
    protectedSoundBaseline = null;
}

document.addEventListener("pointerdown", unlockAppAudio, { capture: true, passive: true });
document.addEventListener("keydown", unlockAppAudio, true);
window.addEventListener("pagehide", stopAllAppSounds);
window.addEventListener("storage", event => {
    if ((event.key === "token" || event.key === null) && !soundSessionCurrent()) stopAllAppSounds();
});
