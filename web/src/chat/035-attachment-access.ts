// JWT stays in the Authorization header. Persisted message URLs contain no credentials.
function attachmentURL(raw: string, kind: "file" | MediaMessageTag): string | null {
  try {
    const url = new URL(raw, location.href);
    if (url.username || url.password || url.hash) return null;
    const folder = kind === "file" ? "files" : "media";
    if (!url.search && (window.zchatDesktop ? window.zchatDesktop.isApplicationURL(url.href) : url.origin === location.origin) && new RegExp("^/uploads/" + folder + "/[a-zA-Z0-9_.-]+$").test(url.pathname)) return url.pathname;
    if (kind === "gif" && url.protocol === "https:" &&
        (url.hostname === "media.tenor.com" || url.hostname === "giphy.com" || url.hostname.endsWith(".giphy.com"))) return url.href;
  } catch { /* Render invalid URLs as plain text. */ }
  return null;
}

const attachmentDisposers = new Map<HTMLElement, () => void>();
new MutationObserver(() => {
  for (const [owner, dispose] of attachmentDisposers) {
    if (!owner.isConnected) { dispose(); attachmentDisposers.delete(owner); }
  }
}).observe(document.body, { childList: true, subtree: true });

function loadProtectedAttachment(path: string, owner: HTMLElement, ready: (url: string) => void, cleanup?: () => void): void {
  const controller = new AbortController();
  let objectURL = "";
  let disposed = false;
  let observer: IntersectionObserver | null = null;
  attachmentDisposers.set(owner, () => {
    disposed = true;
    controller.abort();
    observer?.disconnect();
    cleanup?.();
    if (objectURL) URL.revokeObjectURL(objectURL);
  });
  const start = (): void => { void sessionFetch(path, { headers: { Authorization: "Bearer " + token }, signal: controller.signal, redirect: "error" })
    .then(async response => {
      if (!response.ok) throw new Error("Attachment unavailable");
      const blob = await response.blob();
      if (disposed) return;
      objectURL = URL.createObjectURL(blob);
      ready(objectURL);
    }).catch(() => { if (!disposed) owner.title = "Вложение недоступно"; }); };
  // Do not fetch entire image/audio histories that the user has not viewed.
  observer = new IntersectionObserver(entries => {
    if (entries.some(entry => entry.isIntersecting)) { observer?.disconnect(); start(); }
  }, { rootMargin: "200px" });
  observer.observe(owner);
}

async function downloadAttachment(path: string, name: string): Promise<void> {
  const response = await sessionFetch(path, { headers: { Authorization: "Bearer " + token }, redirect: "error" });
  if (!response.ok) throw new Error("Attachment unavailable");
  const url = URL.createObjectURL(await response.blob());
  const link = document.createElement("a");
  link.href = url;
  link.download = name || "attachment";
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}
