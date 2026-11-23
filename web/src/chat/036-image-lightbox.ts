// ---- Image lightbox ----
(function (): void {
  const overlay = document.createElement("div");
  overlay.className = "lightbox-overlay";
  overlay.style.display = "none";
  overlay.innerHTML = '<img class="lightbox-img" src="">' + '<button class="lightbox-close">&times;</button>';
  document.body.appendChild(overlay);

  const img = overlay.querySelector<HTMLImageElement>(".lightbox-img");
  const closeBtn = overlay.querySelector<HTMLButtonElement>(".lightbox-close");
  if (!img || !closeBtn) return;

  window.openLightbox = function(url: string): void {
    img.src = url;
    overlay.style.display = "flex";
  };

  closeBtn.addEventListener("click", () => {
    overlay.style.display = "none";
  });

  overlay.addEventListener("click", (e: MouseEvent) => {
    if (e.target === overlay) overlay.style.display = "none";
  });

  document.addEventListener("keydown", (e: KeyboardEvent) => {
    if (e.key === "Escape" && overlay.style.display !== "none") {
      overlay.style.display = "none";
    }
  });
})();
