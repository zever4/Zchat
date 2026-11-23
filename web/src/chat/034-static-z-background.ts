// ===== STATIC Z BACKGROUND =====
(function generateZBackground() {
    const container = optionalById<HTMLDivElement>("zBackground");
    if (!container) return;
    const count = 45;
    for (let i = 0; i < count; i++) {
        const span = document.createElement("span");
        span.className = "z-bg-letter";
        span.textContent = "Z";
        span.style.left = (Math.random() * 100) + "%";
        span.style.top = (Math.random() * 100) + "%";
        span.style.fontSize = (30 + Math.random() * 60) + "px";
        span.style.opacity = String(0.06 + Math.random() * 0.08);
        span.style.transform = "rotate(" + (Math.random() * 60 - 30) + "deg)";
        container.appendChild(span);
    }
})();

// =============================================================================
