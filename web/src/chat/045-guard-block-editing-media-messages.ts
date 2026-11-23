// ---- Guard: block editing media messages ----
(function() {
    const _origShowCtx = typeof showContextMenu === "function" ? showContextMenu : null;
    if (!_origShowCtx) return;
    showContextMenu = function(x, y, msgId, chatId, el): void {
        if (el && el.dataset.mediaType) {
            _origShowCtx(x, y, msgId, chatId, el);
            // Remove edit button from context menu for media messages
            setTimeout(() => {
                const menu = document.querySelector<HTMLElement>(".ctx-menu");
                if (menu) {
                    const editBtn = menu.querySelector<HTMLElement>("[data-action='edit']");
                    if (editBtn) editBtn.remove();
                }
            }, 0);
            return;
        }
        _origShowCtx(x, y, msgId, chatId, el);
    };
})();

/* ============================================================
   ===== CALLS (LiveKit) =====
   ============================================================ */
