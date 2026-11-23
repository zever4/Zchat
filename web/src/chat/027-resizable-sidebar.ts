// ===== RESIZABLE SIDEBAR =====
(function() {
    const resizer = optionalById<HTMLDivElement>("sidebarResizer");
    if (!resizer) return;
    const minWidth = 200;
    const maxWidth = 500;
    const savedWidth = localStorage.getItem("sidebar_width");
    if (savedWidth) {
        document.body.style.setProperty("--sidebar-width", savedWidth + "px");
    }
    let isResizing = false;
    resizer.addEventListener("mousedown", (e: MouseEvent) => {
        isResizing = true;
        resizer.classList.add("active");
        document.body.style.cursor = "col-resize";
        document.body.style.userSelect = "none";
        e.preventDefault();
    });
    document.addEventListener("mousemove", (e: MouseEvent) => {
        if (!isResizing) return;
        let w = e.clientX;
        if (w < minWidth) w = minWidth;
        if (w > maxWidth) w = maxWidth;
        document.body.style.setProperty("--sidebar-width", w + "px");
    });
    document.addEventListener("mouseup", () => {
        if (!isResizing) return;
        isResizing = false;
        resizer.classList.remove("active");
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
        const cs = getComputedStyle(document.body).getPropertyValue("--sidebar-width");
        localStorage.setItem("sidebar_width", String(parseInt(cs, 10)));
    });
})();
