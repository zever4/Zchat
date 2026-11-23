// ---- GIF Picker (GIPHY API) ----
(function (): void {
  type GifPickerTab = "trending" | "favorites";
  type GifItem = { url: string; preview: string };
  type GiphyImageNode = { url?: string };
  type GiphyApiItem = {
    images?: {
      original?: GiphyImageNode;
      fixed_height_small?: GiphyImageNode;
      fixed_height?: GiphyImageNode;
    };
  };

  const GIPHY_KEY = "Gc7131jiJuvI7IdN0HZ1D7nh0ow5BU6g";
  const FAV_KEY = "zchat_gif_favorites";
  const MAX_FAVS = 50;

  const gifBtn = optionalById<HTMLButtonElement>("gifBtn");
  if (!gifBtn) return;

  const picker = document.createElement("div");
  picker.className = "gif-picker";
  picker.style.display = "none";
  picker.innerHTML =
    '<div class="gif-picker-header">' +
    '<div class="gif-tabs">' +
    '<button class="gif-tab active" data-tab="trending">Популярные</button>' +
    '<button class="gif-tab" data-tab="favorites">Избранное</button>' +
    "</div>" +
    '<input class="gif-search" type="text" placeholder="Поиск GIF...">' +
    "</div>" +
    '<div class="gif-results"></div>';

  const msgInput = queryOne<HTMLElement>(".message-input");
  if (!msgInput) return;
  msgInput.style.position = "relative";
  msgInput.appendChild(picker);

  const searchInput = picker.querySelector<HTMLInputElement>(".gif-search");
  const resultsEl = picker.querySelector<HTMLDivElement>(".gif-results");
  const tabs = Array.from(picker.querySelectorAll<HTMLButtonElement>(".gif-tab"));
  if (!searchInput || !resultsEl) return;

  let currentTab: GifPickerTab = "trending";
  let searchTimer: number | null = null;
  let gifPickerOpen = false;

  function isGifPickerTab(value: string | undefined): value is GifPickerTab {
    return value === "trending" || value === "favorites";
  }

  function getFavs(): string[] {
    try {
      const raw = JSON.parse(localStorage.getItem(FAV_KEY) || "[]") as unknown;
      if (!Array.isArray(raw)) return [];
      return raw.map((item) => String(item || "")).filter(Boolean);
    } catch {
      return [];
    }
  }

  function saveFavs(favs: string[]): void {
    localStorage.setItem(FAV_KEY, JSON.stringify(favs.slice(0, MAX_FAVS)));
  }

  function toggleFav(url: string): boolean {
    let favs = getFavs();
    let isNowFav = false;
    if (favs.includes(url)) {
      favs = favs.filter((item) => item !== url);
    } else {
      favs.unshift(url);
      isNowFav = true;
    }
    saveFavs(favs);
    return isNowFav;
  }

  gifBtn.addEventListener("click", (e: MouseEvent) => {
    e.stopPropagation();
    gifPickerOpen = !gifPickerOpen;
    picker.style.display = gifPickerOpen ? "flex" : "none";
    if (gifPickerOpen) {
      searchInput.value = "";
      setTab("trending");
      searchInput.focus();
    }
  });

  function closeGifPicker(): void {
    gifPickerOpen = false;
    picker.style.display = "none";
  }

  document.addEventListener("click", (e: MouseEvent) => {
    const target = e.target;
    if (gifPickerOpen && target instanceof Node && !picker.contains(target) && target !== gifBtn) {
      closeGifPicker();
    }
  });

  document.addEventListener(
    "keydown",
    (e: KeyboardEvent) => {
      if (e.key === "Escape" && gifPickerOpen) {
        closeGifPicker();
        e.stopImmediatePropagation();
      }
    },
    true,
  );

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const nextTab = tab.dataset.tab;
      if (isGifPickerTab(nextTab)) setTab(nextTab);
    });
  });

  function setTab(name: GifPickerTab): void {
    currentTab = name;
    tabs.forEach((tab) => tab.classList.toggle("active", tab.dataset.tab === name));
    if (name === "favorites") {
      renderFavoritesGifs();
    } else {
      void loadTrending();
    }
  }

  function renderGifs(gifs: GifItem[]): void {
    resultsEl.innerHTML = "";
    if (gifs.length === 0) {
      resultsEl.innerHTML =
        '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Ничего не найдено</div>';
      return;
    }

    gifs.forEach((gif) => {
      const item = document.createElement("div");
      item.className = "gif-result-item";

      const img = document.createElement("img");
      img.src = gif.preview || gif.url;
      img.loading = "lazy";
      item.appendChild(img);

      item.addEventListener("click", () => {
        sendMediaMessage("gif", gif.url);
        closeGifPicker();
      });

      item.addEventListener("contextmenu", (e: MouseEvent) => {
        e.preventDefault();
        const isNowFav = toggleFav(gif.url);
        showGifToast(isNowFav ? "Добавлено в избранное" : "Удалено из избранного");
        if (currentTab === "favorites") renderFavoritesGifs();
      });

      resultsEl.appendChild(item);
    });
  }

  function showGifToast(text: string): void {
    const toast = document.createElement("div");
    toast.className = "gif-toast";
    toast.textContent = text;
    picker.appendChild(toast);
    window.setTimeout(() => toast.remove(), 1500);
  }

  async function giphyFetch(endpoint: "trending" | "search", params: Record<string, string>): Promise<GifItem[]> {
    const qs = new URLSearchParams({
      api_key: GIPHY_KEY,
      limit: "30",
      rating: "g",
      ...params,
    });

    try {
      const res = await fetch("https://api.giphy.com/v1/gifs/" + endpoint + "?" + qs);
      if (!res.ok) return [];

      const data = await res.json() as { data?: GiphyApiItem[] };
      return (data.data || [])
        .map((item) => ({
          url: String(item.images?.original?.url || ""),
          preview: String(
            item.images?.fixed_height_small?.url ||
              item.images?.fixed_height?.url ||
              item.images?.original?.url ||
              "",
          ),
        }))
        .filter((gif) => Boolean(gif.url && gif.preview));
    } catch (e) {
      console.error("GIPHY error:", e);
      return [];
    }
  }

  async function loadTrending(): Promise<void> {
    resultsEl.innerHTML =
      '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Загрузка...</div>';
    const gifs = await giphyFetch("trending", {});
    renderGifs(gifs);
  }

  async function searchGifs(query: string): Promise<void> {
    if (!query.trim()) {
      await loadTrending();
      return;
    }
    resultsEl.innerHTML =
      '<div style="padding:20px;text-align:center;color:var(--text-secondary)">Поиск...</div>';
    const gifs = await giphyFetch("search", { q: query.trim() });
    renderGifs(gifs);
  }

  function renderFavoritesGifs(): void {
    const favs = getFavs();
    renderGifs(favs.map((url) => ({ url, preview: url })));
  }

  searchInput.addEventListener("input", () => {
    if (searchTimer !== null) window.clearTimeout(searchTimer);
    if (currentTab === "favorites") setTab("trending");
    searchTimer = window.setTimeout(() => {
      void searchGifs(searchInput.value);
    }, 400);
  });
})();
