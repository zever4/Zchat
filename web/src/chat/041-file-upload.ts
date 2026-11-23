// ---- File upload ----
(function (): void {
  const fileBtn = optionalById<HTMLButtonElement>("fileBtn");
  if (!fileBtn) return;

  const fileInput = document.createElement("input");
  fileInput.type = "file";
  fileInput.style.display = "none";
  document.body.appendChild(fileInput);

  fileBtn.addEventListener("click", () => {
    fileInput.click();
  });

  fileInput.addEventListener("change", async () => {
    const file = fileInput.files && fileInput.files[0];
    if (!file) return;
    fileInput.value = "";

    if (file.size > 50 * 1024 * 1024) {
      alert("Файл слишком большой (макс. 50 МБ)");
      return;
    }

    const uploadChatID = currentChatID;
    if (!uploadChatID) { alert("Сначала выберите чат"); return; }
    const fd = new FormData();
    fd.append("chat_id", String(uploadChatID));
    fd.append("file", file);

    fileBtn.classList.add("uploading");
    fileBtn.title = "Загрузка...";

    try {
      const res = await sessionFetch("/api/files/upload", {
        method: "POST",
        headers: { Authorization: "Bearer " + token },
        body: fd,
      });
      if (!res.ok) {
        const text = await res.text();
        alert("Ошибка загрузки: " + text);
        return;
      }
      const data = await res.json() as { name?: string; size?: number | string; url?: string };
      sendFileMessage(data, uploadChatID);
    } catch {
      alert("Ошибка сети при загрузке файла");
    } finally {
      fileBtn.classList.remove("uploading");
      fileBtn.title = "Прикрепить файл";
    }
  });
})();
