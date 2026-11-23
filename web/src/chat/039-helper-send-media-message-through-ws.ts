// ---- Helper: send media message through WS ----
function sendFileMessage(data: { name?: string; size?: number | string; url?: string }, chatID: number): void {
  const name = String(data.name || "").replace(/[:\[\]]/g, "_");
  if (!queueOrdinaryMessage(chatID, "[file:" + name + ":" + String(data.size || 0) + "]" + String(data.url || "") + "[/file]")) {
    throw new Error("Не удалось поставить вложение в очередь отправки");
  }
}

function sendMediaMessage(tag: MediaMessageTag, url: string, chatID: number | null = currentChatID): void {
  if (!chatID || !queueOrdinaryMessage(chatID, "[" + tag + "]" + url + "[/" + tag + "]")) {
    throw new Error("Не удалось поставить вложение в очередь отправки");
  }
}
