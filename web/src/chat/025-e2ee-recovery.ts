// Presentation and explicit user intent only. Native owns signing, durable
// freeze, retries and activation. There is no automatic mutation from polling.
type E2eeRecoveryAction = "propose" | DesktopRecoveryDecision;
interface E2eeRecoveryConsent {
    action: E2eeRecoveryAction;
    context: number;
    chat: string;
    epoch: string;
    own: string | null;
    proposal: string | null;
    stamp: string;
}
interface E2eeRecoveryPanel {
    state: DesktopRecoveryState | null;
    fresh: boolean;
    reading: boolean;
    revision: number;
    consent: E2eeRecoveryConsent | null;
    notice: string;
    verify: boolean;
    failures: number;
    skip: number;
    rows: string;
    buttons: HTMLButtonElement[];
}
let e2eeRecoveryReading = false;
let e2eeRecoveryFlight: { context: number; chat: string } | null = null;
function e2eeRecoveryVisible(view: E2eeHistoryView): boolean {
    return e2eeViewCurrent(view) && document.visibilityState !== "hidden" && !view.suspended;
}
function e2eeRecoverySameFlight(view: E2eeHistoryView): boolean {
    return e2eeRecoveryFlight?.context === view.reply.context && e2eeRecoveryFlight?.chat === view.reply.conversation.conversationId;
}
function initE2eeRecovery(view: E2eeHistoryView): void {
    view.recovery = { state: null, fresh: false, reading: false, revision: 0, consent: null,
        notice: "", verify: false, failures: 0, skip: 0, rows: "", buttons: [] };
    renderE2eeRecovery();
    void refreshE2eeRecovery(view);
}
function clearE2eeRecovery(): void {
    const panel = e2eeHistoryView?.recovery;
    if (panel) { panel.revision++; panel.fresh = false; panel.consent = null; panel.buttons = []; }
    byId<HTMLDetailsElement>("e2eeRecoveryPanel").open = false;
    byId("e2eeRecoveryPanel").hidden = true;
    byId("e2eeRecoveryProposals").replaceChildren();
    byId("e2eeRecoveryStatus").textContent = byId("e2eeRecoveryNotice").textContent = "";
    byId("e2eeRecoveryConsent").hidden = true;
    byId<HTMLInputElement>("e2eeRecoveryConfirm").checked = false;
    byId("e2eeRecoveryPrompt").textContent = "";
}
function invalidateE2eeRecovery(view: E2eeHistoryView): void {
    if (!view.recovery) return;
    view.recovery.revision++;
    view.recovery.fresh = false;
    view.recovery.consent = null;
    byId<HTMLInputElement>("e2eeRecoveryConfirm").checked = false;
}
function e2eeRecoveryStamp(view: E2eeHistoryView, state: DesktopRecoveryState): string {
    return JSON.stringify([view.reply.context, view.reply.conversation.conversationId, view.reply.conversation.state,
        state.cryptoEpoch, state.receiveOnly, state.sendingFrozen,
        state.proposals.map(p => [p.proposalId, p.state, p.ownProposal, p.decision, p.sendingFrozen]).sort((a, b) => String(a[0]).localeCompare(String(b[0])))]);
}
function e2eeRecoverySendBlock(view: E2eeHistoryView): string | null {
    const panel = view.recovery;
    if (!panel) return null; // Isolated components without recovery controls.
    if (e2eeRecoverySameFlight(view)) return "Сохраняем действие восстановления. Отправка временно приостановлена.";
    if (!panel.fresh || !panel.state) return "Проверяем состояние защищённой сессии. Отправка временно приостановлена.";
    if (panel.state.receiveOnly) return "Диалог доступен только для чтения. Для новой переписки откройте профиль собеседника.";
    if (panel.state.sendingFrozen) return "Отправка приостановлена на время восстановления. Черновик и старая история сохранены.";
    return null;
}
function e2eeRecoverySendBusy(view: E2eeHistoryView): boolean {
    return !!e2eeDrafts.get(e2eeDraftKey(view.reply.context, view.reply.conversation.conversationId))?.busy
        || (typeof e2eePhotoBusy === "function" && e2eePhotoBusy(view))
        || (typeof e2eeFileBusy === "function" && e2eeFileBusy(view))
        || (typeof e2eeVoiceBusy === "function" && e2eeVoiceBusy());
}
function e2eeRecoveryAllowed(state: DesktopRecoveryState, action: E2eeRecoveryAction, proposal: string | null): boolean {
    if (state.cryptoEpoch === null || state.receiveOnly) return false;
    if (action === "propose") {
        const terminal = (p: DesktopRecoveryProposal) => ["declined", "cancelled", "expired"].includes(p.state || "");
        return !state.sendingFrozen && state.proposals.every(terminal);
    }
    const p = state.proposals.find(p => p.proposalId === proposal);
    if (!p || p.decision !== null) return false;
    return action === "cancel" ? p.ownProposal && (p.state === null || p.state === "pending")
        : (action === "accept" || action === "decline") && !p.ownProposal && p.state === "pending";
}
function e2eeRecoveryProposalLabel(p: DesktopRecoveryProposal): string {
    const terminal: Record<string, string> = {
        accepted: "Восстановление принято. Завершаем проверку сохранённых сообщений и переход к новой сессии.",
        declined: "Запрос отклонён.", cancelled: "Запрос отменён.", expired: "Срок действия запроса истёк.",
        superseded: "Состояние запроса изменилось из-за другого восстановления.",
    };
    if (terminal[p.state || ""]) return terminal[p.state!];
    if (p.decision) return ({ accept: "Подтверждение сохранено", decline: "Отказ сохранён", cancel: "Отмена сохранена" })[p.decision]
        + " на устройстве. Ожидаем подтверждения сервера.";
    return p.ownProposal ? p.state === null ? "Запрос сохранён на устройстве. Ожидает отправки на сервер."
        : "Ожидаем решения собеседника." : "Собеседник предлагает восстановить защищённую сессию.";
}
const e2eeRecoveryActions: Record<E2eeRecoveryAction, string> = {
    propose: "Предложить восстановление", accept: "Подтвердить восстановление", decline: "Отклонить запрос", cancel: "Отменить свой запрос",
};
const e2eeRecoveryWarnings: Record<E2eeRecoveryAction, string> = {
    propose: "Предложить собеседнику восстановить защищённую сессию? Отправка новых сообщений будет приостановлена до его решения. Старая история сохранится; уже подготовленные сообщения останутся в прежней сессии.",
    accept: "Подтвердить восстановление? Отправка будет временно приостановлена. Переход начнётся после проверки сохранённых сообщений. Старая история останется доступна.",
    decline: "Отправить отказ на запрос собеседника? Итоговое состояние появится после ответа сервера.",
    cancel: "Отправить отмену своего запроса? До подтверждения сервером отправка может оставаться приостановленной. Если восстановление уже принято, отмена не гарантирована.",
};
function renderE2eeRecovery(): void {
    const view = e2eeHistoryView;
    const panel = view?.recovery;
    if (!view || !panel || !e2eeViewCurrent(view)) return;
    byId("e2eeRecoveryPanel").hidden = false;
    const state = panel.state;
    const available = e2eeRecoveryVisible(view) && view.reply.conversation.state !== "closed" && panel.fresh && !!state && !state.receiveOnly;
    if (panel.consent && (!available || panel.consent.stamp !== e2eeRecoveryStamp(view, state!))) {
        panel.consent = null;
        byId<HTMLInputElement>("e2eeRecoveryConfirm").checked = false;
    }
    const blocked = !available || !!e2eeRecoveryFlight || panel.reading || e2eeRecoverySendBusy(view);
    byId("e2eeRecoveryStatus").textContent = view.reply.conversation.state === "closed" || state?.receiveOnly
        ? "Диалог закрыт. История доступна для чтения; восстановление не заменяет создание нового диалога."
        : !panel.fresh ? "Проверяем состояние защищённой сессии…"
        : state?.cryptoEpoch === null ? "Сессия ещё не установлена. Она начнётся при первой отправке сообщения."
        : state?.sendingFrozen ? "Отправка приостановлена до завершения восстановления."
        : "Отправка не приостановлена восстановлением.";
    byId("e2eeRecoveryNotice").textContent = e2eeRecoverySameFlight(view) ? "Сохраняем действие на этом устройстве…" : panel.notice;
    const propose = byId<HTMLButtonElement>("e2eeRecoveryPropose");
    propose.disabled = blocked || !state || !e2eeRecoveryAllowed(state, "propose", null);
    byId<HTMLButtonElement>("e2eeRecoveryRefresh").disabled = !e2eeRecoveryVisible(view) || e2eeRecoveryReading || e2eeRecoverySameFlight(view);
    const rows = JSON.stringify(state && e2eeRecoveryStamp(view, state));
    if (panel.rows !== rows) {
        panel.rows = rows; panel.buttons = [];
        const root = byId("e2eeRecoveryProposals"); root.replaceChildren();
        for (const p of state?.proposals || []) {
            const row = document.createElement("article"); row.className = "e2ee-recovery-request";
            const heading = document.createElement("strong"); heading.textContent = p.ownProposal ? "Ваш запрос" : "Запрос собеседника";
            const description = document.createElement("p"); description.textContent = e2eeRecoveryProposalLabel(p);
            const actions = document.createElement("div"); actions.className = "e2ee-recovery-actions";
            for (const action of (p.ownProposal ? ["cancel"] : ["accept", "decline"]) as E2eeRecoveryAction[]) {
                if (!e2eeRecoveryAllowed(state!, action, p.proposalId)) continue;
                const button = document.createElement("button"); button.type = "button";
                button.textContent = e2eeRecoveryActions[action]; button.disabled = blocked;
                button.dataset.recoveryAction = action;
                button.addEventListener("click", () => { if (e2eeHistoryView === view) beginE2eeRecovery(action, p.proposalId); });
                panel.buttons.push(button); actions.append(button);
            }
            row.append(heading, description, actions); root.append(row);
        }
    }
    for (const button of panel.buttons) button.disabled = blocked;
    byId("e2eeRecoveryConsent").hidden = !panel.consent;
    byId("e2eeRecoveryPrompt").textContent = panel.consent ? e2eeRecoveryWarnings[panel.consent.action] : "";
    const confirm = byId<HTMLButtonElement>("e2eeRecoverySubmit");
    confirm.textContent = panel.consent ? e2eeRecoveryActions[panel.consent.action] : "Подтвердить";
    confirm.disabled = blocked || !panel.consent || !byId<HTMLInputElement>("e2eeRecoveryConfirm").checked;
}
function beginE2eeRecovery(action: E2eeRecoveryAction, proposal: string | null = null): void {
    const view = e2eeHistoryView; const panel = view?.recovery; const state = panel?.state;
    if (!view || !panel || !state || !panel.fresh || panel.reading || e2eeRecoveryFlight || e2eeRecoverySendBusy(view)
        || !e2eeRecoveryVisible(view) || view.reply.conversation.state === "closed" || !e2eeRecoveryAllowed(state, action, proposal)) return;
    panel.consent = Object.freeze({ action, proposal, context: view.reply.context, chat: state.conversationId,
        epoch: state.cryptoEpoch!, own: state.proposals.find(p => p.ownProposal)?.proposalId || null, stamp: e2eeRecoveryStamp(view, state) });
    byId<HTMLInputElement>("e2eeRecoveryConfirm").checked = false;
    byId<HTMLDetailsElement>("e2eeRecoveryPanel").open = true;
    renderE2eeRecovery();
}
async function refreshE2eeRecovery(view: E2eeHistoryView, current: () => boolean = () => true, force: boolean = false): Promise<void> {
    const panel = view.recovery;
    if (!panel || !e2eeRecoveryVisible(view) || !current() || e2eeRecoveryReading || e2eeRecoverySameFlight(view)) return;
    if (!force && panel.skip > 0) { panel.skip--; return; }
    e2eeRecoveryReading = panel.reading = true;
    const revision = panel.revision;
    const valid = () => current() && e2eeRecoveryVisible(view) && view.recovery === panel && panel.revision === revision;
    renderE2eeRecovery();
    try {
        const response = await window.zchatDesktop!.e2ee!.recoveryStatus(view.reply.conversation.conversationId);
        if (!valid()) return;
        if (response.context !== view.reply.context || response.recovery.conversationId !== view.reply.conversation.conversationId) throw "stale";
        const state = response.recovery;
        const old = panel.state;
        if (old?.cryptoEpoch !== null && old?.cryptoEpoch !== undefined
            && (state.cryptoEpoch === null || BigInt(state.cryptoEpoch) < BigInt(old.cryptoEpoch))) throw "stale";
        const advanced = old?.cryptoEpoch != null && state.cryptoEpoch !== null && BigInt(state.cryptoEpoch) > BigInt(old.cryptoEpoch);
        if (panel.consent && panel.consent.stamp !== e2eeRecoveryStamp(view, state)) {
            panel.consent = null; byId<HTMLInputElement>("e2eeRecoveryConfirm").checked = false;
            panel.notice = "Состояние переписки изменилось. Выберите действие и подтвердите его заново.";
        }
        const peer = state.proposals.find(p => !p.ownProposal && p.state === "pending" && !p.decision);
        if (peer && !old?.proposals.some(p => p.proposalId === peer.proposalId && p.state === "pending" && !p.decision)) byId<HTMLDetailsElement>("e2eeRecoveryPanel").open = true;
        panel.state = { ...state, proposals: state.proposals.map(p => ({ ...p })) };
        panel.fresh = true; panel.failures = panel.skip = 0;
        if (advanced) panel.notice = "Защищённая сессия обновлена. Старая история сохранена.";
        else if (panel.verify) panel.notice = "Локальное состояние проверено. Результат показан в панели; действие автоматически не повторяется.";
        panel.verify = false;
    } catch (error) {
        if (!valid()) return;
        panel.fresh = false; panel.consent = null; byId<HTMLInputElement>("e2eeRecoveryConfirm").checked = false;
        panel.failures = Math.min(3, panel.failures + 1); panel.skip = 2 ** panel.failures - 1;
        panel.notice = "Не удалось проверить состояние сессии. Отправка приостановлена; история и черновик сохранены. Повторите проверку.";
        if (error === "stale" || (error as { name?: string })?.name === "AbortError") pauseE2eeView("Состояние устройства изменилось. Откройте диалог заново.");
    } finally {
        e2eeRecoveryReading = panel.reading = false;
        if (e2eeViewCurrent(view) && document.visibilityState !== "hidden") renderE2eeComposer();
        else renderE2eeRecovery();
    }
}
async function submitE2eeRecovery(): Promise<void> {
    const view = e2eeHistoryView; const panel = view?.recovery; const consent = panel?.consent;
    if (!view || !panel || !consent || !panel.fresh || !panel.state || panel.reading || e2eeRecoveryFlight
        || !e2eeRecoveryVisible(view) || e2eeRecoverySendBusy(view) || view.reply.conversation.state === "closed"
        || consent.stamp !== e2eeRecoveryStamp(view, panel.state) || !byId<HTMLInputElement>("e2eeRecoveryConfirm").checked) return;
    const flight = { context: consent.context, chat: consent.chat }; e2eeRecoveryFlight = flight;
    invalidateE2eeRecovery(view); panel.notice = ""; renderE2eeComposer();
    let notice = "Действие сохранено на устройстве. Ожидаем подтверждения сервера и собеседника.";
    try {
        const api = window.zchatDesktop!.e2ee!;
        const response = consent.action === "propose"
            ? await api.proposeRecovery({ conversationId: consent.chat, expectedEpoch: consent.epoch, expectedOwnProposalId: consent.own }, consent.context, true)
            : await api.decideRecovery({ conversationId: consent.chat, expectedEpoch: consent.epoch, proposalId: consent.proposal!, decision: consent.action }, consent.context, true);
        if (response.context !== consent.context || response.recovery.conversationId !== consent.chat || response.recovery.cryptoEpoch !== consent.epoch) throw "stale";
        if (consent.action === "cancel") notice = "Отмена сохранена на устройстве. До ответа сервера отправка может оставаться приостановленной.";
    } catch (error) {
        notice = error === "conflict" || error === "recovery_required" ? "Состояние сессии изменилось. Проверим его; для нового действия потребуется новое подтверждение."
            : error === "conversation_closed" ? "Диалог закрыт. Для новой переписки откройте профиль собеседника."
            : "Результат действия пока неизвестен. Проверим сохранённое состояние; автоматического повтора не будет.";
    } finally {
        if (e2eeRecoveryFlight === flight) e2eeRecoveryFlight = null;
        const selected = e2eeHistoryView;
        if (selected && e2eeViewCurrent(selected) && selected.reply.context === consent.context && selected.reply.conversation.conversationId === consent.chat && selected.recovery) {
            invalidateE2eeRecovery(selected); selected.recovery.notice = notice;
            selected.recovery.verify = true; selected.recovery.skip = 0;
            renderE2eeComposer(); void refreshE2eeRecovery(selected, () => true, true);
        } else renderE2eeRecovery();
    }
}
byId("e2eeRecoveryPropose").addEventListener("click", () => beginE2eeRecovery("propose"));
byId("e2eeRecoveryRefresh").addEventListener("click", () => { if (e2eeHistoryView) void refreshE2eeRecovery(e2eeHistoryView, () => true, true); });
byId("e2eeRecoveryConfirm").addEventListener("change", renderE2eeRecovery);
byId("e2eeRecoverySubmit").addEventListener("click", () => { void submitE2eeRecovery(); });
byId("e2eeRecoveryBack").addEventListener("click", () => {
    if (e2eeHistoryView?.recovery) e2eeHistoryView.recovery.consent = null;
    byId<HTMLInputElement>("e2eeRecoveryConfirm").checked = false; renderE2eeRecovery();
});
document.addEventListener("visibilitychange", () => {
    if (e2eeHistoryView && document.visibilityState === "hidden") { invalidateE2eeRecovery(e2eeHistoryView); renderE2eeComposer(); }
});
