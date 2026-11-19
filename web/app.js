"use strict";

const $ = (id) => document.getElementById(id);
const icon = (name, extra = "") => `<svg class="icon ${extra}" aria-hidden="true"><use href="#i-${name}"/></svg>`;
const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const number = new Intl.NumberFormat("fr-FR");
const decimal = new Intl.NumberFormat("fr-FR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const timeFormat = new Intl.DateTimeFormat("fr-MA", { timeZone: "Africa/Casablanca", hour: "2-digit", minute: "2-digit" });
const secondFormat = new Intl.DateTimeFormat("fr-MA", { timeZone: "Africa/Casablanca", hour: "2-digit", minute: "2-digit", second: "2-digit" });
const dateFormat = new Intl.DateTimeFormat("fr-MA", { timeZone: "Africa/Casablanca", day: "2-digit", month: "short" });
const fullDate = new Intl.DateTimeFormat("fr-MA", { timeZone: "Africa/Casablanca", day: "numeric", month: "long", year: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" });
const RULES = {
  MONTANT_ELEVE: { label: "Montant élevé", icon: "coin", level: "high", indicator: "", severity: "Élevée", color: "#ff7417" },
  VOYAGE_IMPOSSIBLE: { label: "Déplacement impossible", icon: "pin", level: "critical", indicator: "critical", severity: "Critique", color: "#e5484d" },
  FREQUENCE_ELEVEE: { label: "Fréquence inhabituelle", icon: "clock", level: "medium", indicator: "medium", severity: "Modérée", color: "#f5a524" },
};
const SEVERITIES = {
  critique: { label: "Critique", key: "critique", badge: "severity-critical", color: "#e5484d" },
  "élevée": { label: "Élevée", key: "elevee", badge: "severity-high", color: "#ff7417" },
  "modérée": { label: "Modérée", key: "moderee", badge: "severity-medium", color: "#f5a524" },
};
const PERIOD_SECONDS = { "15m": 900, "1h": 3600, "24h": 86400 };
const STATUS = {
  nouvelle: { label: "Nouvelle", className: "status-new" },
  en_cours: { label: "En cours", className: "status-progress" },
  confirmee: { label: "Confirmée", className: "status-confirmed" },
  ecartee: { label: "Écartée", className: "status-dismissed" },
};
const PAGES = {
  overview: { title: "Vue d’ensemble", subtitle: "Transactions et alertes sur {period}." },
  alerts: { title: "Alertes", subtitle: "Retrouvez les événements suspects et suivez leur traitement." },
  transactions: { title: "Transactions", subtitle: "Consultez le flux financier et identifiez les opérations signalées." },
  demo: { title: "Atelier de démo", subtitle: "Faites varier un scénario et observez précisément ce qui déclenche une alerte." },
  pipeline: { title: "Pipeline & règles", subtitle: "Comprenez la chaîne de traitement et vérifiez les détections." },
  guide: { title: "Guide du projet", subtitle: "Les repères essentiels pour utiliser et présenter la console." },
};
const PERIOD_TEXT = { "15m": "les 15 dernières minutes", "1h": "les 60 dernières minutes", "24h": "les dernières 24 heures" };
const state = { view: "overview", period: "1h", data: null, tab: "all", alertPage: 1, transactionPage: 1, detail: null, busy: false, request: 0,
  scenario: "montant", lastRun: null, scenarioBusy: false, detailList: [] };
let toastTimer;
const relatedIds = (alert) => alert.related_transaction_ids || [alert.transaction_id];
const flaggedIds = () => new Set(state.data.alerts.flatMap(relatedIds));
try { const saved = sessionStorage.getItem("vigie-period"); if (Object.hasOwn(PERIOD_TEXT, saved)) state.period = saved; } catch { /* Navigation privée : conserver la valeur par défaut. */ }
const normalizeSearch = (value) => String(value).normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLocaleLowerCase("fr");

async function api(path, body) {
  const response = await fetch(path, body === undefined ? { cache: "no-store" } : {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  });
  let data;
  try { data = await response.json(); } catch { throw new Error("La console a reçu une réponse illisible."); }
  if (!response.ok) {
    const error = new Error(data.error || "La requête n’a pas abouti.");
    error.mode = data.mode;
    throw error;
  }
  return data;
}

function toast(message, isError = false) {
  clearTimeout(toastTimer);
  $("toast").textContent = message;
  $("toast").classList.toggle("error", isError);
  $("toast").hidden = false;
  toastTimer = setTimeout(() => { $("toast").hidden = true; }, 5000);
}

function counted(value, singular, plural = singular + "s") {
  return `${number.format(value)} ${value === 1 ? singular : plural}`;
}

function statusBadge(status) {
  const info = STATUS[status] || STATUS.nouvelle;
  return `<span class="badge ${info.className}">${info.label}</span>`;
}

function severityBadge(severity) {
  const info = SEVERITIES[severity] || SEVERITIES["modérée"];
  return `<span class="badge ${info.badge}">${info.label}</span>`;
}

function amountCell(amount) { return `<span class="amount-cell">${escapeHTML(decimal.format(amount))}<small>DH</small></span>`; }
function dateCell(value) {
  const date = new Date(value);
  return `<span class="date-cell" title="${escapeHTML(fullDate.format(date))}"><strong>${secondFormat.format(date)}</strong><small>${dateFormat.format(date)}</small></span>`;
}
function emptyRow(columns, title, text) {
  return `<tr><td colspan="${columns}" class="empty-cell"><strong>${escapeHTML(title)}</strong>${escapeHTML(text)}</td></tr>`;
}

function route() {
  const previousView = state.view;
  const next = location.hash.slice(1) || "overview";
  state.view = Object.hasOwn(PAGES, next) ? next : "overview";
  const page = PAGES[state.view];
  $("page-title").textContent = page.title;
  $("page-subtitle").textContent = page.subtitle.replace("{period}", PERIOD_TEXT[state.period]);
  document.title = `${page.title} — Vigie`;
  document.querySelectorAll(".nav-item").forEach((item) => {
    const active = item.dataset.view === state.view;
    item.classList.toggle("active", active);
    if (active) item.setAttribute("aria-current", "page"); else item.removeAttribute("aria-current");
  });
  for (const key of Object.keys(PAGES)) $("view-" + key).hidden = state.view !== key || !state.data;
  $("export-button").hidden = ["pipeline", "guide", "demo"].includes(state.view);
  $("period").closest(".period-control").hidden = ["pipeline", "guide", "demo"].includes(state.view);
  $("freshness-chip").hidden = ["pipeline", "guide", "demo"].includes(state.view);
  closePopovers();
  if (["pipeline", "guide", "demo"].includes(state.view)) {
    $("view-" + state.view).hidden = false;
    $("loading-state").hidden = true;
  }
  closeMenu();
  if (state.view === "pipeline") refreshHealth();
  if (previousView !== state.view) window.scrollTo(0, 0);
}

function periodBuckets() {
  // Découpe les données réellement reçues selon les 30 intervalles de la série du serveur.
  const data = state.data, series = data.series;
  const seconds = PERIOD_SECONDS[data.period] || PERIOD_SECONDS[state.period];
  const since = new Date(series[0]?.timestamp || data.generated_at).getTime();
  const step = seconds * 1000 / Math.max(1, series.length);
  const buckets = series.map((point) => ({ transactions: point.transactions, alerts: point.alerts, volume: 0, flagged: 0 }));
  const ids = flaggedIds();
  const indexOf = (value) => Math.min(buckets.length - 1, Math.max(0, Math.floor((new Date(value).getTime() - since) / step)));
  for (const tx of data.transactions) {
    const bucket = buckets[indexOf(tx.timestamp)];
    if (!bucket) continue;
    bucket.volume += tx.amount;
    if (ids.has(tx.transaction_id)) bucket.flagged += 1;
  }
  return buckets;
}

function halves(values) {
  const middle = Math.floor(values.length / 2);
  const sum = (list) => list.reduce((total, value) => total + value, 0);
  return [sum(values.slice(0, middle)), sum(values.slice(middle))];
}

function trendChip(id, previous, current, { higherIsBad = false, points = false } = {}) {
  const element = $(id);
  let text, direction = 0;
  if (points) {
    const delta = current - previous;
    direction = Math.abs(delta) < 0.05 ? 0 : Math.sign(delta);
    text = direction ? `${delta > 0 ? "+" : "−"}${decimal.format(Math.abs(delta))} pt` : "Stable";
  } else if (!previous && !current) {
    text = "Stable";
  } else if (!previous) {
    direction = 1; text = "Nouveau";
  } else {
    const change = (current - previous) / previous * 100;
    direction = Math.abs(change) < 1 ? 0 : Math.sign(change);
    text = direction ? `${change > 0 ? "+" : "−"}${number.format(Math.round(Math.abs(change)))} %` : "Stable";
  }
  // Hausse des alertes ou du taux : défavorable ; baisse du volume : neutre.
  const tone = !direction ? "" : direction > 0 ? (higherIsBad ? "bad" : "good") : (higherIsBad ? "good" : "");
  element.className = `trend ${tone}`;
  element.innerHTML = `${icon(direction > 0 ? "trend-up" : direction < 0 ? "trend-down" : "minus")}${text}`;
  element.title = "Seconde moitié de la période comparée à la première";
}

function sparkline(id, values, color) {
  const width = 200, height = 46, max = Math.max(1, ...values);
  const step = width / Math.max(1, values.length - 1);
  const points = values.map((value, i) => [i * step, height - 4 - value / max * (height - 10)]);
  const line = points.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const gradient = `g-${id}`;
  $(id).innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"><defs><linearGradient id="${gradient}" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="${color}" stop-opacity=".22"/><stop offset="1" stop-color="${color}" stop-opacity="0"/></linearGradient></defs><path d="${line} L${width},${height} L0,${height} Z" fill="url(#${gradient})"/><path d="${line}" fill="none" stroke="${color}" stroke-width="2" vector-effect="non-scaling-stroke" stroke-linejoin="round" stroke-linecap="round"/></svg>`;
}

function renderKpis() {
  const summary = state.data.summary;
  const buckets = periodBuckets();
  const transactions = buckets.map((b) => b.transactions), alerts = buckets.map((b) => b.alerts);
  const volume = buckets.map((b) => b.volume), rate = buckets.map((b) => b.transactions ? b.flagged / b.transactions * 100 : 0);
  trendChip("trend-transactions", ...halves(transactions));
  trendChip("trend-alerts", ...halves(alerts), { higherIsBad: true });
  trendChip("trend-volume", ...halves(volume));
  const [txBefore, txAfter] = halves(transactions);
  const [flaggedBefore, flaggedAfter] = halves(buckets.map((b) => b.flagged));
  trendChip("trend-rate", txBefore ? flaggedBefore / txBefore * 100 : 0, txAfter ? flaggedAfter / txAfter * 100 : 0, { higherIsBad: true, points: true });
  sparkline("spark-transactions", transactions, "#ff7417");
  sparkline("spark-alerts", alerts, "#e5484d");
  sparkline("spark-volume", volume, "#ff7417");
  sparkline("spark-rate", rate, "#e5484d");
  $("kpi-transactions").textContent = number.format(summary.transactions);
  $("kpi-alerts").textContent = number.format(summary.alerts);
  $("kpi-volume").innerHTML = `${escapeHTML(number.format(Math.round(summary.volume)))}<small>DH</small>`;
  $("kpi-volume").title = `${decimal.format(summary.volume)} DH`;
  $("kpi-rate").innerHTML = `${escapeHTML(decimal.format(summary.risk_rate))}<small>%</small>`;
  $("kpi-clients").textContent = `${counted(summary.clients, "client distinct", "clients distincts")}`;
  $("kpi-open").textContent = `${number.format(summary.open_alerts)} à traiter`;
  $("kpi-critical").textContent = `${counted(summary.critical_alerts, "critique")}`;
  $("kpi-flagged").textContent = `${counted(summary.flagged_transactions, "transaction concernée", "transactions concernées")}`;
}

function renderRiskOverview() {
  const data = state.data, summary = data.summary, alerts = data.alerts, total = alerts.length;
  $("exposure-value").innerHTML = `${escapeHTML(decimal.format(summary.exposure))}<small>DH</small>`;
  const levels = Object.entries(SEVERITIES).map(([severity, info]) => {
    const rows = alerts.filter((alert) => alert.severity === severity);
    return { severity, info, count: rows.length, open: rows.filter((alert) => ["nouvelle", "en_cours"].includes(alert.status)).length };
  });
  $("severity-bar").innerHTML = total ? levels.filter((level) => level.count).map((level) => `<span class="${level.info.key}" style="width:${level.count / total * 100}%" title="${level.info.label} : ${level.count}"></span>`).join("") : "";
  $("severity-legend").innerHTML = levels.map((level) => `<button class="severity-item" data-severity="${escapeHTML(level.severity)}" title="Voir les alertes de niveau ${level.info.label.toLocaleLowerCase("fr")}"><span><i style="background:${level.info.color}"></i>${level.info.label}</span><strong>${number.format(level.count)}</strong><small>${level.open ? counted(level.open, "à traiter", "à traiter") : "Aucune à traiter"}</small></button>`).join("");
  const inProgress = alerts.filter((alert) => alert.status === "en_cours").length;
  const closed = alerts.filter((alert) => ["confirmee", "ecartee"].includes(alert.status)).length;
  const clients = new Set(alerts.map((alert) => alert.card_holder_id)).size;
  const stat = (iconName, tone, value, label) => `<div class="risk-stat"><span class="card-icon ${tone}">${icon(iconName)}</span><span><strong>${number.format(value)}</strong><small>${label}</small></span></div>`;
  $("risk-stats").innerHTML = stat("alert", "", summary.open_alerts, "À traiter ou en cours") + stat("activity", "blue", inProgress, "En cours d’analyse") +
    stat("check", "teal", closed, "Clôturées") + stat("users", "danger", clients, clients === 1 ? "Client concerné" : "Clients concernés");
}

function renderPriority() {
  const data = state.data;
  const open = data.alerts.filter((alert) => ["nouvelle", "en_cours"].includes(alert.status));
  const critical = open.filter((alert) => alert.severity === "critique");
  $("priority-title").textContent = open.length ? `${counted(open.length, "alerte")} à examiner` : (data.alerts.length ? "Toutes les alertes sont traitées" : "Aucune alerte sur la période");
  $("priority-count").textContent = number.format(critical.length);
  $("priority-critical").disabled = !critical.length;
  $("priority-open").disabled = !open.length;
  $("critical-list").innerHTML = critical.slice(0, 3).map((alert) => `<li><button class="critical-item" data-alert="${escapeHTML(alert.alert_id)}"><span><strong>${escapeHTML(alert.card_holder_id)}</strong><small>${escapeHTML(alert.previous_location ? alert.previous_location + " → " + alert.location : alert.location)} · ${timeFormat.format(new Date(alert.timestamp))}</small></span>${amountCell(alert.amount)}</button></li>`).join("") ||
    `<li class="critical-empty">${icon("check")}Aucune alerte critique en attente.</li>`;
}

function renderIndicators() {
  const data = state.data, summary = data.summary, alerts = data.alerts;
  const average = alerts.length ? alerts.reduce((total, alert) => total + alert.amount, 0) / alerts.length : 0;
  const highest = alerts.reduce((best, alert) => (!best || alert.amount > best.amount ? alert : best), null);
  const city = [...data.cities].sort((a, b) => b.alerts - a.alerts || b.count - a.count)[0];
  const flaggedClients = new Set(alerts.map((alert) => alert.card_holder_id)).size;
  const row = (iconName, tone, label, note, value, meter) => `<li class="indicator"><span class="card-icon ${tone}">${icon(iconName)}</span><span>${label}<small>${note}</small></span><strong>${value}</strong>${meter === undefined ? "" : `<span class="meter" aria-hidden="true"><span style="width:${Math.min(100, meter)}%"></span></span>`}</li>`;
  $("risk-indicators").innerHTML = [
    row("percent", "", "Transactions signalées", `${counted(summary.flagged_transactions, "transaction")} sur ${number.format(summary.transactions)}`, `${decimal.format(summary.risk_rate)} %`, summary.risk_rate),
    row("coin", "", "Montant moyen par alerte", alerts.length ? `Sur ${counted(alerts.length, "alerte")}` : "Aucune alerte", `${number.format(Math.round(average))} DH`),
    row("wallet", "danger", "Alerte la plus élevée", highest ? `${escapeHTML(highest.rule_label)} · ${escapeHTML(highest.location)}` : "Aucune alerte", highest ? `${number.format(Math.round(highest.amount))} DH` : "—"),
    row("pin", "danger", "Ville la plus exposée", city && city.alerts ? `${counted(city.alerts, "alerte")} · ${counted(city.count, "transaction")}` : "Aucune alerte localisée", city && city.alerts ? escapeHTML(city.name) : "—"),
    row("users", "", "Clients signalés", `Sur ${counted(summary.clients, "client actif", "clients actifs")}`, number.format(flaggedClients), summary.clients ? flaggedClients / summary.clients * 100 : 0),
  ].join("");
}

function renderNotifications() {
  const open = state.data.alerts.filter((alert) => ["nouvelle", "en_cours"].includes(alert.status));
  const count = state.data.summary.open_alerts;
  $("notifications-count").textContent = count > 99 ? "99+" : number.format(count);
  $("notifications-count").hidden = !count;
  $("notifications-button").setAttribute("aria-label", count ? `Notifications : ${counted(count, "alerte")} à traiter` : "Notifications");
  $("notifications-summary").textContent = count ? `${counted(count, "alerte")} à traiter` : "Aucune alerte à traiter";
  $("notifications-list").innerHTML = open.slice(0, 6).map((alert) => {
    const rule = RULES[alert.rule] || RULES.MONTANT_ELEVE;
    return `<button class="notification-item" data-alert="${escapeHTML(alert.alert_id)}"><span class="rule-indicator ${rule.indicator}">${icon(rule.icon)}</span><span><strong>${escapeHTML(rule.label)}</strong><small>${escapeHTML(alert.card_holder_id)} · ${escapeHTML(alert.location)} · ${timeFormat.format(new Date(alert.timestamp))}</small></span>${amountCell(alert.amount)}</button>`;
  }).join("") || `<p class="popover-empty">Aucune alerte en attente sur la période.</p>`;
  $("notifications-all").disabled = !count;
}

function renderOverview() {
  const data = state.data;
  renderKpis();
  renderRiskOverview();
  renderPriority();
  renderActivity();
  renderDistribution();
  renderIndicators();
  $("recent-alerts").innerHTML = data.alerts.slice(0, 6).map((alert) => alertRow(alert, true)).join("") || emptyRow(6, "Aucune alerte sur cette période", "Les prochaines détections apparaîtront ici.");
  const cities = data.cities.filter((city) => city.count > 0).sort((a, b) => b.count - a.count).slice(0, 8);
  const maximum = Math.max(1, ...cities.map((city) => city.count));
  $("city-list").innerHTML = cities.map((city) => `<button class="city-row" data-city="${escapeHTML(city.name)}" title="Voir les transactions à ${escapeHTML(city.name)}"><span class="city-row-info"><strong>${escapeHTML(city.name)}</strong><small class="${city.alerts ? "has-alerts" : ""}">${city.alerts ? counted(city.alerts, "alerte") : "Aucune alerte"}</small></span><span class="city-bar" aria-hidden="true"><span style="width:${city.count / maximum * 100}%"></span></span><span class="city-total">${number.format(city.count)}</span></button>`).join("") || `<p class="empty-cell">Aucune transaction sur cette période.</p>`;
}

function renderActivity() {
  const series = state.data.series;
  const width = Math.max(290, $("activity-chart").clientWidth), height = innerWidth <= 600 ? 200 : 240, left = 38, right = 8, top = 14, bottom = 28;
  const innerWidth_ = width - left - right, innerHeight = height - top - bottom;
  const maxValue = Math.max(4, ...series.map((point) => point.transactions), ...series.map((point) => point.alerts));
  const max = Math.ceil(maxValue / 4) * 4;
  const gap = innerWidth_ / series.length;
  const cx = (i) => left + i * gap + gap / 2;
  const y = (value) => top + innerHeight - value / max * innerHeight;
  let svg = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"><title>Transactions et alertes pendant la période sélectionnée</title><defs><linearGradient id="activity-fill" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#ff7417" stop-opacity=".26"/><stop offset="1" stop-color="#ff7417" stop-opacity="0"/></linearGradient></defs>`;
  for (let i = 0; i <= 4; i++) {
    const value = i * max / 4;
    svg += `<line x1="${left}" x2="${width - right}" y1="${y(value)}" y2="${y(value)}" stroke="#eef1f5" ${i ? "" : 'stroke="#dfe3ea"'}/><text x="${left - 10}" y="${y(value) + 3.5}" text-anchor="end">${number.format(value)}</text>`;
  }
  const line = series.map((point, i) => `${i ? "L" : "M"}${cx(i).toFixed(1)},${y(point.transactions).toFixed(1)}`).join(" ");
  svg += `<path d="${line} L${cx(series.length - 1).toFixed(1)},${y(0)} L${cx(0).toFixed(1)},${y(0)} Z" fill="url(#activity-fill)"/>`;
  series.forEach((point, i) => {
    if (point.alerts > 0) svg += `<rect x="${cx(i) - Math.min(9, gap * .5) / 2}" y="${y(point.alerts)}" width="${Math.min(9, gap * .5)}" height="${Math.max(2, point.alerts / max * innerHeight)}" rx="2.5" fill="#e5484d"/>`;
  });
  svg += `<path d="${line}" fill="none" stroke="#ff7417" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/>`;
  series.forEach((point, i) => {
    if (i % 6 === 0 || i === series.length - 1) svg += `<text x="${cx(i)}" y="${height - 8}" text-anchor="middle">${timeFormat.format(new Date(point.timestamp))}</text>`;
    svg += `<rect x="${left + i * gap}" y="${top}" width="${gap}" height="${innerHeight}" fill="transparent"><title>${timeFormat.format(new Date(point.timestamp))} · ${counted(point.transactions, "transaction")} · ${counted(point.alerts, "alerte")}</title></rect>`;
  });
  $("activity-chart").innerHTML = svg + "</svg>";
  const seconds = { "15m": 30, "1h": 120, "24h": 2880 }[state.period];
  $("chart-caption").textContent = `Intervalles de ${seconds < 60 ? seconds + " secondes" : seconds / 60 + " minutes"} · Heure de Casablanca`;
}

function renderDistribution() {
  const total = state.data.summary.alerts;
  const items = state.data.distribution;
  let offset = 0;
  const segments = items.filter((item) => item.count).map((item) => {
    const share = item.count / Math.max(1, total) * 100;
    const segment = `<circle cx="21" cy="21" r="15.9155" fill="none" stroke="${RULES[item.rule].color}" stroke-width="5.2" stroke-dasharray="${Math.max(0, share - (items.filter((i) => i.count).length > 1 ? 1.2 : 0))} ${100 - share + (items.filter((i) => i.count).length > 1 ? 1.2 : 0)}" stroke-dashoffset="${-offset}"><title>${escapeHTML(item.label)} : ${item.count}</title></circle>`;
    offset += share;
    return segment;
  }).join("");
  $("distribution-chart").innerHTML = `<svg viewBox="0 0 42 42"><circle cx="21" cy="21" r="15.9155" fill="none" stroke="#eef1f5" stroke-width="5.2"/>${segments}</svg><div class="donut-center"><strong>${number.format(total)}</strong><small>${total === 1 ? "alerte" : "alertes"}</small></div>`;
  $("distribution-legend").innerHTML = items.map((item) => `<button class="distribution-row" data-rule="${item.rule}" title="Voir les alertes : ${escapeHTML(item.label)}"><i style="background:${RULES[item.rule].color}"></i><span>${escapeHTML(item.label)}</span><strong>${number.format(item.count)}</strong><span class="percent">${total ? Math.round(item.count / total * 100) : 0} %</span></button>`).join("");
}

function alertRow(alert, compact = false) {
  const rule = RULES[alert.rule] || RULES.MONTANT_ELEVE;
  const title = `<span class="rule-cell"><span class="rule-indicator ${rule.indicator}">${icon(rule.icon)}</span><span><strong>${escapeHTML(rule.label)}</strong><small>${compact ? escapeHTML(alert.card_holder_id) + " · " + timeFormat.format(new Date(alert.timestamp)) : escapeHTML(alert.transaction_id)}</small></span></span>`;
  return `<tr class="alert-row" data-alert="${escapeHTML(alert.alert_id)}"><td>${title}</td><td>${severityBadge(alert.severity)}</td>${compact ? "" : `<td class="mono">${escapeHTML(alert.card_holder_id)}</td>`}<td><span class="location-cell">${icon("pin")}${escapeHTML(alert.location)}</span></td><td class="align-right">${amountCell(alert.amount)}</td>${compact ? "" : `<td>${dateCell(alert.timestamp)}</td>`}<td>${statusBadge(alert.status)}</td><td><button class="detail-button" data-alert="${escapeHTML(alert.alert_id)}" aria-label="Ouvrir l’alerte ${escapeHTML(alert.card_holder_id)}, ${escapeHTML(rule.label)}">${icon("chevron")}</button></td></tr>`;
}

function filteredAlerts() {
  if (!state.data) return [];
  const search = normalizeSearch($("alert-search").value.trim());
  const rule = $("rule-filter").value, severity = $("severity-filter").value;
  const rows = state.data.alerts.filter((alert) => {
    const tabMatches = state.tab === "all" || (state.tab === "open" && ["nouvelle", "en_cours"].includes(alert.status)) ||
      (state.tab === "closed" && ["confirmee", "ecartee"].includes(alert.status)) || alert.status === state.tab;
    return tabMatches && (rule === "all" || alert.rule === rule) && (severity === "all" || alert.severity === severity) &&
      (!search || normalizeSearch([alert.card_holder_id, alert.transaction_id, alert.location, alert.rule_label, alert.alert_id].join(" ")).includes(search));
  });
  const sort = $("alert-sort").value;
  if (sort === "priority") {
    const priority = { critique: 0, "élevée": 1, "modérée": 2 };
    rows.sort((a, b) => priority[a.severity] - priority[b.severity] || b.timestamp.localeCompare(a.timestamp));
  } else if (sort === "amount") rows.sort((a, b) => b.amount - a.amount || b.timestamp.localeCompare(a.timestamp));
  return rows;
}

function renderAlerts() {
  document.querySelectorAll("[data-tab]").forEach((button) => {
    const selected = button.dataset.tab === state.tab;
    button.classList.toggle("selected", selected);
    button.setAttribute("aria-pressed", String(selected));
  });
  const alerts = filteredAlerts(), total = state.data.alerts.length;
  $("tab-all").textContent = number.format(total);
  $("tab-open").textContent = number.format(state.data.summary.open_alerts);
  $("clear-alert-filters").hidden = !($("alert-search").value || $("rule-filter").value !== "all" || $("severity-filter").value !== "all" || state.tab !== "all" || $("alert-sort").value !== "date");
  $("alert-summary").innerHTML = `<span><strong>${number.format(total)}</strong>alertes sur la période</span><span><strong>${number.format(state.data.summary.critical_alerts)}</strong>critiques</span><span><strong>${number.format(state.data.summary.open_alerts)}</strong>à traiter ou en cours</span>`;
  state.alertPage = renderPagination("alert", state.alertPage, alerts.length);
  $("all-alerts").innerHTML = alerts.slice((state.alertPage - 1) * 12, state.alertPage * 12).map((alert) => alertRow(alert)).join("") || emptyRow(8, "Aucune alerte ne correspond à cette sélection", "Modifiez les filtres, la recherche ou la période d’analyse.");
}

function filteredTransactions() {
  if (!state.data) return [];
  const search = normalizeSearch($("transaction-search").value.trim());
  const filter = $("transaction-filter").value;
  const ids = flaggedIds();
  return state.data.transactions.filter((tx) => (filter === "all" || (filter === "flagged" ? ids.has(tx.transaction_id) : !ids.has(tx.transaction_id))) &&
    (!search || normalizeSearch([tx.card_holder_id, tx.transaction_id, tx.location, tx.merchant_type].join(" ")).includes(search)));
}

function renderTransactions() {
  const transactions = filteredTransactions(), summary = state.data.summary;
  const ids = flaggedIds();
  $("clear-transaction-filters").hidden = !($("transaction-search").value || $("transaction-filter").value !== "all");
  $("transaction-summary").innerHTML = `<span><strong>${number.format(summary.transactions)}</strong>transactions</span><span><strong>${number.format(summary.clients)}</strong>clients</span><span><strong>${decimal.format(summary.volume)}</strong>DH sur la période</span>`;
  state.transactionPage = renderPagination("transaction", state.transactionPage, transactions.length);
  $("all-transactions").innerHTML = transactions.slice((state.transactionPage - 1) * 12, state.transactionPage * 12).map((tx) => `<tr><td class="mono">${escapeHTML(tx.transaction_id)}</td><td class="mono">${escapeHTML(tx.card_holder_id)}</td><td>${escapeHTML(tx.merchant_type)}</td><td><span class="location-cell">${icon("pin")}${escapeHTML(tx.location)}</span></td><td class="align-right">${amountCell(tx.amount)}</td><td>${dateCell(tx.timestamp)}</td><td>${ids.has(tx.transaction_id) ? '<span class="badge status-new">Signalée</span>' : '<span class="badge status-clean">Sans alerte</span>'}</td></tr>`).join("") || emptyRow(7, "Aucune transaction ne correspond à cette sélection", "Modifiez les filtres, la recherche ou la période d’analyse.");
}

function renderPagination(prefix, currentPage, total) {
  const pages = Math.max(1, Math.ceil(total / 12));
  const page = Math.max(1, Math.min(currentPage, pages));
  $(prefix + "-result-count").textContent = total ? `${(page - 1) * 12 + 1}–${Math.min(page * 12, total)} sur ${counted(total, "résultat")}` : "Aucun résultat";
  $(prefix + "-page").textContent = `${page} / ${pages}`;
  $(prefix + "-prev").disabled = page === 1;
  $(prefix + "-next").disabled = page === pages;
  return page;
}

function setEnvironment(mode) {
  $("demo-notice").hidden = mode !== "demo";
  $("scenario-panel").hidden = mode !== "demo";
  $("demo-nav").hidden = mode !== "demo";
  $("settings-pause").hidden = mode !== "demo";
  if (mode !== "demo" && state.view === "demo") location.hash = "overview";
  $("mode-badge").className = `mode-badge ${mode === "demo" ? "demo" : "cluster"}`;
  $("mode-badge").innerHTML = `<span class="status-dot ${mode === "demo" ? "amber" : "green"}"></span><span class="mode-label">${mode === "demo" ? "Mode démonstration" : "Cluster Cassandra"}</span><span class="mode-short">${mode === "demo" ? "Démo" : "Cluster"}</span>`;
  $("mode-badge").title = mode === "demo" ? "Mode démonstration : transactions simulées" : "Mode cluster : données lues dans Cassandra";
}

function render() {
  const data = state.data;
  $("loading-state").hidden = true;
  setEnvironment(data.mode);
  const notices = [];
  if (data.summary.pending_transactions) notices.push(`${counted(data.summary.pending_transactions, "transaction associée", "transactions associées")} aux alertes hors du journal chargé : hors période ou en attente de lecture. Les montants portent uniquement sur les transactions affichées.`);
  if (data.summary.legacy_alerts) notices.push(`${counted(data.summary.legacy_alerts, "alerte historique")} sans liste complète des transactions associées. Les indicateurs de signalement peuvent être sous-estimés.`);
  if (data.truncated) notices.push("La consultation est limitée aux 10 000 événements les plus récents par type. Réduisez la période pour affiner les résultats.");
  $("consistency-notice").textContent = notices.join(" ");
  $("consistency-notice").hidden = notices.length === 0;
  $("nav-alert-count").textContent = number.format(data.summary.open_alerts);
  $("pause-button").innerHTML = `${icon(data.paused ? "play" : "pause")}<span>${data.paused ? "Reprendre le flux" : "Mettre en pause"}</span>`;
  $("demo-notice-title").textContent = data.paused ? "Flux automatique en pause." : "Mode démonstration.";
  $("demo-notice-copy").textContent = data.paused ? "Les essais manuels restent disponibles." : "Transactions simulées, règles de détection actives.";
  $("demo-pause-button").textContent = data.paused ? "Reprendre" : "Mettre en pause";
  $("settings-pause-label").textContent = data.paused ? "Reprendre le flux" : "Mettre le flux en pause";
  $("settings-pause").querySelector("use").setAttribute("href", data.paused ? "#i-play" : "#i-pause");
  $("demo-flow-status").textContent = data.paused ? "Flux automatique en pause" : "Flux automatique actif";
  $("demo-flow-dot").classList.toggle("paused", data.paused);
  if (data.interval) $("demo-speed").value = String(data.interval);
  $("recent-foot").textContent = data.truncated ? "Affichage limité aux 10 000 événements les plus récents par type" : "Les six plus récentes · actualisation toutes les 5 secondes";
  renderOverview(); renderAlerts(); renderTransactions(); renderNotifications();
  for (const view of Object.keys(PAGES)) $("view-" + view).hidden = state.view !== view;
}

async function refresh(force = false) {
  if (state.busy && !force) return;
  if (!force && $("alert-dialog").open) return;
  const request = ++state.request;
  const period = state.period;
  state.busy = true;
  $("refresh-button").disabled = true;
  $("refresh-button").classList.add("is-refreshing");
  try {
    const data = await api(`/api/snapshot?period=${encodeURIComponent(period)}`);
    if (request !== state.request) return;
    state.data = data;
    $("connection-error").hidden = true;
    $("data-freshness").textContent = `Données reçues à ${secondFormat.format(new Date(data.generated_at))} · Casablanca`;
    $("data-freshness").classList.remove("stale");
    $("freshness-text").textContent = `En direct · ${secondFormat.format(new Date(data.generated_at))}`;
    $("freshness-chip").classList.remove("stale");
    render();
  } catch (error) {
    if (request !== state.request) return;
    if (error.mode) setEnvironment(error.mode);
    $("connection-error-text").textContent = `${error.message} ${state.data ? "Les dernières données reçues restent affichées." : "Une nouvelle tentative aura lieu automatiquement."}`;
    $("connection-error").hidden = false;
    $("data-freshness").textContent = state.data ? "Connexion interrompue · données conservées" : "Données indisponibles";
    $("data-freshness").classList.add("stale");
    $("freshness-text").textContent = state.data ? "Connexion interrompue" : "Données indisponibles";
    $("freshness-chip").classList.add("stale");
    $("loading-state").hidden = true;
  } finally {
    if (request === state.request) {
      state.busy = false;
      $("refresh-button").disabled = false;
      $("refresh-button").classList.remove("is-refreshing");
    }
  }
}

async function refreshHealth() {
  $("health-refresh").disabled = true;
  try {
    const health = await api("/api/health");
    for (const service of health.services) {
      const element = $("health-" + service.name);
      element.textContent = { simulation: "Simulation locale", joignable: "Port joignable", actif: "Application active", operationnel: "Opérationnel", en_attente: "En attente", indisponible: "Indisponible" }[service.state] || "Non vérifié";
      element.dataset.state = service.state;
      element.title = service.detail;
    }
    if (!state.data) setEnvironment(health.mode);
  } catch (error) { toast(error.message, true); }
  finally { $("health-refresh").disabled = false; }
}

function openAlert(id, origin = "view") {
  const alert = state.data?.alerts.find((item) => item.alert_id === id) || state.lastRun?.detected_alerts.find((item) => item.alert_id === id);
  if (!alert) return toast("Cette alerte ne figure plus dans la période sélectionnée.", true);
  if (origin !== "retain") {
    const source = origin === "demo" ? state.lastRun.detected_alerts : state.view === "alerts" ? filteredAlerts() : state.data.alerts;
    state.detailList = source.map((item) => item.alert_id);
  }
  const position = state.detailList.indexOf(id);
  $("detail-position").textContent = `${position + 1} / ${state.detailList.length}`;
  $("detail-prev").disabled = position <= 0;
  $("detail-next").disabled = position >= state.detailList.length - 1;
  state.detail = alert;
  const rule = RULES[alert.rule];
  const detail = (label, value, extra = "") => `<div><dt>${escapeHTML(label)}</dt><dd class="${extra}">${escapeHTML(value)}</dd></div>`;
  $("alert-detail").innerHTML = `<div class="detail-title"><span class="rule-indicator ${rule.indicator}">${icon(rule.icon)}</span><div><h2 id="dialog-title">${escapeHTML(rule.label)}</h2><span class="badge severity-${rule.level}">${rule.severity}</span></div></div><div class="detail-amount">${escapeHTML(decimal.format(alert.amount))}<small>DH</small></div><div class="detail-reason"><strong>MOTIF DE DÉTECTION</strong>${escapeHTML(alert.detail)}</div><dl class="detail-data">${detail("Client", alert.card_holder_id, "mono")}${detail("Transaction", alert.transaction_id, "mono")}${detail("Date et heure", fullDate.format(new Date(alert.timestamp)))}${detail("Ville", alert.location)}${detail("Commerce", alert.merchant_type)}${alert.previous_location ? detail("Ville précédente", alert.previous_location) : ""}${alert.distance_km ? detail("Distance", decimal.format(alert.distance_km) + " km") : ""}${alert.speed_kmh ? detail("Vitesse estimée", number.format(alert.speed_kmh) + " km/h") : ""}${alert.transaction_count ? detail("Transactions dans la fenêtre", alert.transaction_count) : ""}${detail("Identifiant d’alerte", alert.alert_id, "mono alert-identity")}</dl>`;
  $("review-status").value = alert.status;
  const transactions = new Map([...(state.data?.transactions || []), ...(state.lastRun?.events || [])].map((tx) => [tx.transaction_id, tx]));
  $("alert-detail").insertAdjacentHTML("beforeend", `<section class="evidence"><h3>Transactions associées <span>${relatedIds(alert).length}</span></h3><p>${alert.related_transaction_ids ? "Opérations utilisées par cette détection." : "Historique ancien : seule la transaction déclenchante est disponible."}</p><ol>${relatedIds(alert).map((id) => {
    const tx = transactions.get(id);
    return `<li><code>${escapeHTML(id)}</code>${tx ? `<div><span>${escapeHTML(tx.location)} · ${secondFormat.format(new Date(tx.timestamp))}</span><strong>${decimal.format(tx.amount)} DH</strong></div>` : '<small>Hors de la période ou du journal chargé</small>'}</li>`;
  }).join("")}</ol></section>`);
  if (!$("alert-dialog").open) $("alert-dialog").showModal();
}

function csvValue(value) {
  let text = String(value ?? "");
  if (/^\s*[=+@\-]|^[\t\r\n]/.test(text)) text = "'" + text;
  return '"' + text.replaceAll('"', '""') + '"';
}

function exportCSV() {
  if (!state.data) return toast("Aucune donnée à exporter.", true);
  let rows;
  const environment = state.data.mode === "demo" ? "Démonstration" : "Cluster";
  if (state.view === "transactions") {
    rows = [["Environnement", "Transaction", "Client", "Montant (DH)", "Ville", "Commerce", "Horodatage UTC"],
      ...filteredTransactions().map((tx) => [environment, tx.transaction_id, tx.card_holder_id, tx.amount, tx.location, tx.merchant_type, tx.timestamp])];
  } else {
    const alerts = state.view === "alerts" ? filteredAlerts() : state.data.alerts;
    rows = [["Environnement", "Alerte", "Règle", "Niveau", "Client", "Transaction", "Montant (DH)", "Ville", "Horodatage UTC", "Statut", "Motif"],
      ...alerts.map((alert) => [environment, alert.alert_id, alert.rule_label, alert.severity, alert.card_holder_id, alert.transaction_id, alert.amount, alert.location, alert.timestamp, STATUS[alert.status].label, alert.detail])];
  }
  const blob = new Blob(["\ufeff" + rows.map((row) => row.map(csvValue).join(";")).join("\r\n")], { type: "text/csv;charset=utf-8" });
  const href = URL.createObjectURL(blob), anchor = document.createElement("a");
  anchor.href = href;
  anchor.download = `vigie-${state.view === "transactions" ? "transactions" : "alertes"}-${state.data.mode}-${new Date().toISOString().slice(0, 10)}.csv`;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(href), 1000);
  toast(`${counted(rows.length - 1, "ligne exportée", "lignes exportées")}.`);
}

function resetAlertFilters() {
  $("alert-search").value = "";
  $("rule-filter").value = "all";
  $("severity-filter").value = "all";
  $("alert-sort").value = "date";
  state.tab = "all"; state.alertPage = 1;
  if (state.data) renderAlerts();
}

function showTransactions(search) {
  $("transaction-search").value = search;
  $("transaction-filter").value = "all";
  state.transactionPage = 1;
  if ($("alert-dialog").open) $("alert-dialog").close();
  if (state.data) renderTransactions();
  location.hash = "transactions";
}

function selectScenario(kind) {
  if (!["normale", "montant", "voyage", "frequence"].includes(kind)) return;
  state.scenario = kind;
  document.querySelectorAll('[name="scenario-kind"]').forEach((item) => { item.checked = item.value === kind; });
  document.querySelectorAll("[data-options]").forEach((section) => {
    section.hidden = section.dataset.options !== kind;
    section.querySelectorAll("input, select").forEach((input) => { input.disabled = section.hidden; });
  });
  $("scenario-error").hidden = true;
  updatePrediction();
}

function updatePrediction() {
  const amount = Number($("demo-amount").value);
  $("amount-prediction").textContent = amount > 10000 ? "Une alerte de montant élevé est attendue." : "Aucune alerte de montant élevé attendue à ce montant.";
  $("amount-prediction").classList.toggle("below-threshold", amount <= 10000);
}

function renderDemoResult() {
  const result = state.lastRun;
  if (!result) return;
  $("demo-result-state").textContent = "Essai terminé";
  const summary = result.alerts ? counted(result.alerts, "alerte détectée", "alertes détectées") : "Aucune règle déclenchée";
  const scenarioLabel = { normale: "Paiement habituel", montant: "Montant à examiner", voyage: "Deux villes, une carte", frequence: "Paiements rapprochés" }[result.scenario];
  const explanation = result.alerts ? "Les motifs de détection sont disponibles ci-dessous." : "Les transactions sont enregistrées dans le journal, sans alerte associée.";
  const events = result.events.map((tx, index) => `<li class="demo-event"><span class="event-number">${index + 1}</span><div><strong>${escapeHTML(tx.location)}</strong><small>${escapeHTML(tx.merchant_type)} · ${secondFormat.format(new Date(tx.timestamp))}</small><code>${escapeHTML(tx.transaction_id)}</code></div><span>${amountCell(tx.amount)}</span></li>`).join("");
  const alerts = result.detected_alerts.map((alert) => {
    const rule = RULES[alert.rule];
    return `<button class="result-alert" data-alert="${alert.alert_id}"><span class="rule-indicator ${rule.indicator}">${icon(rule.icon)}</span><span><strong>${escapeHTML(rule.label)}</strong><small>${escapeHTML(alert.detail)}</small></span>${icon("chevron")}</button>`;
  }).join("");
  $("demo-result").innerHTML = `<div class="result-summary ${result.alerts ? "has-alert" : "no-alert"}"><span class="result-summary-icon">${icon(result.alerts ? "shield" : "check")}</span><div><h3>${summary}</h3><p><strong>${scenarioLabel}</strong> · ${explanation}</p></div></div><div class="result-meta"><span>Client <strong class="mono">${escapeHTML(result.card_holder_id)}</strong></span><time datetime="${escapeHTML(result.executed_at)}">${secondFormat.format(new Date(result.executed_at))}</time></div><div class="result-section-heading"><span>TRANSACTIONS DE L’ESSAI</span><strong>${counted(result.transactions, "paiement")}</strong></div><ol class="demo-events">${events}</ol>${alerts ? '<div class="result-section-heading"><span>ALERTES ASSOCIÉES</span><span>Ouvrir pour examiner</span></div><div class="result-alerts">' + alerts + "</div>" : ""}<div class="result-actions"><button class="button" data-client="${escapeHTML(result.card_holder_id)}">${icon("arrows")}Voir ces transactions</button>${alerts ? `<button class="button" data-client-alerts="${escapeHTML(result.card_holder_id)}">${icon("shield")}Voir ces alertes</button>` : ""}</div>`;
}

async function executeScenario(options = {}) {
  if (state.scenarioBusy) return;
  state.scenarioBusy = true;
  document.querySelectorAll('[name="scenario-kind"], .scenario-fields input, .scenario-fields select, .amount-presets button').forEach((input) => { input.disabled = true; });
  $("run-scenario").disabled = true;
  $("run-scenario").innerHTML = '<span class="loader"></span><span>Analyse en cours…</span>';
  $("scenario-error").hidden = true;
  document.querySelectorAll("[data-scenario]").forEach((button) => { button.disabled = true; });
  try {
    state.lastRun = await api("/api/scenario", { scenario: state.scenario, options });
    renderDemoResult();
    await refresh(true);
    if (innerWidth <= 900) $("demo-result").closest("article").scrollIntoView({
      block: "start", behavior: matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth",
    });
  } catch (error) {
    $("scenario-error").textContent = error.message;
    $("scenario-error").hidden = false;
  } finally {
    state.scenarioBusy = false;
    document.querySelectorAll('[name="scenario-kind"], .amount-presets button').forEach((input) => { input.disabled = false; });
    document.querySelectorAll("[data-options]").forEach((section) => {
      section.querySelectorAll("input, select").forEach((input) => { input.disabled = section.hidden; });
    });
    $("run-scenario").disabled = false;
    $("run-scenario").innerHTML = `${icon("play")}<span>Relancer un essai</span>`;
    document.querySelectorAll("[data-scenario]").forEach((button) => { button.disabled = false; });
  }
}

document.addEventListener("click", (event) => {
  const alertElement = event.target.closest("[data-alert]");
  if (alertElement) openAlert(alertElement.dataset.alert, alertElement.closest("#demo-result") ? "demo" : "view");
  const client = event.target.closest("[data-client]");
  if (client) showTransactions(client.dataset.client);
  const city = event.target.closest("[data-city]");
  if (city) showTransactions(city.dataset.city);
  const clientAlerts = event.target.closest("[data-client-alerts]");
  if (clientAlerts) {
    resetAlertFilters(); $("alert-search").value = clientAlerts.dataset.clientAlerts;
    if (state.data) renderAlerts(); location.hash = "alerts";
  }
  const severity = event.target.closest("[data-severity]");
  if (severity) {
    resetAlertFilters(); $("severity-filter").value = severity.dataset.severity;
    if (state.data) renderAlerts(); location.hash = "alerts";
  }
  const ruleFilter = event.target.closest("[data-rule]");
  if (ruleFilter) {
    resetAlertFilters(); $("rule-filter").value = ruleFilter.dataset.rule;
    if (state.data) renderAlerts(); location.hash = "alerts";
  }
  const amountPreset = event.target.closest("[data-amount]");
  if (amountPreset) { $("demo-amount").value = amountPreset.dataset.amount; updatePrediction(); }
  const tab = event.target.closest("[data-tab]");
  if (tab) {
    state.tab = tab.dataset.tab; state.alertPage = 1;
    if (state.data) renderAlerts();
  }
  const scenario = event.target.closest("[data-scenario]");
  if (scenario) runScenario(scenario);
});

async function runScenario(button) {
  if (state.scenarioBusy) return;
  $("demo-amount").value = "14500";
  $("demo-from").value = "Casablanca";
  $("demo-to").value = "Tanger";
  $("demo-elapsed").value = "40";
  $("demo-count").value = "3";
  selectScenario(button.dataset.scenario);
  location.hash = "demo";
  await executeScenario();
}

$("review-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.detail || $("save-review").disabled) return;
  const savedAlert = state.detail;
  const selectedStatus = $("review-status").value;
  $("save-review").disabled = true;
  try {
    await api(`/api/alerts/${savedAlert.alert_id}/status`, { status: selectedStatus });
    if (state.detail?.alert_id === savedAlert.alert_id) $("alert-dialog").close();
    toast(`Suivi enregistré pour ${savedAlert.card_holder_id}.`);
    await refresh(true);
  } catch (error) { toast(error.message, true); }
  finally { $("save-review").disabled = false; }
});
$("close-dialog").addEventListener("click", () => $("alert-dialog").close());
$("alert-dialog").addEventListener("click", (event) => {
  if (event.target === $("alert-dialog")) {
    const rect = $("alert-dialog").getBoundingClientRect();
    if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) $("alert-dialog").close();
  }
});
async function toggleFlow() {
  if (!state.data) return;
  $("pause-button").disabled = true;
  try {
    const response = await api("/api/control", { action: state.data.paused ? "resume" : "pause" });
    toast(response.paused ? "Le flux automatique est en pause." : "Le flux automatique a repris.");
    await refresh(true);
  } catch (error) { toast(error.message, true); }
  finally { $("pause-button").disabled = false; }
}
$("pause-button").addEventListener("click", toggleFlow);
$("demo-pause-button").addEventListener("click", toggleFlow);
$("period").addEventListener("change", () => {
  state.period = $("period").value;
  try { sessionStorage.setItem("vigie-period", state.period); } catch { /* Préférence facultative. */ }
  state.alertPage = 1; state.transactionPage = 1;
  state.data = null;
  $("loading-state").hidden = false;
  route(); refresh(true);
});
for (const id of ["alert-search", "rule-filter", "severity-filter", "alert-sort"]) $(id).addEventListener(id.includes("search") ? "input" : "change", () => { state.alertPage = 1; if (state.data) renderAlerts(); });
for (const id of ["transaction-search", "transaction-filter"]) $(id).addEventListener(id.includes("search") ? "input" : "change", () => { state.transactionPage = 1; if (state.data) renderTransactions(); });
for (const [prefix, property, renderTable] of [["alert", "alertPage", renderAlerts], ["transaction", "transactionPage", renderTransactions]]) {
  $(prefix + "-prev").addEventListener("click", () => { state[property]--; renderTable(); });
  $(prefix + "-next").addEventListener("click", () => { state[property]++; renderTable(); });
}
$("export-button").addEventListener("click", exportCSV);
$("refresh-button").addEventListener("click", () => refresh(true));
$("retry-button").addEventListener("click", () => refresh(true));
$("clear-alert-filters").addEventListener("click", resetAlertFilters);
for (const id of ["priority-open", "priority-critical"]) $(id).addEventListener("click", () => {
  resetAlertFilters();
  state.tab = "open";
  $("alert-sort").value = "priority";
  if (id === "priority-critical") $("severity-filter").value = "critique";
  renderAlerts();
  location.hash = "alerts";
});
$("clear-transaction-filters").addEventListener("click", () => { $("transaction-search").value = ""; $("transaction-filter").value = "all"; state.transactionPage = 1; if (state.data) renderTransactions(); });
$("detail-client-transactions").addEventListener("click", () => { if (state.detail) showTransactions(state.detail.card_holder_id); });
for (const [id, direction] of [["detail-prev", -1], ["detail-next", 1]]) $(id).addEventListener("click", () => {
  const index = state.detailList.indexOf(state.detail?.alert_id) + direction;
  if (state.detailList[index]) openAlert(state.detailList[index], "retain");
});
document.querySelectorAll('[name="scenario-kind"]').forEach((input) => input.addEventListener("change", () => selectScenario(input.value)));
$("demo-amount").addEventListener("input", updatePrediction);
$("scenario-form").addEventListener("submit", (event) => {
  event.preventDefault();
  let options = {};
  if (state.scenario === "montant") options = { amount: Number($("demo-amount").value) };
  if (state.scenario === "voyage") options = { from_city: $("demo-from").value, to_city: $("demo-to").value, elapsed_seconds: Number($("demo-elapsed").value) };
  if (state.scenario === "frequence") options = { count: Number($("demo-count").value) };
  executeScenario(options);
});
$("demo-speed").addEventListener("change", async () => {
  $("demo-speed").disabled = true;
  try { await api("/api/control", { action: "speed", interval: Number($("demo-speed").value) }); await refresh(true); }
  catch (error) { toast(error.message, true); if (state.data?.interval) $("demo-speed").value = String(state.data.interval); }
  finally { $("demo-speed").disabled = false; }
});
$("health-refresh").addEventListener("click", refreshHealth);
function closeMenu() {
  $("sidebar").classList.remove("open");
  $("sidebar-backdrop").hidden = true;
  $("menu-button").setAttribute("aria-expanded", "false");
}

$("menu-button").addEventListener("click", () => {
  const expanded = $("sidebar").classList.toggle("open");
  $("sidebar-backdrop").hidden = !expanded;
  $("menu-button").setAttribute("aria-expanded", String(expanded));
  if (expanded) $("sidebar").querySelector(".nav-item.active, .nav-item")?.focus();
});
$("sidebar-close").addEventListener("click", () => { closeMenu(); $("menu-button").focus(); });

const POPOVERS = [["notifications-button", "notifications-panel"], ["settings-button", "settings-panel"]];
function closePopovers(except) {
  for (const [button, panel] of POPOVERS) {
    if (panel === except) continue;
    $(panel).hidden = true;
    $(button).setAttribute("aria-expanded", "false");
  }
}
for (const [button, panel] of POPOVERS) {
  $(button).addEventListener("click", (event) => {
    event.stopPropagation();
    const opening = $(panel).hidden;
    closePopovers(panel);
    $(panel).hidden = !opening;
    $(button).setAttribute("aria-expanded", String(opening));
  });
}
document.addEventListener("click", (event) => {
  if (!event.target.closest(".popover-anchor") || event.target.closest(".popover [data-alert], .popover-item, .popover-link")) closePopovers();
});
$("notifications-all").addEventListener("click", () => $("priority-open").click());
$("settings-refresh").addEventListener("click", () => refresh(true));
$("settings-pause").addEventListener("click", toggleFlow);
$("settings-services").addEventListener("click", () => { if (state.view === "pipeline") refreshHealth(); else location.hash = "pipeline"; });
$("global-search").addEventListener("submit", (event) => {
  event.preventDefault();
  const query = $("global-search-input").value.trim();
  resetAlertFilters();
  $("alert-search").value = query;
  if (state.data) renderAlerts();
  location.hash = "alerts";
  $("global-search-input").blur();
});
document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  const openPopover = POPOVERS.find(([, panel]) => !$(panel).hidden);
  if (openPopover) { closePopovers(); $(openPopover[0]).focus(); return; }
  if ($("sidebar").classList.contains("open")) {
    closeMenu();
    $("menu-button").focus();
  }
});
for (const eventName of ["click", "focusin"]) {
  document.addEventListener(eventName, (event) => {
    if ($("sidebar").classList.contains("open") && !event.target.closest("#sidebar, #menu-button")) closeMenu();
  });
}
document.querySelector(".skip-link").addEventListener("click", (event) => {
  event.preventDefault();
  $("main").focus();
});
$("sidebar").addEventListener("click", (event) => {
  if (event.target.closest(".nav-item") && $("sidebar").classList.contains("open")) {
    closeMenu();
    $("main").focus();
  }
});
window.addEventListener("hashchange", route);
new ResizeObserver(() => { if (state.data && state.view === "overview") renderActivity(); }).observe($("activity-chart"));
document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
setInterval(() => { if (!document.hidden) refresh(); }, 5000);
$("period").value = state.period;
selectScenario(state.scenario);
route();
refresh();
