const HAS_DOM = typeof document !== "undefined";
const HAS_WINDOW = typeof window !== "undefined";

function resolveApiBaseUrl() {
  if (!HAS_WINDOW) return "";
  const fromGlobal = String(window.CLOUDV2_API_BASE_URL || "").trim();
  if (fromGlobal) {
    return fromGlobal.replace(/\/+$/, "");
  }
  if (!HAS_DOM) return "";
  const meta = document.querySelector('meta[name="cloudv2-api-base-url"]');
  if (!meta) return "";
  const fromMeta = String(meta.getAttribute("content") || "").trim();
  return fromMeta.replace(/\/+$/, "");
}

const API_BASE_URL = resolveApiBaseUrl();

function resolveApiOrigin() {
  if (!HAS_WINDOW || !API_BASE_URL) return "";
  try {
    const parsed = new URL(API_BASE_URL, window.location.href);
    return String(parsed.origin || "").trim();
  } catch (err) {
    return "";
  }
}

function buildAppUrl(url) {
  const normalized = String(url || "").trim();
  if (!normalized) return normalized;
  if (!normalized.startsWith("/") || /^https?:\/\//i.test(normalized)) return normalized;
  const apiOrigin = resolveApiOrigin();
  if (!apiOrigin) return normalized;
  return `${apiOrigin}${normalized}`;
}

function ensureCanonicalAppOrigin() {
  if (!HAS_WINDOW) return false;
  const apiOrigin = resolveApiOrigin();
  if (!apiOrigin || apiOrigin === window.location.origin) return false;
  const target = `${apiOrigin}${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (target === window.location.href) return false;
  window.location.replace(target);
  return true;
}

function buildApiUrl(url) {
  const normalized = String(url || "").trim();
  if (!normalized) return normalized;
  if (/^https?:\/\//i.test(normalized)) return normalized;
  if (!normalized.startsWith("/")) return normalized;
  const isApiPath =
    normalized.startsWith("/api/")
    || normalized.startsWith("/auth/")
    || normalized.startsWith("/account/")
    || normalized.startsWith("/admin/");
  if (!isApiPath || !API_BASE_URL) return normalized;
  return `${API_BASE_URL}${normalized}`;
}

const ui = HAS_DOM
  ? {
      updatedAt: document.getElementById("updatedAt"),
      countsMeta: document.getElementById("countsMeta"),
      logoutBtn: document.getElementById("logoutBtn"),
      adminCreateAccountLink: document.getElementById("adminCreateAccountLink"),
      adminUsersPanel: document.getElementById("adminUsersPanel"),
      adminUsersRefresh: document.getElementById("adminUsersRefresh"),
      adminUsersTable: document.getElementById("adminUsersTable"),
      adminUsersEmpty: document.getElementById("adminUsersEmpty"),
      searchInput: document.getElementById("searchInput"),
      sortSelect: document.getElementById("sortSelect"),
      clearFilters: document.getElementById("clearFilters"),
      technologyFilter: document.getElementById("technologyFilter"),
      firmwareFilter: document.getElementById("firmwareFilter"),
      statusFilterSelect: document.getElementById("statusFilterSelect"),
      connectivityFilterSelect: document.getElementById("connectivityFilterSelect"),
      statusSummary: document.getElementById("statusSummary"),
      pendingPanel: document.getElementById("pendingPanel"),
      pendingList: document.getElementById("pendingList"),
      expectedPivotAdminDock: document.getElementById("expectedPivotAdminDock"),
      expectedPivotInput: document.getElementById("expectedPivotInput"),
      addExpectedPivotBtn: document.getElementById("addExpectedPivotBtn"),
      toggleExpectedPivotPanelBtn: document.getElementById("toggleExpectedPivotPanelBtn"),
      expectedPivotPanel: document.getElementById("expectedPivotPanel"),
      expectedPivotList: document.getElementById("expectedPivotList"),
      bulkPivotActions: document.getElementById("bulkPivotActions"),
      bulkPivotSelectionLabel: document.getElementById("bulkPivotSelectionLabel"),
      bulkResetPivotsBtn: document.getElementById("bulkResetPivotsBtn"),
      bulkDeletePivotsBtn: document.getElementById("bulkDeletePivotsBtn"),
      cardsGrid: document.getElementById("cardsGrid"),
      cardsPrev: document.getElementById("cardsPrev"),
      cardsNext: document.getElementById("cardsNext"),
      cardsPageInfo: document.getElementById("cardsPageInfo"),
      pivotView: document.getElementById("pivotView"),
      closePivot: document.getElementById("closePivot"),
      pivotTitle: document.getElementById("pivotTitle"),
      pivotStatus: document.getElementById("pivotStatus"),
      pivotQuality: document.getElementById("pivotQuality"),
      deletePivotBtn: document.getElementById("deletePivotBtn"),
      pivotSettingsBtn: document.getElementById("pivotSettingsBtn"),
      pivotAdminTech: document.getElementById("pivotAdminTech"),
      pivotTechControls: document.getElementById("pivotTechControls"),
      pivotConcentratorToggle: document.getElementById("pivotConcentratorToggle"),
      savePivotTechnologyBtn: document.getElementById("savePivotTechnologyBtn"),
      pivotLocationInput: document.getElementById("pivotLocationInput"),
      savePivotLocationBtn: document.getElementById("savePivotLocationBtn"),
      resetPivotModemBtn: document.getElementById("resetPivotModemBtn"),
      pivotLocationHint: document.getElementById("pivotLocationHint"),
      pivotTechnologyHint: document.getElementById("pivotTechnologyHint"),
      pivotMoreInfoBtn: document.getElementById("pivotMoreInfoBtn"),
      pivotMetrics: document.getElementById("pivotMetrics"),
      pivotMetricsToggleBtn: document.getElementById("pivotMetricsToggleBtn"),
      connPreset: document.getElementById("connPreset"),
      connFromWrap: document.getElementById("connFromWrap"),
      connToWrap: document.getElementById("connToWrap"),
      connFrom: document.getElementById("connFrom"),
      connTo: document.getElementById("connTo"),
      connApply: document.getElementById("connApply"),
      connSummary: document.getElementById("connSummary"),
      connTrack: document.getElementById("connTrack"),
      connSegmentCard: document.getElementById("connSegmentCard"),
      connStartLabel: document.getElementById("connStartLabel"),
      connEndLabel: document.getElementById("connEndLabel"),
      probeEnabled: document.getElementById("probeEnabled"),
      probeInterval: document.getElementById("probeInterval"),
      saveProbe: document.getElementById("saveProbe"),
      probeHint: document.getElementById("probeHint"),
      probeStatLastSent: document.getElementById("probeStatLastSent"),
      probeStatLastResponse: document.getElementById("probeStatLastResponse"),
      probeStatTimeoutStreak: document.getElementById("probeStatTimeoutStreak"),
      probeStatResponseRatio: document.getElementById("probeStatResponseRatio"),
      probeStatDelayLast: document.getElementById("probeStatDelayLast"),
      probeStatDelayAvg: document.getElementById("probeStatDelayAvg"),
      probeStatsExtraRows: document.getElementById("probeStatsExtraRows"),
      probeDelayPreset: document.getElementById("probeDelayPreset"),
      probeDelayRange: document.getElementById("probeDelayRange"),
      probeDelayFromWrap: document.getElementById("probeDelayFromWrap"),
      probeDelayToWrap: document.getElementById("probeDelayToWrap"),
      probeDelayFrom: document.getElementById("probeDelayFrom"),
      probeDelayTo: document.getElementById("probeDelayTo"),
      probeDelayApply: document.getElementById("probeDelayApply"),
      probeDelayHint: document.getElementById("probeDelayHint"),
      probeDelayChart: document.getElementById("probeDelayChart"),
      probeDelayStartLabel: document.getElementById("probeDelayStartLabel"),
      probeDelayEndLabel: document.getElementById("probeDelayEndLabel"),
      pivotRssiPanel: document.getElementById("pivotRssiPanel"),
      rssiPreset: document.getElementById("rssiPreset"),
      rssiRange: document.getElementById("rssiRange"),
      rssiFromWrap: document.getElementById("rssiFromWrap"),
      rssiToWrap: document.getElementById("rssiToWrap"),
      rssiFrom: document.getElementById("rssiFrom"),
      rssiTo: document.getElementById("rssiTo"),
      rssiApply: document.getElementById("rssiApply"),
      rssiHint: document.getElementById("rssiHint"),
      rssiChart: document.getElementById("rssiChart"),
      rssiStartLabel: document.getElementById("rssiStartLabel"),
      rssiEndLabel: document.getElementById("rssiEndLabel"),
      timelineList: document.getElementById("timelineList"),
      timelinePrev: document.getElementById("timelinePrev"),
      timelineNext: document.getElementById("timelineNext"),
      timelinePageInfo: document.getElementById("timelinePageInfo"),
      timelineTopicSelect: document.getElementById("timelineTopicSelect"),
      timelineDownloadTxt: document.getElementById("timelineDownloadTxt"),
      cloud2Table: document.getElementById("cloud2Table"),
      toastRegion: document.getElementById("toastRegion"),
      sessionHint: document.getElementById("sessionHint"),
      initialLoadingOverlay: document.getElementById("initialLoadingOverlay"),
      initialLoadingText: document.getElementById("initialLoadingText"),
      settingsModal: document.getElementById("settingsModal"),
      settingsModalClose: document.getElementById("settingsModalClose"),
      requestPivotConfigBtn: document.getElementById("requestPivotConfigBtn"),
      pivotConfigHint: document.getElementById("pivotConfigHint"),
      pivotConfigContactor: document.getElementById("pivotConfigContactor"),
      pivotConfigPressure: document.getElementById("pivotConfigPressure"),
      pivotConfigPressurizationTime: document.getElementById("pivotConfigPressurizationTime"),
      pivotConfigOnTime: document.getElementById("pivotConfigOnTime"),
      pivotConfigOffTime: document.getElementById("pivotConfigOffTime"),
      pivotConfigReadTime: document.getElementById("pivotConfigReadTime"),
    }
  : {};

const state = {
  rawState: null,
  pivots: [],
  statusFilter: "all",
  connectivityFilter: "all",
  search: "",
  sort: "critical",
  technologyFilter: "all",
  firmwareFilter: "all",
  technologyOptions: [],
  firmwareOptions: [],
  cardsPage: 1,
  cardsPageSize: 18,
  selectedPivot: null,
  pivotData: null,
  pivotMetricsExpanded: false,
  connPreset: "30d",
  connCustomFrom: "",
  connCustomTo: "",
  connSelectedSegmentKey: null,
  probeDelayPreset: "30d",
  probeDelayCustomFrom: "",
  probeDelayCustomTo: "",
  rssiPreset: "30d",
  rssiCustomFrom: "",
  rssiCustomTo: "",
  timelinePage: 1,
  timelinePageSize: 25,
  timelineTopicFilter: "global",
  timelinePageByTopic: {},
  refreshMs: 5000,
  devReloadToken: null,
  toastSeq: 0,
  lastRefreshToastAtMs: 0,
  qualityOverridesByPivotId: {},
  statusOverridesByPivotId: {},
  qualitySourceSignatureByPivotId: {},
  connectivitySummaryByPivotId: {},
  connectivityMiniSegmentsByPivotId: {},
  qualityOverridesReady: false,
  lastStableSummaryCardCounts: null,
  qualityOverridesLastRefreshMs: 0,
  qualityOverrideRefreshIntervalMs: 45000,
  qualityOverrideMaxConcurrency: 3,
  visibleConnectivityRefreshTimer: null,
  visiblePivotIds: [],
  selectedPivotIds: [],
  expectedPivotPanelExpanded: false,
  expectedPivotPanelSignature: "",
  expectedPivotActionInFlight: false,
  qualityRefreshSeq: 0,
  selectedRunId: null,
  lastRunResolveAttemptMs: 0,
  runAutoDetected: false,
  panelSessionMeta: null,
  panelRunMeta: null,
  refreshInFlight: false,
  auxRefreshPromise: null,
  authUserRole: "user",
  authUserEmail: "",
  pivotDeleteAllowed: false,
  adminUsers: [],
  summaryCardsHistoryPayload: null,
  summaryCardsHistoryLoading: false,
  summaryCardsHistoryLoadedAtMs: 0,
  summaryCardsHistoryError: "",
  summaryCardsHistoryRefreshIntervalMs: 10 * 60 * 1000,
  summaryCardsHistoryFetchPromise: null,
  headerRenderSignature: "",
  statusSummaryRenderSignature: "",
  pendingRenderSignature: "",
  cardsPagerSignature: "",
  cardsRenderPageKey: "",
  cardsRenderSignaturesByPivotId: {},
  cardsRenderIsEmpty: false,
  pivotTableColumnOrder: [],
  pivotTableDragKey: "",
  pivotTablePreferenceLoaded: false,
  pendingModemResetAcks: {},
  bulkPivotActionInFlight: false,
  pivotConfigRequestInFlight: false,
};

const API_REQUEST_TIMEOUT_MS = 20000;
const API_GET_RETRY_DELAYS_MS = [0, 1200];
const API_STALE_RESPONSE_TTL_MS = 180000;
const DASHBOARD_CACHE_STORAGE_PREFIX = "cloud-monitoring:get:";
const DASHBOARD_CACHE_STORAGE_MAX_CHARS = 750000;
const MODEM_RESET_ACK_MIN_FIRMWARE = [2, 8, 4];
const DASHBOARD_TIMEZONE = "America/Sao_Paulo";
const DASHBOARD_DATETIME_UTC_REGEX = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
const CONNECTIVITY_EVENT_TOPIC_FILTERS = [
  { key: "global", label: "Global" },
  { key: "cloudv2", label: "cloudv2" },
  { key: "cloudv2-info", label: "cloudv2-info" },
  { key: "cloudv2-shutdown", label: "cloudv2-shutdown" },
  { key: "cloudv2-error", label: "cloudv2-error" },
  { key: "cloudv2-scheduling", label: "cloudv2-scheduling" },
  { key: "pivot", label: "Pivot_id" },
];
const CONNECTIVITY_EVENT_TOPIC_FILTER_KEYS = new Set(
  CONNECTIVITY_EVENT_TOPIC_FILTERS.map((item) => item.key)
);
const GET_JSON_RESPONSE_CACHE = new Map();
const GET_JSON_INFLIGHT_REQUESTS = new Map();
const DASHBOARD_DATETIME_FORMATTER = typeof Intl !== "undefined"
  ? new Intl.DateTimeFormat("sv-SE", {
      timeZone: DASHBOARD_TIMEZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    })
  : null;
const DASHBOARD_DISPLAY_DATETIME_FORMATTER = typeof Intl !== "undefined"
  ? new Intl.DateTimeFormat("pt-BR", {
      timeZone: DASHBOARD_TIMEZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    })
  : null;

const STATUS_META = {
  all: { label: "Todos", css: "gray", rank: 99 },
  green: { label: "Conectado", css: "green", rank: 2 },
  yellow: { label: "Instável", css: "yellow", rank: 99 },
  critical: { label: "Crítico", css: "critical", rank: 99 },
  red: { label: "Desconectado", css: "red", rank: 0 },
  gray: { label: "Inicial", css: "gray", rank: 1 },
};

const QUALITY_META = {
  critical: { label: "Crítico", rank: 0 },
  yellow: { label: "Instável", rank: 1 },
  calculating: { label: "Em análise", rank: 2 },
  green: { label: "Estável", rank: 3 },
};

const EVENT_LABEL = {
  pivot_discovered: "Início de monitoramento",
  cloudv2: "Atualização de conectividade",
  ping: "Atualização de conectividade",
  cloud2: "Atualização de rede",
  probe_sent: "Solicitação de latência enviada",
  probe_response: "Resposta de latência recebida",
  probe_timeout: "Falha de resposta de latência",
  probe_response_unmatched: "Resposta fora da janela",
  session_started: "Nova sessão",
};

const PIVOT_TABLE_COLUMNS = [
  { key: "pivot_id", label: "Pivo" },
  { key: "status", label: "Status" },
  { key: "connectivity", label: "Conectividade" },
  { key: "timeline", label: "Timeline" },
  { key: "last_cloudv2_at", label: "Ultima atualizacao de dados" },
  { key: "median", label: "Intervalo tipico de atualizacao" },
  { key: "last_activity_at", label: "Ultima atualizacao" },
  { key: "signal", label: "Sinal" },
  { key: "technology", label: "Tecnologia" },
  { key: "firmware", label: "Firmware" },
];
const PIVOT_TABLE_HEADER_STACKS = {
  last_activity_at: ["Ultima", "Atualizacao"],
  last_cloudv2_at: ["Ultima", "Atualizacao", "de Dados"],
  median: ["Intervalo tipico", "de atualizacao"],
};
const PIVOT_TABLE_COLUMN_KEYS = PIVOT_TABLE_COLUMNS.map((item) => item.key);

const INTERNAL_TERMS = [
  "cloudv2",
  "cloud2",
  "probe",
  "#11$",
  "payload",
  "topico",
  "tópico",
  "topic",
  "ping",
  "network",
  "mqtt",
];

const FIXED_PIVOT_DELETE_EMAIL = "eduardocostar03@gmail.com";

function parseHashPivot() {
  const raw = String(location.hash || "");
  const match = raw.match(/pivot=([^&]+)/i);
  if (!match) return null;
  try {
    return decodeURIComponent(match[1]);
  } catch (err) {
    return null;
  }
}

function setHashPivot(pivotId) {
  const id = String(pivotId || "").trim();
  if (!id) {
    history.replaceState(null, "", location.pathname + location.search);
    return;
  }
  const nextHash = `#pivot=${encodeURIComponent(id)}`;
  if (location.hash !== nextHash) {
    history.replaceState(null, "", `${location.pathname}${location.search}${nextHash}`);
  }
}

function text(value, fallback = "-") {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value);
}

function normalizeFilterText(value) {
  return String(value || "").trim();
}

function normalizeFilterKey(value) {
  const normalized = normalizeFilterText(value);
  return normalized ? normalized.toLowerCase() : "";
}

function toBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "sim"].includes(normalized)) return true;
    if (["0", "false", "no", "nao", "não"].includes(normalized)) return false;
  }
  return fallback;
}

function isPivotConcentrator(item) {
  if (!item || typeof item !== "object") return false;
  const summary = item.summary && typeof item.summary === "object" ? item.summary : null;
  if (summary && Object.prototype.hasOwnProperty.call(summary, "is_concentrator")) {
    return toBoolean(summary.is_concentrator, false);
  }
  if (Object.prototype.hasOwnProperty.call(item, "is_concentrator")) {
    return toBoolean(item.is_concentrator, false);
  }
  return false;
}

function buildFilterOptionItems(values) {
  const byKey = new Map();
  for (const rawValue of values || []) {
    const label = normalizeFilterText(rawValue);
    if (!label) continue;
    const key = normalizeFilterKey(label);
    if (!key) continue;
    if (!byKey.has(key)) byKey.set(key, label);
  }

  return [...byKey.entries()]
    .sort((a, b) => a[1].localeCompare(b[1], "pt-BR", { sensitivity: "base" }))
    .map(([key, label]) => ({ key, label }));
}

function pivotTechnologyValue(item) {
  if (isPivotConcentrator(item)) return "concentrador";
  const cloud2 = (item || {}).last_cloud2 || {};
  return normalizeFilterText(cloud2.technology);
}

function pivotFirmwareValue(item) {
  const cloud2 = (item || {}).last_cloud2 || {};
  return normalizeFilterText(cloud2.firmware);
}

function parseSignalTechnologyCombined(value) {
  const raw = normalizeFilterText(value);
  if (!raw) return { signal: "", technology: "" };
  const sepIndex = raw.indexOf("/");
  if (sepIndex < 0) return { signal: raw, technology: "" };
  const signal = normalizeFilterText(raw.slice(0, sepIndex));
  const technology = normalizeFilterText(raw.slice(sepIndex + 1));
  return {
    signal: signal === "-" ? "" : signal,
    technology: technology === "-" ? "" : technology,
  };
}

function pivotSignalValue(item) {
  const safePivot = item || {};
  const directSignal = normalizeFilterText(safePivot.signal);
  if (directSignal) return directSignal;
  const cloud2 = safePivot.last_cloud2 || {};
  const cloudSignal = normalizeFilterText(cloud2.rssi);
  if (cloudSignal) return cloudSignal;
  const parsed = parseSignalTechnologyCombined(safePivot.signal_technology);
  return parsed.signal;
}

function pivotTechnologyColumnValue(item) {
  const safePivot = item || {};
  const directTechnology = normalizeFilterText(safePivot.technology);
  if (directTechnology) return directTechnology;
  const cloud2 = safePivot.last_cloud2 || {};
  const cloudTechnology = normalizeFilterText(cloud2.technology);
  if (cloudTechnology) return cloudTechnology;
  const parsed = parseSignalTechnologyCombined(safePivot.signal_technology);
  if (parsed.technology) return parsed.technology;
  return pivotTechnologyValue(safePivot);
}

function normalizeTimelineMiniSegments(rawSegments) {
  if (!Array.isArray(rawSegments)) return [];
  const normalized = [];
  let consumedRatio = 0;
  for (const segment of rawSegments) {
    if (!segment || typeof segment !== "object") continue;
    const state = normalizeFilterKey(segment.state);
    if (state !== "online" && state !== "offline") continue;
    const ratio = Number(segment.ratio);
    if (!Number.isFinite(ratio) || ratio <= 0) continue;
    const clamped = Math.max(0, Math.min(1, ratio));
    consumedRatio += clamped;
    normalized.push({ state, ratio: clamped });
    if (normalized.length >= 60) break;
  }
  if (!normalized.length) return [];
  if (consumedRatio <= 0) return [];
  return normalized.map((segment) => ({
    state: segment.state,
    ratio: segment.ratio / consumedRatio,
  }));
}

function buildTimelineMiniSegmentsFromConnectivity(segments, durationSec) {
  const safeDurationSec = Math.max(1, Number(durationSec || 0));
  const raw = [];
  for (const segment of segments || []) {
    if (!segment || typeof segment !== "object") continue;
    const state = segment.state === "connected" ? "online" : "offline";
    const duration = Math.max(0, Number(segment.duration || 0));
    if (!Number.isFinite(duration) || duration <= 0) continue;
    const ratio = duration / safeDurationSec;
    if (!Number.isFinite(ratio) || ratio <= 0) continue;
    raw.push({ state, ratio });
  }
  if (!raw.length) return [];

  const merged = [];
  for (const segment of raw) {
    const last = merged[merged.length - 1];
    if (last && last.state === segment.state) {
      last.ratio += segment.ratio;
    } else {
      merged.push({ state: segment.state, ratio: segment.ratio });
    }
  }
  return normalizeTimelineMiniSegments(merged);
}

function resolvePivotTimelineMiniSegments(item) {
  const safePivot = item || {};
  const pivotId = text(safePivot.pivot_id, "").trim();
  if (pivotId) {
    const computed = state.connectivityMiniSegmentsByPivotId[pivotId];
    if (Array.isArray(computed) && computed.length) {
      return normalizeTimelineMiniSegments(computed);
    }
  }

  const summary =
    safePivot.summary && typeof safePivot.summary === "object"
      ? safePivot.summary
      : null;
  const fallbackCandidates = [
    safePivot.timeline_mini,
    summary ? summary.timeline_mini : null,
  ];
  for (const candidate of fallbackCandidates) {
    if (Array.isArray(candidate) && candidate.length) {
      return normalizeTimelineMiniSegments(candidate);
    }
  }
  return [];
}

function buildTimelineMiniHtml(item) {
  const segments = resolvePivotTimelineMiniSegments(item);
  if (!segments.length) {
    return `
      <div class="pivot-timeline-mini empty" aria-hidden="true">
        <span class="pivot-timeline-mini-segment neutral" style="width: 100%;"></span>
      </div>
    `;
  }

  const segmentsHtml = segments
    .map((segment) => {
      const widthPct = Math.max(0, Math.min(100, segment.ratio * 100));
      const cssState = segment.state === "online" ? "online" : "offline";
      return `<span class="pivot-timeline-mini-segment ${cssState}" style="width: ${widthPct.toFixed(4)}%;"></span>`;
    })
    .join("");
  return `<div class="pivot-timeline-mini" aria-hidden="true">${segmentsHtml}</div>`;
}

function fallbackCloud2FilterOptionsFromPivots(pivots) {
  const technologies = [];
  const firmwares = [];
  for (const pivot of pivots || []) {
    const technology = pivotTechnologyValue(pivot);
    const firmware = pivotFirmwareValue(pivot);
    if (technology) technologies.push(technology);
    if (firmware) firmwares.push(firmware);
  }
  return { technologies, firmwares };
}

function renderDynamicSelectOptions(selectEl, defaultLabel, options, selectedKey) {
  if (!selectEl) return "all";

  const currentKey = normalizeFilterKey(selectedKey);
  const normalizedCurrent = currentKey || "all";
  const available = new Set(options.map((item) => item.key));
  const safeSelected = normalizedCurrent !== "all" && available.has(normalizedCurrent) ? normalizedCurrent : "all";

  const html = [`<option value="all">${escapeHtml(defaultLabel)}</option>`];
  for (const option of options) {
    const selected = option.key === safeSelected ? " selected" : "";
    html.push(`<option value="${escapeHtml(option.key)}"${selected}>${escapeHtml(option.label)}</option>`);
  }
  selectEl.innerHTML = html.join("");
  selectEl.value = safeSelected;
  return safeSelected;
}

function syncCloud2FilterOptions(rawState) {
  const raw = rawState || {};
  const fromState = raw.cloud2_filter_options && typeof raw.cloud2_filter_options === "object"
    ? raw.cloud2_filter_options
    : {};
  const fallback = fallbackCloud2FilterOptionsFromPivots(state.pivots);

  const technologiesSource = Array.isArray(fromState.technologies) && fromState.technologies.length
    ? fromState.technologies
    : fallback.technologies;
  const firmwaresSource = Array.isArray(fromState.firmwares) && fromState.firmwares.length
    ? fromState.firmwares
    : fallback.firmwares;

  const technologyOptions = buildFilterOptionItems(technologiesSource);
  const firmwareOptions = buildFilterOptionItems(firmwaresSource);
  state.technologyOptions = technologyOptions;
  state.firmwareOptions = firmwareOptions;

  state.technologyFilter = renderDynamicSelectOptions(
    ui.technologyFilter,
    "Todas tecnologias",
    technologyOptions,
    state.technologyFilter
  );
  state.firmwareFilter = renderDynamicSelectOptions(
    ui.firmwareFilter,
    "Todos firmwares",
    firmwareOptions,
    state.firmwareFilter
  );
}

function fmtDuration(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)} min`;
  return `${(s / 3600).toFixed(1)}h`;
}

function fmtSecondsPrecise(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s >= 10) return `${s.toFixed(1)}s`;
  if (s >= 1) return `${s.toFixed(2)}s`;
  return `${s.toFixed(3)}s`;
}

function fmtPercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "-";
  return `${n.toFixed(1)}%`;
}

function fmtPercentWhole(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "-";
  return `${Math.round(n)}%`;
}

function agoFromTs(tsSec) {
  const ts = Number(tsSec);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  const delta = Math.max(0, Math.floor(Date.now() / 1000 - ts));
  if (delta < 60) return `${delta}s atrás`;
  if (delta < 3600) return `${Math.floor(delta / 60)} min atrás`;
  if (delta < 86400) return `${(delta / 3600).toFixed(1)}h atrás`;
  return `${(delta / 86400).toFixed(1)}d atrás`;
}

function toDateTimeLocal(tsSec) {
  const ts = Number(tsSec);
  if (!Number.isFinite(ts) || ts <= 0) return "";
  const date = new Date(ts * 1000);
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(
    date.getMinutes()
  )}`;
}

function parseDateTimeLocal(value) {
  const textValue = String(value || "").trim();
  if (!textValue) return null;
  const parsed = new Date(textValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return Math.floor(parsed.getTime() / 1000);
}

function formatShortDateTime(tsSec) {
  const ts = Number(tsSec);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  const date = new Date(ts * 1000);
  if (DASHBOARD_DISPLAY_DATETIME_FORMATTER) {
    return DASHBOARD_DISPLAY_DATETIME_FORMATTER.format(date);
  }
  return date.toLocaleString("pt-BR");
}

function formatCompactDateTime(tsSec) {
  const ts = Number(tsSec);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  const date = new Date(ts * 1000);
  if (DASHBOARD_DATETIME_FORMATTER) {
    return DASHBOARD_DATETIME_FORMATTER.format(date).replace(",", "");
  }
  return formatShortDateTime(tsSec);
}

function formatTimestampFromTsOrValue(tsSec, value, compact = false) {
  const ts = Number(tsSec);
  if (Number.isFinite(ts) && ts > 0) {
    return compact ? formatCompactDateTime(ts) : formatShortDateTime(ts);
  }
  return text(value, "-");
}

function formatConnectivityEventTxtTimestamp(event) {
  const formatted = formatTimestampFromTsOrValue((event || {}).ts, (event || {}).at, true);
  const match = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}:\d{2}:\d{2})$/.exec(formatted);
  if (match) return `${match[3]}-${match[2]}-${match[1]} ${match[4]}`;
  return formatted;
}

function sanitizeDownloadFilenamePart(value) {
  return text(value, "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    || "eventos";
}

function formatDateTimeValue(value) {
  if (value === null || value === undefined || value === "") return "-";

  const asNumber = Number(value);
  let date = null;
  if (Number.isFinite(asNumber) && asNumber > 0) {
    date = asNumber > 1000000000000 ? new Date(asNumber) : new Date(asNumber * 1000);
  } else {
    const parsed = new Date(String(value));
    if (!Number.isNaN(parsed.getTime())) {
      date = parsed;
    }
  }

  if (!date || Number.isNaN(date.getTime())) {
    return text(value, "-");
  }

  if (DASHBOARD_DISPLAY_DATETIME_FORMATTER) {
    return DASHBOARD_DISPLAY_DATETIME_FORMATTER.format(date);
  }
  return date.toLocaleString("pt-BR");
}

function formatUtcToDashboardTimezone(value) {
  if (value === null || value === undefined || value === "") return "-";

  const raw = String(value).trim();
  if (!raw) return "-";

  const asNumber = Number(raw);
  let date = null;
  if (Number.isFinite(asNumber) && asNumber > 0) {
    date = asNumber > 1000000000000 ? new Date(asNumber) : new Date(asNumber * 1000);
  } else if (DASHBOARD_DATETIME_UTC_REGEX.test(raw)) {
    date = new Date(raw.replace(" ", "T") + "Z");
  } else {
    date = new Date(raw);
  }

  if (!date || Number.isNaN(date.getTime())) {
    return text(value, "-");
  }

  if (DASHBOARD_DATETIME_FORMATTER) {
    return DASHBOARD_DATETIME_FORMATTER.format(date).replace(",", "");
  }
  return formatDateTimeValue(value);
}

function formatProbeStatusTimestamp(value) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  const formatted = formatDateTimeValue(ts);
  if (formatted === "-") return `${Math.trunc(ts)}`;
  return `${formatted} (${Math.trunc(ts)})`;
}

function formatProbeStatusInteger(value, suffix = "") {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return `${Math.trunc(parsed)}${suffix}`;
}

function formatProbeStatusTemperature(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return `${parsed.toFixed(1)}C`;
}

function formatProbeConfiguredNetworks(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";

  switch (Math.trunc(parsed)) {
    case 1:
      return "LTE, WCDMA";
    case 2:
      return "WCDMA, LTE, GSM";
    case 3:
      return "LTE";
    case 4:
      return "WCDMA";
    case 5:
      return "GSM";
    case 6:
      return "GSM, LTE, WCDMA";
    case 7:
      return "Automático";
    case 8:
      return "LTE, Automático";
    default:
      return "LTE, WCDMA, GSM";
  }
}

function normalizeConnectedPivotsHistoryPoints(points) {
  if (!Array.isArray(points)) return [];
  return points
    .filter((item) => item && typeof item === "object")
    .map((item) => {
      const ts = Number(item.ts);
      const connectedCount = Math.max(0, Number(item.connected_count || 0));
      const totalCount = Math.max(connectedCount, Number(item.total_count || 0));
      return {
        ts,
        connected_count: Number.isFinite(connectedCount) ? connectedCount : 0,
        total_count: Number.isFinite(totalCount) ? totalCount : 0,
        at: text(item.at, "-"),
      };
    })
    .filter((item) => Number.isFinite(item.ts) && item.ts > 0)
    .sort((a, b) => a.ts - b.ts);
}

function sampleConnectedPivotsHistoryPoints(points, maxPoints = 180) {
  const safePoints = normalizeConnectedPivotsHistoryPoints(points);
  if (safePoints.length <= maxPoints) return safePoints;
  const bucketSize = Math.max(1, Math.ceil(safePoints.length / maxPoints));
  const sampled = [];
  for (let index = 0; index < safePoints.length; index += bucketSize) {
    const bucket = safePoints.slice(index, index + bucketSize);
    if (!bucket.length) continue;
    sampled.push(bucket[bucket.length - 1]);
  }
  const lastPoint = safePoints[safePoints.length - 1];
  if (!sampled.length || sampled[sampled.length - 1].ts !== lastPoint.ts) {
    sampled.push(lastPoint);
  }
  return sampled;
}

function formatConnectedHistoryAxisLabel(ts) {
  const parsedTs = Number(ts);
  if (!Number.isFinite(parsedTs) || parsedTs <= 0) return "-";
  const date = new Date(parsedTs * 1000);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
  }).replace(",", "");
}

function buildConnectedPivotsHistorySvgModel(points, width = 420, height = 170) {
  const sampled = sampleConnectedPivotsHistoryPoints(points);
  if (!sampled.length) return null;

  const safeWidth = Math.max(240, Number(width || 420));
  const safeHeight = Math.max(120, Number(height || 170));
  const padding = { top: 10, right: 10, bottom: 22, left: 10 };
  const plotWidth = safeWidth - padding.left - padding.right;
  const plotHeight = safeHeight - padding.top - padding.bottom;
  const maxValue = Math.max(1, ...sampled.map((item) => Number(item.total_count || item.connected_count || 0) || 0));
  const minValue = 0;
  const valueRange = Math.max(1, maxValue - minValue);

  const chartPoints = sampled.map((item, index) => {
    const x = padding.left + (plotWidth * index) / Math.max(1, sampled.length - 1);
    const normalized = (Number(item.connected_count || 0) - minValue) / valueRange;
    const y = padding.top + plotHeight - (normalized * plotHeight);
    return {
      x: Number(x.toFixed(2)),
      y: Number(y.toFixed(2)),
      connected_count: Number(item.connected_count || 0),
      total_count: Number(item.total_count || 0),
      ts: Number(item.ts || 0),
    };
  });

  const linePath = chartPoints
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  const areaPath = `${linePath} L ${chartPoints[chartPoints.length - 1].x} ${padding.top + plotHeight} L ${chartPoints[0].x} ${padding.top + plotHeight} Z`;
  const gridLines = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
    const y = Number((padding.top + plotHeight - (ratio * plotHeight)).toFixed(2));
    const value = Math.round(maxValue * ratio);
    return { y, value };
  });

  return {
    width: safeWidth,
    height: safeHeight,
    areaPath,
    linePath,
    gridLines,
    firstTs: chartPoints[0].ts,
    lastTs: chartPoints[chartPoints.length - 1].ts,
  };
}

function buildConnectedPivotsHistoryPopoverHtml() {
  const payload = state.connectedPivotsHistoryPayload || {};
  const points = normalizeConnectedPivotsHistoryPoints(payload.points);
  const source = text(payload.source, "live");
  const latest = points.length ? points[points.length - 1] : null;
  const peak = points.length ? points.reduce((max, item) => Math.max(max, Number(item.connected_count || 0)), 0) : 0;
  const average = points.length
    ? (points.reduce((sum, item) => sum + Number(item.connected_count || 0), 0) / points.length)
    : 0;
  const chart = buildConnectedPivotsHistorySvgModel(points);

  let bodyHtml = `<div class="summary-chart-empty">Nenhum histórico horário registrado ainda.</div>`;
  if (state.connectedPivotsHistoryLoading && !points.length) {
    bodyHtml = `<div class="summary-chart-empty">Carregando histórico de pivôs conectados...</div>`;
  } else if (chart) {
    bodyHtml = `
      <div class="summary-chart-metrics">
        <span><strong>${escapeHtml(text(latest ? latest.connected_count : 0, "0"))}</strong> conectados agora</span>
        <span><strong>${escapeHtml(String(peak))}</strong> pico no período</span>
        <span><strong>${escapeHtml(average.toFixed(1))}</strong> média por hora</span>
      </div>
      <svg class="summary-chart-svg" viewBox="0 0 ${chart.width} ${chart.height}" preserveAspectRatio="none" aria-hidden="true">
        ${chart.gridLines
          .map((line) => `<line x1="10" y1="${line.y}" x2="${chart.width - 10}" y2="${line.y}" class="summary-chart-grid-line"></line>`)
          .join("")}
        <path d="${chart.areaPath}" class="summary-chart-area"></path>
        <path d="${chart.linePath}" class="summary-chart-line"></path>
      </svg>
      <div class="summary-chart-axis">
        <span>${escapeHtml(formatConnectedHistoryAxisLabel(chart.firstTs))}</span>
        <span>${escapeHtml(formatConnectedHistoryAxisLabel(chart.lastTs))}</span>
      </div>
    `;
  }

  const noteText = source === "mock"
    ? "Dados simulados locais para visualizar o gráfico."
    : "Amostras horárias mantidas pelos últimos 30 dias.";

  return `
    <div class="summary-chart-popover" role="tooltip">
      <div class="summary-chart-head">
        <strong>Pivôs conectados</strong>
        <span>Últimos 30 dias • 1 ponto por hora</span>
      </div>
      ${bodyHtml}
      <div class="summary-chart-note">${escapeHtml(noteText)}</div>
      ${state.connectedPivotsHistoryError
        ? `<div class="summary-chart-error">${escapeHtml(state.connectedPivotsHistoryError)}</div>`
        : ""}
    </div>
  `;
}

const SUMMARY_CARD_HISTORY_FIELDS = {
  all: "total_count",
  connected: "connected_count",
  disconnected: "disconnected_count",
  initial: "initial_count",
  quality_green: "quality_green_count",
  quality_calculating: "quality_calculating_count",
  quality_yellow: "quality_yellow_count",
  quality_critical: "quality_critical_count",
};

const SUMMARY_CARD_HISTORY_CURRENT_LABELS = {
  all: "pivôs no total",
  connected: "conectados agora",
  disconnected: "desconectados agora",
  initial: "em status inicial",
  quality_green: "estáveis agora",
  quality_calculating: "em análise agora",
  quality_yellow: "instáveis agora",
  quality_critical: "críticos agora",
};

const SUMMARY_CARD_TEXT_ONLY_KEYS = new Set(["initial", "quality_calculating"]);

function normalizeSummaryCardsHistoryPoints(points) {
  if (!Array.isArray(points)) return [];
  return points
    .filter((item) => item && typeof item === "object")
    .map((item) => {
      const ts = Number(item.ts);
      const normalized = {
        ts,
        at: text(item.at, "-"),
      };
      for (const field of Object.values(SUMMARY_CARD_HISTORY_FIELDS)) {
        const parsed = Math.max(0, Number(item[field] || 0));
        normalized[field] = Number.isFinite(parsed) ? parsed : 0;
      }
      const statusTotal = normalized.connected_count + normalized.disconnected_count + normalized.initial_count;
      const qualityTotal = (
        normalized.quality_green_count
        + normalized.quality_calculating_count
        + normalized.quality_yellow_count
        + normalized.quality_critical_count
      );
      normalized.total_count = Math.max(normalized.total_count, statusTotal, qualityTotal);
      return normalized;
    })
    .filter((item) => Number.isFinite(item.ts) && item.ts > 0)
    .sort((a, b) => a.ts - b.ts);
}

function computeSummaryCardCountsFromPivots(pivots = state.pivots) {
  const stateCounts = { all: 0, green: 0, red: 0, gray: 0 };
  const qualityCounts = { green: 0, yellow: 0, critical: 0, calculating: 0 };
  const safePivots = Array.isArray(pivots) ? pivots : [];

  for (const pivot of safePivots) {
    stateCounts.all += 1;
    const stateCode = getDisplayStatus(pivot).code;
    if (stateCounts[stateCode] !== undefined) stateCounts[stateCode] += 1;

    const qualityCode = getDisplayQuality(pivot).code;
    if (qualityCounts[qualityCode] !== undefined) qualityCounts[qualityCode] += 1;
  }

  return {
    stateCounts,
    qualityCounts,
    historyPoint: {
      ts: 0,
      total_count: stateCounts.all,
      connected_count: stateCounts.green,
      disconnected_count: stateCounts.red,
      initial_count: stateCounts.gray,
      quality_green_count: qualityCounts.green,
      quality_calculating_count: qualityCounts.calculating,
      quality_yellow_count: qualityCounts.yellow,
      quality_critical_count: qualityCounts.critical,
    },
  };
}

function hasCompleteQualityOverridesForPivots(pivots = state.pivots, maps = state) {
  const safePivots = Array.isArray(pivots) ? pivots : [];
  if (!safePivots.length) return true;
  const qualityMap = maps.qualityOverridesByPivotId || {};
  const statusMap = maps.statusOverridesByPivotId || {};
  const connectivityMap = maps.connectivitySummaryByPivotId || {};
  const miniMap = maps.connectivityMiniSegmentsByPivotId || {};

  return safePivots.every((pivot) => {
    const pivotId = text((pivot || {}).pivot_id, "").trim();
    if (!pivotId) return true;
    return (
      Object.prototype.hasOwnProperty.call(qualityMap, pivotId)
      && Object.prototype.hasOwnProperty.call(statusMap, pivotId)
      && Object.prototype.hasOwnProperty.call(connectivityMap, pivotId)
      && Object.prototype.hasOwnProperty.call(miniMap, pivotId)
    );
  });
}

function resolveSummaryCardsHistoryReferenceTs(payload = state.summaryCardsHistoryPayload || {}) {
  const candidates = [
    Number((payload || {}).updated_at_ts || 0),
    Number(((state.rawState || {}).updated_at_ts) || 0),
  ];
  for (const candidate of candidates) {
    if (Number.isFinite(candidate) && candidate > 0) return candidate;
  }
  return Math.floor(Date.now() / 1000);
}

function isRecentSummaryHistoryWarmupOutlier(point, currentPoint, referenceTs, bucketSec) {
  if (!point || typeof point !== "object" || !currentPoint || typeof currentPoint !== "object") return false;
  const pointTs = Number(point.ts || 0);
  const safeReferenceTs = Number(referenceTs || 0);
  if (!Number.isFinite(pointTs) || pointTs <= 0 || !Number.isFinite(safeReferenceTs) || safeReferenceTs <= 0) {
    return false;
  }

  const safeBucketSec = Math.max(3600, Number(bucketSec || 3600) || 3600);
  const ageSec = safeReferenceTs - pointTs;
  if (ageSec < 0 || ageSec > safeBucketSec * 3) return false;

  const currentTotal = Math.max(0, Number(currentPoint.total_count || 0) || 0);
  const pointTotal = Math.max(0, Number(point.total_count || 0) || 0);
  if (currentTotal <= 0 || pointTotal <= 0) return false;
  if (Math.abs(currentTotal - pointTotal) > Math.max(5, currentTotal * 0.1)) return false;

  const fields = Object.values(SUMMARY_CARD_HISTORY_FIELDS).filter((field) => field !== "total_count");
  const maxDiff = fields.reduce((largest, field) => {
    const currentValue = Math.max(0, Number(currentPoint[field] || 0) || 0);
    const pointValue = Math.max(0, Number(point[field] || 0) || 0);
    return Math.max(largest, Math.abs(currentValue - pointValue));
  }, 0);

  return maxDiff >= Math.max(8, currentTotal * 0.2);
}

function buildSummaryCardsDisplayHistoryPoints(points, currentCounts, options = {}) {
  const safePoints = normalizeSummaryCardsHistoryPoints(points);
  const counts = currentCounts && typeof currentCounts === "object" ? currentCounts : {};
  const historyPoint = counts.historyPoint && typeof counts.historyPoint === "object"
    ? { ...counts.historyPoint }
    : null;
  if (!historyPoint) return safePoints;

  const referenceTs = resolveSummaryCardsHistoryReferenceTs(options.payload);
  historyPoint.ts = Number.isFinite(referenceTs) && referenceTs > 0 ? referenceTs : historyPoint.ts;

  if (!safePoints.length) {
    return historyPoint.total_count > 0 ? normalizeSummaryCardsHistoryPoints([historyPoint]) : [];
  }

  const bucketSec = Number((options.payload || {}).bucket_sec || 3600) || 3600;
  const mergedPoints = safePoints.map((point) => {
    if (!isRecentSummaryHistoryWarmupOutlier(point, historyPoint, referenceTs, bucketSec)) {
      return { ...point };
    }
    return {
      ...point,
      ...historyPoint,
      ts: Number(point.ts || 0) || historyPoint.ts,
      at: point.at,
    };
  });
  const lastIndex = mergedPoints.length - 1;
  const lastPoint = mergedPoints[lastIndex] || {};
  mergedPoints[lastIndex] = {
    ...lastPoint,
    ...historyPoint,
    ts: Math.max(Number(lastPoint.ts || 0), Number(historyPoint.ts || 0)),
  };
  return normalizeSummaryCardsHistoryPoints(mergedPoints);
}

function sampleSummaryCardsHistoryPoints(points, maxPoints = 180) {
  const safePoints = normalizeSummaryCardsHistoryPoints(points);
  if (safePoints.length <= maxPoints) return safePoints;
  const bucketSize = Math.max(1, Math.ceil(safePoints.length / maxPoints));
  const sampled = [];
  for (let index = 0; index < safePoints.length; index += bucketSize) {
    const bucket = safePoints.slice(index, index + bucketSize);
    if (!bucket.length) continue;
    sampled.push(bucket[bucket.length - 1]);
  }
  const lastPoint = safePoints[safePoints.length - 1];
  if (!sampled.length || sampled[sampled.length - 1].ts !== lastPoint.ts) {
    sampled.push(lastPoint);
  }
  return sampled;
}

function formatSummaryHistoryAxisLabel(ts) {
  const parsedTs = Number(ts);
  if (!Number.isFinite(parsedTs) || parsedTs <= 0) return "-";
  const date = new Date(parsedTs * 1000);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
  }).replace(",", "");
}

function getSummaryCardHistoryField(metricKey) {
  return SUMMARY_CARD_HISTORY_FIELDS[String(metricKey || "").trim()] || "total_count";
}

function getSummaryCardHistoryValue(item, metricKey) {
  if (!item || typeof item !== "object") return 0;
  const field = getSummaryCardHistoryField(metricKey);
  const parsed = Number(item[field] || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function buildSummaryCardsHistorySvgModel(points, metricKey, width = 420, height = 170) {
  const sampled = sampleSummaryCardsHistoryPoints(points);
  if (!sampled.length) return null;

  const field = getSummaryCardHistoryField(metricKey);
  const safeWidth = Math.max(240, Number(width || 420));
  const safeHeight = Math.max(120, Number(height || 170));
  const padding = { top: 10, right: 10, bottom: 22, left: 10 };
  const plotWidth = safeWidth - padding.left - padding.right;
  const plotHeight = safeHeight - padding.top - padding.bottom;
  const maxValue = Math.max(1, ...sampled.map((item) => Number(item.total_count || item[field] || 0) || 0));
  const minValue = 0;
  const valueRange = Math.max(1, maxValue - minValue);

  const chartPoints = sampled.map((item, index) => {
    const value = getSummaryCardHistoryValue(item, metricKey);
    const x = padding.left + (plotWidth * index) / Math.max(1, sampled.length - 1);
    const normalized = (value - minValue) / valueRange;
    const y = padding.top + plotHeight - (normalized * plotHeight);
    return {
      x: Number(x.toFixed(2)),
      y: Number(y.toFixed(2)),
      value,
      total_count: Number(item.total_count || 0),
      ts: Number(item.ts || 0),
    };
  });

  const linePath = chartPoints
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  const areaPath = `${linePath} L ${chartPoints[chartPoints.length - 1].x} ${padding.top + plotHeight} L ${chartPoints[0].x} ${padding.top + plotHeight} Z`;
  const gridLines = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
    const y = Number((padding.top + plotHeight - (ratio * plotHeight)).toFixed(2));
    const value = Math.round(maxValue * ratio);
    return { y, value };
  });

  return {
    width: safeWidth,
    height: safeHeight,
    areaPath,
    linePath,
    gridLines,
    firstTs: chartPoints[0].ts,
    lastTs: chartPoints[chartPoints.length - 1].ts,
  };
}

function buildSummaryCardHistoryPopoverHtml(item) {
  const payload = state.summaryCardsHistoryPayload || {};
  const currentCounts = state.qualityOverridesReady
    ? computeSummaryCardCountsFromPivots(state.pivots)
    : state.lastStableSummaryCardCounts;
  const points = buildSummaryCardsDisplayHistoryPoints(payload.points, currentCounts, { payload });
  const source = text(payload.source, "live");
  const latestPoint = points.length ? points[points.length - 1] : null;
  const latest = latestPoint ? getSummaryCardHistoryValue(latestPoint, item.key) : 0;
  const peak = points.length ? points.reduce((max, point) => Math.max(max, getSummaryCardHistoryValue(point, item.key)), 0) : 0;
  const average = points.length
    ? (points.reduce((sum, point) => sum + getSummaryCardHistoryValue(point, item.key), 0) / points.length)
    : 0;
  const chart = buildSummaryCardsHistorySvgModel(points, item.key);
  const currentLabel = SUMMARY_CARD_HISTORY_CURRENT_LABELS[item.key] || "registros agora";

  let bodyHtml = `<div class="summary-chart-empty">Nenhum histórico horário registrado ainda.</div>`;
  if (state.summaryCardsHistoryLoading && !points.length) {
    bodyHtml = `<div class="summary-chart-empty">Carregando histórico do resumo...</div>`;
  } else if (chart) {
    bodyHtml = `
      <div class="summary-chart-metrics">
        <span><strong>${escapeHtml(text(latest, "0"))}</strong> ${escapeHtml(currentLabel)}</span>
        <span><strong>${escapeHtml(String(peak))}</strong> pico no período</span>
        <span><strong>${escapeHtml(average.toFixed(1))}</strong> média por hora</span>
      </div>
      <svg class="summary-chart-svg" viewBox="0 0 ${chart.width} ${chart.height}" preserveAspectRatio="none" aria-hidden="true">
        ${chart.gridLines
          .map((line) => `<line x1="10" y1="${line.y}" x2="${chart.width - 10}" y2="${line.y}" class="summary-chart-grid-line"></line>`)
          .join("")}
        <path d="${chart.areaPath}" class="summary-chart-area"></path>
        <path d="${chart.linePath}" class="summary-chart-line"></path>
      </svg>
      <div class="summary-chart-axis">
        <span>${escapeHtml(formatSummaryHistoryAxisLabel(chart.firstTs))}</span>
        <span>${escapeHtml(formatSummaryHistoryAxisLabel(chart.lastTs))}</span>
      </div>
    `;
  }

  const noteText = source === "mock"
    ? "Dados simulados locais para visualizar o gráfico."
    : "Amostras horárias mantidas pelos últimos 30 dias.";

  return `
    <div class="summary-chart-popover" role="tooltip">
      <div class="summary-chart-head">
        <strong>${escapeHtml(item.label)}</strong>
        <span>Últimos 30 dias • 1 ponto por hora</span>
      </div>
      ${bodyHtml}
      <div class="summary-chart-note">${escapeHtml(noteText)}</div>
      <div class="summary-chart-copy summary-chart-copy--footer">${escapeHtml(item.tooltip)}</div>
      ${state.summaryCardsHistoryError
        ? `<div class="summary-chart-error">${escapeHtml(state.summaryCardsHistoryError)}</div>`
        : ""}
    </div>
  `;
}

function buildSummaryCardInfoPopoverHtml(item) {
  return `
    <div class="summary-chart-popover summary-chart-popover--info" role="tooltip">
      <div class="summary-chart-head">
        <strong>${escapeHtml(item.label)}</strong>
        <span>Significado e cálculo</span>
      </div>
      <div class="summary-chart-copy summary-chart-copy--only">${escapeHtml(item.tooltip)}</div>
    </div>
  `;
}

function getProbeResponseInfoDisplayRows(probe) {
  const info = probe && typeof probe === "object" ? probe.last_response_info : null;
  if (!info || typeof info !== "object") return [];

  return [
    { label: "RSSI reportado", value: formatProbeStatusInteger(info.rssi) },
    { label: "Tecnologia reportada", value: text(info.technology) },
    { label: "Timestamp da placa", value: formatProbeStatusTimestamp(info.board_timestamp_ts) },
    { label: "Operadora reportada", value: text(info.operator) },
    { label: "Modelo do modem", value: text(info.modem_name) },
    { label: "IMEI reportado", value: text(info.imei) },
    { label: "Keep alive reportado", value: formatProbeStatusInteger(info.keep_alive_sec, "s") },
    { label: "Socket timeout reportado", value: formatProbeStatusInteger(info.socket_timeout_sec, "s") },
    { label: "Tempo APN reportado", value: formatProbeStatusInteger(info.apn_time_ms, "ms") },
    { label: "APN reportada", value: text(info.apn) },
    { label: "Redes configuradas", value: formatProbeConfiguredNetworks(info.networks) },
    { label: "Temperatura ESP reportada", value: formatProbeStatusTemperature(info.esp_temp_c) },
    { label: "Firmware reportado", value: text(info.firmware) },
  ];
}

function renderProbeResponseInfoRows(probe) {
  if (!ui.probeStatsExtraRows) return;
  const rows = getProbeResponseInfoDisplayRows(probe);
  ui.probeStatsExtraRows.innerHTML = rows
    .map(
      (row) => `<tr><th scope="row">${escapeHtml(row.label)}</th><td>${escapeHtml(text(row.value))}</td></tr>`
    )
    .join("");
}

function buildPivotTableHeaderLabelHtml(columnKey, label) {
  const lines = PIVOT_TABLE_HEADER_STACKS[columnKey];
  if (!Array.isArray(lines) || !lines.length) {
    return escapeHtml(label);
  }
  return `<span class="pivot-table-header-stack">${lines
    .map((line) => `<span>${escapeHtml(line)}</span>`)
    .join("")}</span>`;
}

function buildPivotTableDateTimeHtml(value, tsSec = null) {
  const formatted = formatTimestampFromTsOrValue(tsSec, value, true);
  const compact = formatted.replace(/\s*,\s*/g, " ").trim();
  const parts = compact.split(/\s+/);
  const time = parts.length > 1 ? parts[parts.length - 1] : "";

  if (/^\d{2}:\d{2}:\d{2}$/.test(time)) {
    const date = parts.slice(0, -1).join(" ").trim();
    return `<span class="pivot-table-datetime"><span class="pivot-table-date">${escapeHtml(date || "-")}</span><span class="pivot-table-time">${escapeHtml(time)}</span></span>`;
  }

  return `<span class="pivot-table-datetime"><span class="pivot-table-date">${escapeHtml(formatted)}</span></span>`;
}

function extractPivotCoordinates(pivot) {
  const safePivot = pivot || {};
  const summary = safePivot.summary && typeof safePivot.summary === "object" ? safePivot.summary : {};
  const latitudeCandidates = [summary.latitude, safePivot.latitude];
  const longitudeCandidates = [summary.longitude, safePivot.longitude];

  let latitude = null;
  let longitude = null;
  for (const candidate of latitudeCandidates) {
    if (candidate === null || candidate === undefined || String(candidate).trim() === "") continue;
    const parsed = Number(candidate);
    if (Number.isFinite(parsed)) {
      latitude = parsed;
      break;
    }
  }
  for (const candidate of longitudeCandidates) {
    if (candidate === null || candidate === undefined || String(candidate).trim() === "") continue;
    const parsed = Number(candidate);
    if (Number.isFinite(parsed)) {
      longitude = parsed;
      break;
    }
  }
  return { latitude, longitude };
}

function hasPivotCoordinates(pivot) {
  const coordinates = extractPivotCoordinates(pivot);
  if (coordinates.latitude === null || coordinates.longitude === null) return false;
  const latitude = Number(coordinates.latitude);
  const longitude = Number(coordinates.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return false;
  if (latitude === 0 && longitude === 0) return false;
  return true;
}

function formatPivotCoordinates(latitude, longitude) {
  const lat = Number(latitude);
  const lon = Number(longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return "";
  return `${lat}, ${lon}`;
}

function parsePivotCoordinatesInput(value) {
  const textValue = String(value || "").trim();
  if (!textValue) {
    return { ok: false, error: "Informe latitude e longitude no formato: latitude, longitude." };
  }
  const parts = textValue.split(",");
  if (parts.length !== 2) {
    return { ok: false, error: "Formato inválido. Use: latitude, longitude." };
  }
  const latitude = Number(parts[0].trim());
  const longitude = Number(parts[1].trim());
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return { ok: false, error: "Latitude/longitude inválidas." };
  }
  if (latitude < -90 || latitude > 90) {
    return { ok: false, error: "Latitude deve estar entre -90 e 90." };
  }
  if (longitude < -180 || longitude > 180) {
    return { ok: false, error: "Longitude deve estar entre -180 e 180." };
  }
  return { ok: true, latitude, longitude };
}

function parseFirmwareVersionParts(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw || raw === "-" || raw === "null" || raw === "undefined") return null;
  const match = raw.match(/v?\s*(\d+)\.(\d+)\.(\d+)/i);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function compareFirmwareVersionParts(left, right) {
  const a = Array.isArray(left) ? left : [];
  const b = Array.isArray(right) ? right : [];
  for (let index = 0; index < 3; index += 1) {
    const av = Number(a[index] ?? 0);
    const bv = Number(b[index] ?? 0);
    if (av > bv) return 1;
    if (av < bv) return -1;
  }
  return 0;
}

function supportsModemResetAckByFirmware(firmwareValue) {
  const firmwareParts = parseFirmwareVersionParts(firmwareValue);
  if (!firmwareParts) return false;
  return compareFirmwareVersionParts(firmwareParts, MODEM_RESET_ACK_MIN_FIRMWARE) >= 0;
}

function normalizePivotIdList(values) {
  if (!Array.isArray(values)) return [];
  const normalized = [];
  const seen = new Set();
  for (const rawValue of values) {
    const pivotId = String(rawValue || "").trim();
    if (!pivotId || seen.has(pivotId)) continue;
    seen.add(pivotId);
    normalized.push(pivotId);
  }
  return normalized;
}

function parsePivotIdBatchInput(value) {
  const normalizedText = String(value || "").replace(/\r/g, "\n");
  if (!normalizedText.trim()) return [];
  const parts = normalizedText
    .split(/[\s,;]+/)
    .map((item) => String(item || "").trim())
    .filter(Boolean);
  return normalizePivotIdList(parts);
}

function summarizeExpectedPivotResults(results, status) {
  return (Array.isArray(results) ? results : []).filter((item) => String(item?.status || "") === status);
}

function getExpectedPivotsPending() {
  const rawItems = Array.isArray((state.rawState || {}).expected_pivots_pending)
    ? (state.rawState || {}).expected_pivots_pending
    : [];
  return rawItems
    .map((item) => {
      const pivotId = text(item?.pivot_id, "").trim();
      if (!pivotId) return null;
      return {
        pivot_id: pivotId,
        added_at: formatTimestampFromTsOrValue(item?.added_at_ts, item?.added_at),
        added_at_ts: Number(item?.added_at_ts || 0),
        source: text(item?.source, "ui"),
      };
    })
    .filter(Boolean);
}

function setExpectedPivotsPendingState(pendingItems) {
  const normalized = Array.isArray(pendingItems)
    ? pendingItems
      .map((item) => {
        const pivotId = text(item?.pivot_id, "").trim();
        if (!pivotId) return null;
        return {
          pivot_id: pivotId,
          added_at: formatTimestampFromTsOrValue(item?.added_at_ts, item?.added_at),
          added_at_ts: Number(item?.added_at_ts || 0),
          source: text(item?.source, "ui"),
        };
      })
      .filter(Boolean)
    : [];

  if (!state.rawState || typeof state.rawState !== "object") {
    state.rawState = {};
  }
  state.rawState.expected_pivots_pending = normalized;
  if (!state.rawState.counts || typeof state.rawState.counts !== "object") {
    state.rawState.counts = {};
  }
  state.rawState.counts.expected_pivots_pending = normalized.length;
}

function removeExpectedPivotPendingState(pivotId) {
  const normalizedPivotId = String(pivotId || "").trim();
  if (!normalizedPivotId) return;
  const currentItems = getExpectedPivotsPending();
  setExpectedPivotsPendingState(currentItems.filter((item) => item.pivot_id !== normalizedPivotId));
}

function getPivotSummaryObject(pivot) {
  if (!pivot || typeof pivot !== "object") return {};
  const nestedSummary = pivot.summary;
  if (nestedSummary && typeof nestedSummary === "object") {
    return nestedSummary;
  }
  return pivot;
}

function getPivotFirmwareVersionForReset(pivot) {
  const summary = getPivotSummaryObject(pivot);
  const lastCloud2 = (summary.last_cloud2 && typeof summary.last_cloud2 === "object" ? summary.last_cloud2 : {}) || {};
  return String(lastCloud2.firmware || "").trim();
}

function getPivotModemResetSummary(pivot) {
  const summary = getPivotSummaryObject(pivot);
  const modemReset = summary.modem_reset;
  return modemReset && typeof modemReset === "object" ? modemReset : {};
}

function collectConfirmedModemResetAcks(pendingByPivotId, pivots, panelPivot = null) {
  const sourcePending = pendingByPivotId && typeof pendingByPivotId === "object" ? pendingByPivotId : {};
  const pivotSummaryById = {};

  if (Array.isArray(pivots)) {
    for (const pivot of pivots) {
      const pivotId = String((pivot || {}).pivot_id || "").trim();
      if (!pivotId) continue;
      pivotSummaryById[pivotId] = getPivotModemResetSummary(pivot);
    }
  }

  const panelPivotId = String((panelPivot || {}).pivot_id || "").trim();
  if (panelPivotId) {
    pivotSummaryById[panelPivotId] = getPivotModemResetSummary(panelPivot);
  }

  const confirmedPivotIds = [];
  const remainingPendingByPivotId = {};
  for (const [pivotId, pendingEntry] of Object.entries(sourcePending)) {
    const normalizedPivotId = String(pivotId || "").trim();
    if (!normalizedPivotId) continue;
    const commandTs = Number((pendingEntry || {}).commandTs);
    const modemReset = pivotSummaryById[normalizedPivotId] || {};
    const ackTs = Number(modemReset.last_ack_ts);
    if (Number.isFinite(commandTs) && Number.isFinite(ackTs) && ackTs + 0.001 >= commandTs) {
      confirmedPivotIds.push(normalizedPivotId);
      continue;
    }
    if (Number.isFinite(commandTs)) {
      remainingPendingByPivotId[normalizedPivotId] = {
        pivotId: normalizedPivotId,
        commandTs,
      };
    }
  }

  return { confirmedPivotIds, remainingPendingByPivotId };
}

function queuePendingModemResetAck(pivotId, commandTs) {
  const normalizedPivotId = String(pivotId || "").trim();
  const normalizedCommandTs = Number(commandTs);
  if (!normalizedPivotId || !Number.isFinite(normalizedCommandTs)) return;
  state.pendingModemResetAcks = {
    ...state.pendingModemResetAcks,
    [normalizedPivotId]: {
      pivotId: normalizedPivotId,
      commandTs: normalizedCommandTs,
    },
  };
}

function buildPivotListPreview(pivotIds, maxItems = 6) {
  const normalized = normalizePivotIdList(pivotIds);
  if (!normalized.length) return "";
  const preview = normalized.slice(0, maxItems);
  if (normalized.length <= maxItems) {
    return preview.join(", ");
  }
  return `${preview.join(", ")} e mais ${normalized.length - maxItems}`;
}

function checkPendingModemResetAcks() {
  const pendingEntries = state.pendingModemResetAcks;
  if (!pendingEntries || typeof pendingEntries !== "object" || !Object.keys(pendingEntries).length) return;

  const { confirmedPivotIds, remainingPendingByPivotId } = collectConfirmedModemResetAcks(
    pendingEntries,
    state.pivots,
    state.pivotData
  );
  if (!confirmedPivotIds.length) return;

  state.pendingModemResetAcks = remainingPendingByPivotId;
  if (confirmedPivotIds.length === 1 && String(state.selectedPivot || "").trim() === confirmedPivotIds[0]) {
    window.alert("Restart realizado com sucesso");
    return;
  }

  const count = confirmedPivotIds.length;
  const pivotLabel = count === 1 ? "pivô" : "pivôs";
  showToast(`Restart confirmado para ${count} ${pivotLabel}: ${buildPivotListPreview(confirmedPivotIds, 4)}.`, "success", 4800);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function sanitizeUserText(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const lowered = raw.toLowerCase();
  for (const term of INTERNAL_TERMS) {
    if (lowered.includes(term)) return "";
  }
  return raw;
}

function statusReasonByCode(code) {
  if (code === "green") return "Conectividade dentro do esperado.";
  if (code === "red") return "Sem comunicação recente.";
  if (code === "yellow") return "Oscilações de conectividade detectadas.";
  if (code === "critical") return "Conectividade crítica no período.";
  return "Coletando dados para definir o status.";
}

function qualityReasonByCode(code) {
  if (code === "green") return "Conectividade estável no período selecionado.";
  if (code === "yellow") return "Conectividade instável no período selecionado.";
  if (code === "critical") return "Conectividade crítica no período selecionado.";
  return "Coletando dados para avaliar a conectividade.";
}

function buildEventSummary(event) {
  const type = String((event || {}).type || "").toLowerCase();
  if (type === "pivot_discovered") return "Monitoramento iniciado para este pivô.";
  if (type === "cloudv2" || type === "ping" || type === "cloud2") return "Atualização de conectividade registrada.";
  if (type === "probe_sent") return "Solicitação de latência enviada.";
  if (type === "probe_response") return "Resposta de latência recebida no período esperado.";
  if (type === "probe_timeout") return "Sem resposta de latência no tempo esperado.";
  if (type === "probe_response_unmatched") return "Resposta de latência registrada fora da janela esperada.";
  if (type === "session_started") return "Nova sessão de monitoramento iniciada.";
  return sanitizeUserText((event || {}).summary) || "Evento registrado.";
}

function buildEventDetailsText(event) {
  const details = (event || {}).details || {};
  const lines = [];
  const latencySec = Number(details.latency_sec);
  const dropDurationSec = Number(details.drop_duration_sec);
  const timeoutStreak = Number(details.timeout_streak);
  const count = Number(details.count);

  if (Number.isFinite(latencySec) && latencySec >= 0) {
    lines.push(`Latência: ${fmtSecondsPrecise(latencySec)}`);
  }
  if (Number.isFinite(dropDurationSec) && dropDurationSec >= 0) {
    lines.push(`Duração da desconexão: ${fmtDuration(dropDurationSec)}`);
  }
  if (Number.isFinite(timeoutStreak) && timeoutStreak >= 0) {
    lines.push(`Falhas consecutivas: ${timeoutStreak}`);
  }
  if (Number.isFinite(count) && count >= 0) {
    lines.push(`Quantidade: ${count}`);
  }

  return lines.length ? lines.join("\n") : "Sem detalhes adicionais.";
}

function resolveConnectivityEventSourceTopic(event) {
  const details = (event || {}).details;
  const detailsObj = details && typeof details === "object" ? details : {};
  const sourceTopic = text(detailsObj.source_topic, "").trim();
  if (sourceTopic) return sourceTopic;
  const topic = text((event || {}).topic, "").trim();
  if (topic) return topic;
  return "-";
}

function resolveConnectivityEventRawPayload(event) {
  const details = (event || {}).details;
  const detailsObj = details && typeof details === "object" ? details : {};
  for (const key of ["raw_payload", "payload", "payload_text", "last_command_payload", "last_ack_payload"]) {
    const payload = text(detailsObj[key], "").trim();
    if (payload) return payload;
  }
  const parsedPayload = detailsObj.parsed_payload;
  if (parsedPayload && typeof parsedPayload === "object") {
    const rawFromParsed = text(parsedPayload.raw, "").trim();
    if (rawFromParsed) return rawFromParsed;
  }
  return "";
}

function normalizeConnectivityEventTopicFilter(filterKey) {
  const normalized = text(filterKey, "global").trim();
  return CONNECTIVITY_EVENT_TOPIC_FILTER_KEYS.has(normalized) ? normalized : "global";
}

function eventMatchesConnectivityTopicFilter(event, pivot, filterKey) {
  const key = normalizeConnectivityEventTopicFilter(filterKey);
  if (key === "global") return true;

  const pivotId = text((pivot || {}).pivot_id, "").trim();
  const eventTopic = text((event || {}).topic, "").trim();
  const sourceTopic = resolveConnectivityEventSourceTopic(event);

  if (key === "pivot") {
    return !!pivotId && (eventTopic === pivotId || sourceTopic === pivotId);
  }
  return eventTopic === key || sourceTopic === key;
}

function getConnectivityEventsPanelFiltered(pivot, filterKey) {
  const events = Array.isArray((pivot || {}).timeline) ? pivot.timeline : [];
  const maxEvents = state.timelinePageSize * 5;
  const key = normalizeConnectivityEventTopicFilter(filterKey);
  const filteredEvents = key === "global"
    ? events
    : events.filter((event) => eventMatchesConnectivityTopicFilter(event, pivot, key));
  if (filteredEvents.length <= maxEvents) return filteredEvents;
  return filteredEvents.slice(0, maxEvents);
}

function buildConnectivityEventTitle(event) {
  const sourceTopic = resolveConnectivityEventSourceTopic(event);
  if (sourceTopic && sourceTopic !== "-") return `Mensagem Recebida de ${sourceTopic}`;
  return "Mensagem recebida";
}

function buildConnectivityEventDetailsText(event) {
  const sourceTopic = resolveConnectivityEventSourceTopic(event);
  const rawPayload = resolveConnectivityEventRawPayload(event);
  const detailsText = buildEventDetailsText(event);
  const lines = [];
  if (sourceTopic && sourceTopic !== "-") {
    lines.push(`Origem: ${sourceTopic}`);
  }
  if (rawPayload) {
    lines.push(`Payload:\n${rawPayload}`);
  }
  if (detailsText && detailsText !== "Sem detalhes adicionais.") {
    lines.push(detailsText);
  }
  return lines.length ? lines.join("\n\n") : "Sem detalhes adicionais.";
}

function getDisplayQuality(item) {
  const baseQuality = resolveQualityBase(item);
  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.qualityOverridesByPivotId[pivotId] : null;
  const base = override || baseQuality;
  const displayStatus = getDisplayStatus(item);
  const statusCode = text(displayStatus.code, "gray");
  const statusReason = sanitizeUserText(displayStatus.reason);
  if (statusCode === "gray") {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: statusReason || qualityReasonByCode("calculating"),
      rank: QUALITY_META.calculating.rank,
    };
  }
  const code = text(base.code, "green");
  const meta = QUALITY_META[code] || QUALITY_META.green;
  const rankRaw = Number(base.rank);
  const baseReason = sanitizeUserText(base.reason);

  return {
    code,
    label: meta.label,
    reason: baseReason || qualityReasonByCode(code),
    rank: Number.isFinite(rankRaw) ? rankRaw : meta.rank,
  };
}

function resolveQualityBase(item) {
  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.qualityOverridesByPivotId[pivotId] : null;
  return override || ((item || {}).quality || {});
}

function getDisplayStatus(item) {
  const qualityBase = resolveQualityBase(item);
  const qualityCode = text((qualityBase || {}).code, "").trim().toLowerCase();
  if (qualityCode === "calculating") {
    return {
      code: "gray",
      label: STATUS_META.gray.label,
      reason: "Conectividade em análise.",
      rank: Number(STATUS_META.gray.rank ?? 99),
    };
  }

  const pivotId = text((item || {}).pivot_id, "").trim();
  const override = pivotId ? state.statusOverridesByPivotId[pivotId] : null;
  const base = override || ((item || {}).status || {});
  const code = text(base.code, "gray");
  const meta = STATUS_META[code] || STATUS_META.gray;
  const rankRaw = Number(base.rank);
  const baseReason = sanitizeUserText(base.reason);

  return {
    code,
    label: meta.label,
    reason: baseReason || statusReasonByCode(code),
    rank: Number.isFinite(rankRaw) ? rankRaw : Number(meta.rank ?? 99),
  };
}

function showToast(message, level = "success", ttlMs = 3200) {
  if (!ui.toastRegion) return;
  const textMessage = String(message || "").trim();
  if (!textMessage) return;

  const toast = document.createElement("div");
  const normalizedLevel = ["success", "error", "warn"].includes(level) ? level : "success";
  toast.className = `toast ${normalizedLevel}`;
  toast.setAttribute("role", normalizedLevel === "error" ? "alert" : "status");
  toast.setAttribute("aria-live", normalizedLevel === "error" ? "assertive" : "polite");
  toast.innerHTML = `
    <span class="toast-dot" aria-hidden="true"></span>
    <div>${escapeHtml(textMessage)}</div>
  `;
  toast.dataset.toastId = `toast-${Date.now()}-${state.toastSeq++}`;
  ui.toastRegion.appendChild(toast);

  const leaveDelay = Math.max(850, Number(ttlMs) - 260);
  window.setTimeout(() => {
    toast.classList.add("leave");
  }, leaveDelay);
  window.setTimeout(() => {
    toast.remove();
  }, Math.max(900, Number(ttlMs)));
}

function setInitialLoading(loading, message = "") {
  if (!HAS_DOM) return;
  if (ui.initialLoadingText) {
    const safeMessage = String(message || "").trim();
    if (safeMessage) {
      ui.initialLoadingText.textContent = safeMessage;
    }
  }
  if (ui.initialLoadingOverlay) {
    ui.initialLoadingOverlay.hidden = !loading;
  }
  if (document.body) {
    document.body.classList.toggle("initial-loading", !!loading);
  }
}

function cloneJsonValue(value) {
  if (value === null || value === undefined) return value;
  return JSON.parse(JSON.stringify(value));
}

function canUseSessionStorage() {
  try {
    return typeof sessionStorage !== "undefined";
  } catch (err) {
    return false;
  }
}

function isCacheableDashboardGet(url) {
  const targetUrl = buildApiUrl(url);
  return (
    targetUrl.startsWith("/api/state")
    || targetUrl.startsWith("/api/quality-lite")
    || targetUrl.startsWith("/api/summary/cards-history")
    || targetUrl.startsWith("/api/summary/connected-history")
    || targetUrl.startsWith("/api/pivot/")
    || targetUrl.startsWith("/api/monitoring/runs")
    || targetUrl === "data/ui_config.json"
  );
}

function isPersistentDashboardGet(url) {
  const targetUrl = buildApiUrl(url);
  return (
    targetUrl.startsWith("/api/state")
    || targetUrl.startsWith("/api/quality-lite")
    || targetUrl.startsWith("/api/summary/cards-history")
    || targetUrl.startsWith("/api/summary/connected-history")
    || targetUrl.startsWith("/api/monitoring/runs")
    || targetUrl === "data/ui_config.json"
  );
}

function readPersistentCachedGetJson(url, maxAgeMs = API_STALE_RESPONSE_TTL_MS) {
  if (!isPersistentDashboardGet(url) || !canUseSessionStorage()) return null;
  const storageKey = `${DASHBOARD_CACHE_STORAGE_PREFIX}${buildApiUrl(url)}`;
  let raw = "";
  try {
    raw = String(sessionStorage.getItem(storageKey) || "");
  } catch (err) {
    return null;
  }
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw);
    const savedAtMs = Number(parsed?.savedAtMs || 0);
    if (!Number.isFinite(savedAtMs) || Date.now() - savedAtMs > Math.max(0, Number(maxAgeMs || 0))) {
      try {
        sessionStorage.removeItem(storageKey);
      } catch (cleanupErr) {
        // no-op
      }
      return null;
    }
    return cloneJsonValue(parsed.payload);
  } catch (err) {
    try {
      sessionStorage.removeItem(storageKey);
    } catch (cleanupErr) {
      // no-op
    }
    return null;
  }
}

function readCachedGetJson(url, maxAgeMs = API_STALE_RESPONSE_TTL_MS) {
  if (!isCacheableDashboardGet(url)) return null;
  const key = buildApiUrl(url);
  const cached = GET_JSON_RESPONSE_CACHE.get(key);
  if (cached) {
    if (Date.now() - Number(cached.savedAtMs || 0) > Math.max(0, Number(maxAgeMs || 0))) {
      GET_JSON_RESPONSE_CACHE.delete(key);
    } else {
      return cloneJsonValue(cached.payload);
    }
  }
  return readPersistentCachedGetJson(url, maxAgeMs);
}

function writePersistentCachedGetJson(url, payload) {
  if (!isPersistentDashboardGet(url) || !canUseSessionStorage()) return;
  const storageKey = `${DASHBOARD_CACHE_STORAGE_PREFIX}${buildApiUrl(url)}`;
  let serialized = "";
  try {
    serialized = JSON.stringify({
      savedAtMs: Date.now(),
      payload: cloneJsonValue(payload),
    });
  } catch (err) {
    return;
  }
  if (!serialized || serialized.length > DASHBOARD_CACHE_STORAGE_MAX_CHARS) {
    try {
      sessionStorage.removeItem(storageKey);
    } catch (cleanupErr) {
      // no-op
    }
    return;
  }
  try {
    sessionStorage.setItem(storageKey, serialized);
  } catch (err) {
    try {
      sessionStorage.removeItem(storageKey);
    } catch (cleanupErr) {
      // no-op
    }
  }
}

function writeCachedGetJson(url, payload) {
  if (!isCacheableDashboardGet(url)) return;
  const key = buildApiUrl(url);
  GET_JSON_RESPONSE_CACHE.set(key, {
    savedAtMs: Date.now(),
    payload: cloneJsonValue(payload),
  });
  writePersistentCachedGetJson(url, payload);
}

function sleepMs(ms) {
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, Math.max(0, Number(ms || 0)));
  });
}

async function executeJsonGet(url, options = {}) {
  const targetUrl = buildApiUrl(url);
  const timeoutMs = Math.max(4000, Number(options.timeoutMs || API_REQUEST_TIMEOUT_MS));
  const retryDelaysMs = Array.isArray(options.retryDelaysMs) && options.retryDelaysMs.length
    ? options.retryDelaysMs
    : API_GET_RETRY_DELAYS_MS;
  const allowStale = options.allowStale !== false;
  const staleTtlMs = Math.max(0, Number(options.staleTtlMs || API_STALE_RESPONSE_TTL_MS));
  let lastError = null;

  for (let attemptIndex = 0; attemptIndex < retryDelaysMs.length; attemptIndex += 1) {
    if (attemptIndex > 0) {
      await sleepMs(retryDelaysMs[attemptIndex]);
    }

    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timeoutId = controller
      ? globalThis.setTimeout(() => controller.abort(), timeoutMs)
      : null;
    let response;
    try {
      response = await fetch(targetUrl, {
        credentials: "include",
        cache: "no-store",
        signal: controller ? controller.signal : undefined,
      });
      let payload = {};
      try {
        payload = await response.json();
      } catch (err) {
        payload = {};
      }
      if (!response.ok) {
        const redirect = String(payload.redirect || "").trim();
        if (redirect) {
          window.location.assign(buildAppUrl(redirect));
          throw new Error(`AUTH_REDIRECT_${response.status}`);
        }
        throw new Error(String(payload.error || payload.message || response.status));
      }
      writeCachedGetJson(url, payload);
      return payload;
    } catch (err) {
      if (controller && err && err.name === "AbortError") {
        lastError = new Error(`timeout:${timeoutMs}`);
      } else {
        lastError = err;
      }
      if (String((lastError || {}).message || "").startsWith("AUTH_REDIRECT_")) {
        throw lastError;
      }
    } finally {
      if (timeoutId !== null) globalThis.clearTimeout(timeoutId);
    }
  }

  if (allowStale) {
    const cachedPayload = readCachedGetJson(url, staleTtlMs);
    if (cachedPayload !== null) return cachedPayload;
  }
  throw lastError || new Error("request_failed");
}

async function getJson(url, options = {}) {
  const targetUrl = buildApiUrl(url);
  if (!options.bypassInflight) {
    const inFlight = GET_JSON_INFLIGHT_REQUESTS.get(targetUrl);
    if (inFlight) {
      return cloneJsonValue(await inFlight);
    }
  }

  const requestPromise = executeJsonGet(url, options);
  if (!options.bypassInflight) {
    GET_JSON_INFLIGHT_REQUESTS.set(targetUrl, requestPromise);
  }
  try {
    return cloneJsonValue(await requestPromise);
  } finally {
    if (!options.bypassInflight) {
      const active = GET_JSON_INFLIGHT_REQUESTS.get(targetUrl);
      if (active === requestPromise) {
        GET_JSON_INFLIGHT_REQUESTS.delete(targetUrl);
      }
    }
  }
}

async function putJson(url, body) {
  const targetUrl = buildApiUrl(url);
  const response = await fetch(targetUrl, {
    method: "PUT",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  let payload = {};
  try {
    payload = await response.json();
  } catch (err) {
    payload = {};
  }
  if (!response.ok) {
    throw new Error(String(payload.error || payload.message || response.status));
  }
  return payload;
}

function buildStateUrl(runId = null) {
  const normalizedRun = String(runId || "").trim();
  if (!normalizedRun) return "/api/state";
  return `/api/state?run_id=${encodeURIComponent(normalizedRun)}`;
}

function buildQualityLiteUrl(runId = null, pivotIds = []) {
  const params = new URLSearchParams();
  const normalizedRun = String(runId || "").trim();
  if (normalizedRun) {
    params.set("run_id", normalizedRun);
  }
  const normalizedPivotIds = normalizePivotIdList(pivotIds);
  if (normalizedPivotIds.length) {
    params.set("pivot_ids", normalizedPivotIds.join(","));
  }
  const query = params.toString();
  return query ? `/api/quality-lite?${query}` : "/api/quality-lite";
}

function buildSummaryCardsHistoryUrl() {
  return "/api/summary/cards-history";
}

function buildPivotPanelUrl(pivotId, runId = null, sessionId = null) {
  const normalized = String(pivotId || "").trim();
  if (!normalized) return "";
  const params = new URLSearchParams();
  const normalizedRun = String(runId || "").trim();
  if (normalizedRun) {
    params.set("run_id", normalizedRun);
  }
  const normalizedSession = String(sessionId || "").trim();
  if (normalizedSession) {
    params.set("session_id", normalizedSession);
  }
  const query = params.toString();
  const base = `/api/pivot/${encodeURIComponent(normalized)}/panel`;
  return query ? `${base}?${query}` : base;
}

function buildPivotTableColumnsUrl() {
  return "/api/user/pivot-table-columns";
}

function normalizeRunId(value) {
  return String(value || "").trim();
}

function pickBestRunIdFromRuns(runs) {
  const list = Array.isArray(runs) ? runs : [];
  if (!list.length) return null;

  const normalized = list
    .map((item) => {
      const runId = normalizeRunId(item?.run_id);
      if (!runId) return null;
      return {
        runId,
        isActive: !!item?.is_active,
        pivotCount: Number(item?.pivot_count || 0),
      };
    })
    .filter(Boolean);

  if (!normalized.length) return null;

  const activeWithData = normalized.find((item) => item.isActive && item.pivotCount > 0);
  if (activeWithData) return activeWithData.runId;

  const latestWithData = normalized.find((item) => item.pivotCount > 0);
  if (latestWithData) return latestWithData.runId;

  const activeAny = normalized.find((item) => item.isActive);
  if (activeAny) return activeAny.runId;

  return normalized[0].runId;
}

async function autoResolveRunIdFromBackend({ force = false, allowOverride = false } = {}) {
  const currentRunId = normalizeRunId(state.selectedRunId);
  if (currentRunId && !allowOverride) return false;

  const nowMs = Date.now();
  if (!force && nowMs - Number(state.lastRunResolveAttemptMs || 0) < 30000) {
    return false;
  }
  state.lastRunResolveAttemptMs = nowMs;

  let payload = null;
  try {
    payload = await getJson("/api/monitoring/runs?limit=200");
  } catch (err) {
    return false;
  }

  const resolvedRunId = pickBestRunIdFromRuns(payload?.runs);
  if (!resolvedRunId) return false;
  if (currentRunId && resolvedRunId === currentRunId) return false;

  state.selectedRunId = resolvedRunId;
  if (!state.runAutoDetected) {
    showToast("Sessão de monitoramento restaurada automaticamente.", "success", 3200);
    state.runAutoDetected = true;
  }
  return true;
}

function renderSessionControls() {
  return;
}

function isLocalhostRuntime() {
  const host = String(location.hostname || "").trim().toLowerCase();
  return host === "localhost" || host === "127.0.0.1";
}

async function checkDevReloadToken() {
  const payload = await getJson("/api/dev/reload-token");
  return String(payload.token || "");
}

function startDevAutoReload() {
  if (!isLocalhostRuntime()) return;

  const pollReloadToken = async () => {
    try {
      const token = await checkDevReloadToken();
      if (!token) return;

      if (!state.devReloadToken) {
        state.devReloadToken = token;
        return;
      }

      if (state.devReloadToken !== token) {
        location.reload();
      }
    } catch (err) {
      return;
    }
  };

  pollReloadToken();
  setInterval(pollReloadToken, 1500);
}

function applyFilterSort() {
  const list = [...state.pivots];
  const needle = state.search.trim().toLowerCase();
  const selectedStatus = normalizeFilterKey(state.statusFilter) || "all";
  const selectedConnectivity = normalizeFilterKey(state.connectivityFilter) || "all";
  const selectedTechnology = normalizeFilterKey(state.technologyFilter) || "all";
  const selectedFirmware = normalizeFilterKey(state.firmwareFilter) || "all";
  const probeActiveRankingMode = state.sort === "probe_active_response_ratio_desc";

  let filtered = list.filter((item) => {
    const quality = getDisplayQuality(item);
    const status = getDisplayStatus(item);
    if (probeActiveRankingMode && !isProbeMonitoringEnabled(item)) return false;
    if (selectedStatus !== "all" && status.code !== selectedStatus) return false;
    if (selectedConnectivity !== "all" && quality.code !== selectedConnectivity) return false;
    if (needle) {
      const pivotId = String(item.pivot_id || "").toLowerCase();
      if (!pivotId.includes(needle)) return false;
    }
    if (selectedTechnology !== "all") {
      const pivotTechnology = normalizeFilterKey(pivotTechnologyValue(item));
      if (pivotTechnology !== selectedTechnology) return false;
    }
    if (selectedFirmware !== "all") {
      const pivotFirmware = normalizeFilterKey(pivotFirmwareValue(item));
      if (pivotFirmware !== selectedFirmware) return false;
    }
    return true;
  });

  filtered.sort((a, b) => {
    const ap = String(a.pivot_id || "");
    const bp = String(b.pivot_id || "");
    const aActivity = Number(a.last_activity_ts || 0);
    const bActivity = Number(b.last_activity_ts || 0);
    if (state.sort === "probe_active_response_ratio_desc") {
      return compareByProbeResponseRatioDesc(a, b, ap, bp, aActivity, bActivity);
    }
    if (state.sort === "pivot_asc") return ap.localeCompare(bp);
    if (state.sort === "missing_coordinates") {
      const aMissing = hasPivotCoordinates(a) ? 0 : 1;
      const bMissing = hasPivotCoordinates(b) ? 0 : 1;
      if (aMissing !== bMissing) return bMissing - aMissing;
      if (aActivity !== bActivity) return bActivity - aActivity;
      return ap.localeCompare(bp);
    }
    if (state.sort === "samples_desc") return compareBySamplesDesc(a, b, ap, bp, aActivity, bActivity);
    if (state.sort === "connected_pct_desc") {
      return compareByConnectedPctDesc(a, b, ap, bp, state.connectivitySummaryByPivotId);
    }
    if (state.sort === "disconnected_pct_desc") {
      return compareByDisconnectedPctDesc(a, b, ap, bp, state.connectivitySummaryByPivotId);
    }

    if (state.sort === "activity_desc") return bActivity - aActivity || ap.localeCompare(bp);
    if (state.sort === "activity_asc") return aActivity - bActivity || ap.localeCompare(bp);

    const aStateRank = Number(getDisplayStatus(a).rank ?? 99);
    const bStateRank = Number(getDisplayStatus(b).rank ?? 99);
    if (aStateRank !== bStateRank) return aStateRank - bStateRank;

    const aQualityRank = Number(getDisplayQuality(a).rank ?? 99);
    const bQualityRank = Number(getDisplayQuality(b).rank ?? 99);
    if (aQualityRank !== bQualityRank) return aQualityRank - bQualityRank;

    const aAlert = (a.probe || {}).alert ? 1 : 0;
    const bAlert = (b.probe || {}).alert ? 1 : 0;
    if (aAlert !== bAlert) return bAlert - aAlert;

    if (aActivity !== bActivity) return aActivity - bActivity;
    return ap.localeCompare(bp);
  });

  return filtered;
}

function pivotSampleCount(item) {
  const fromSummary = Number((item || {}).median_sample_count);
  if (Number.isFinite(fromSummary) && fromSummary >= 0) return Math.floor(fromSummary);
  const nested = Number((((item || {}).summary || {}).median_sample_count));
  if (Number.isFinite(nested) && nested >= 0) return Math.floor(nested);
  return 0;
}

function pivotProbeSummary(item) {
  const direct = (item || {}).probe;
  if (direct && typeof direct === "object") return direct;
  const nested = ((item || {}).summary || {}).probe;
  if (nested && typeof nested === "object") return nested;
  return {};
}

function isProbeMonitoringEnabled(item) {
  const probe = pivotProbeSummary(item);
  return toBoolean(probe.enabled, false);
}

function pivotProbeResponseRatioPct(item) {
  const probe = pivotProbeSummary(item);
  const ratio = Number(probe.response_ratio_pct);
  if (Number.isFinite(ratio) && ratio >= 0) return ratio;

  const sentCount = Number(probe.sent_count);
  const responseCount = Number(probe.response_count);
  if (Number.isFinite(sentCount) && sentCount > 0 && Number.isFinite(responseCount) && responseCount >= 0) {
    return (responseCount / sentCount) * 100;
  }
  return -1;
}

function compareByProbeResponseRatioDesc(
  a,
  b,
  ap = String(a?.pivot_id || ""),
  bp = String(b?.pivot_id || ""),
  aActivity = null,
  bActivity = null
) {
  const aRatio = pivotProbeResponseRatioPct(a);
  const bRatio = pivotProbeResponseRatioPct(b);
  if (aRatio !== bRatio) return bRatio - aRatio;

  const aProbe = pivotProbeSummary(a);
  const bProbe = pivotProbeSummary(b);
  const aSent = Number(aProbe.sent_count || 0);
  const bSent = Number(bProbe.sent_count || 0);
  if (aSent !== bSent) return bSent - aSent;

  const aResponse = Number(aProbe.response_count || 0);
  const bResponse = Number(bProbe.response_count || 0);
  if (aResponse !== bResponse) return bResponse - aResponse;

  const aAct = Number.isFinite(Number(aActivity)) ? Number(aActivity) : Number(a?.last_activity_ts || 0);
  const bAct = Number.isFinite(Number(bActivity)) ? Number(bActivity) : Number(b?.last_activity_ts || 0);
  if (aAct !== bAct) return bAct - aAct;

  return ap.localeCompare(bp);
}

function compareBySamplesDesc(a, b, ap = String(a?.pivot_id || ""), bp = String(b?.pivot_id || ""), aActivity = null, bActivity = null) {
  const aSamples = pivotSampleCount(a);
  const bSamples = pivotSampleCount(b);
  if (aSamples !== bSamples) return bSamples - aSamples;

  const aAct = Number.isFinite(Number(aActivity)) ? Number(aActivity) : Number(a?.last_activity_ts || 0);
  const bAct = Number.isFinite(Number(bActivity)) ? Number(bActivity) : Number(b?.last_activity_ts || 0);
  if (aAct !== bAct) return bAct - aAct;

  return ap.localeCompare(bp);
}

function parseSortablePct(value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric >= 0) return numeric;

  if (typeof value === "string") {
    const normalized = value.trim().replace("%", "").replace(",", ".");
    const parsed = Number(normalized);
    if (Number.isFinite(parsed) && parsed >= 0) return parsed;
  }

  return null;
}

function resolveConnectivitySummaryForPivot(item, connectivitySummaryByPivotId = null) {
  const pivotId = text((item || {}).pivot_id, "").trim();
  if (!pivotId) return null;
  const source =
    connectivitySummaryByPivotId && typeof connectivitySummaryByPivotId === "object"
      ? connectivitySummaryByPivotId
      : state.connectivitySummaryByPivotId;
  if (!source || typeof source !== "object") return null;
  const entry = source[pivotId];
  return entry && typeof entry === "object" ? entry : null;
}

function pivotConnectedPct(item, connectivitySummaryByPivotId = null) {
  const summary = resolveConnectivitySummaryForPivot(item, connectivitySummaryByPivotId);
  const candidates = [
    summary ? summary.connectedPct : null,
    summary ? summary.connected_pct : null,
  ];
  for (const candidate of candidates) {
    const parsed = parseSortablePct(candidate);
    if (parsed !== null) return parsed;
  }

  return 0;
}

function pivotDisconnectedPct(item, connectivitySummaryByPivotId = null) {
  const summary = resolveConnectivitySummaryForPivot(item, connectivitySummaryByPivotId);
  const candidates = [
    summary ? summary.disconnectedPct : null,
    summary ? summary.disconnected_pct : null,
    summary ? summary.attentionDisconnectedPct : null,
    summary ? summary.attention_disconnected_pct : null,
  ];
  for (const candidate of candidates) {
    const parsed = parseSortablePct(candidate);
    if (parsed !== null) return parsed;
  }

  return 0;
}

function compareByConnectedPctDesc(
  a,
  b,
  ap = String(a?.pivot_id || ""),
  bp = String(b?.pivot_id || ""),
  connectivitySummaryByPivotId = null
) {
  const aPct = pivotConnectedPct(a, connectivitySummaryByPivotId);
  const bPct = pivotConnectedPct(b, connectivitySummaryByPivotId);
  if (aPct !== bPct) return bPct - aPct;
  return ap.localeCompare(bp);
}

function compareByDisconnectedPctDesc(
  a,
  b,
  ap = String(a?.pivot_id || ""),
  bp = String(b?.pivot_id || ""),
  connectivitySummaryByPivotId = null
) {
  const aPct = pivotDisconnectedPct(a, connectivitySummaryByPivotId);
  const bPct = pivotDisconnectedPct(b, connectivitySummaryByPivotId);
  if (aPct !== bPct) return bPct - aPct;
  return ap.localeCompare(bp);
}

function buildQualitySourceSignature(item) {
  const pivot = item && typeof item === "object" ? item : {};
  const status = pivot.status && typeof pivot.status === "object" ? pivot.status : {};
  const quality = pivot.quality && typeof pivot.quality === "object" ? pivot.quality : {};
  const lastCloud2 = pivot.last_cloud2 && typeof pivot.last_cloud2 === "object" ? pivot.last_cloud2 : {};
  const probe = pivotProbeSummary(pivot);

  return [
    text(pivot.run_id, "").trim(),
    text(pivot.session_id, "").trim(),
    Number(pivot.last_activity_ts || 0),
    Number(pivot.last_ping_ts || 0),
    Number(pivot.last_cloudv2_ts || 0),
    Number(lastCloud2.ts || 0),
    Number(pivot.median_sample_count || 0),
    pivot.median_ready ? 1 : 0,
    text(status.code, "").trim(),
    text(quality.code, "").trim(),
    Number(probe.sent_count || 0),
    Number(probe.response_count || 0),
  ].join("|");
}

function resolveVisiblePivotIdsForRefresh() {
  const fromState = Array.isArray(state.visiblePivotIds)
    ? state.visiblePivotIds.map((pivotId) => String(pivotId || "").trim()).filter(Boolean)
    : [];
  if (fromState.length) return fromState;

  const filtered = applyFilterSort();
  if (!filtered.length) return [];
  const totalPages = Math.max(1, Math.ceil(filtered.length / state.cardsPageSize));
  const safePage = Math.max(1, Math.min(totalPages, Number(state.cardsPage || 1)));
  const start = (safePage - 1) * state.cardsPageSize;
  const pageItems = filtered.slice(start, start + state.cardsPageSize);
  return pageItems.map((item) => text((item || {}).pivot_id, "").trim()).filter(Boolean);
}

function resetAllFilters() {
  state.search = "";
  state.sort = "critical";
  state.statusFilter = "all";
  state.connectivityFilter = "all";
  state.technologyFilter = "all";
  state.firmwareFilter = "all";
  state.cardsPage = 1;

  if (ui.searchInput) ui.searchInput.value = "";
  if (ui.sortSelect) ui.sortSelect.value = "critical";
  if (ui.statusFilterSelect) ui.statusFilterSelect.value = "all";
  if (ui.connectivityFilterSelect) ui.connectivityFilterSelect.value = "all";
  if (ui.technologyFilter) ui.technologyFilter.value = "all";
  if (ui.firmwareFilter) ui.firmwareFilter.value = "all";

  renderCards();
}

function queueVisibleCardsConnectivityRefresh() {
  if (!HAS_WINDOW) return;
  if (state.visibleConnectivityRefreshTimer !== null) {
    window.clearTimeout(state.visibleConnectivityRefreshTimer);
  }
  state.visibleConnectivityRefreshTimer = window.setTimeout(() => {
    state.visibleConnectivityRefreshTimer = null;
    void refreshQualityOverrides({ visibleOnly: true });
  }, 450);
}

function arraysShallowEqual(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;
  for (let index = 0; index < a.length; index += 1) {
    if (a[index] !== b[index]) return false;
  }
  return true;
}

function expandPivotTableColumnOrderKey(rawKey) {
  const key = String(rawKey || "").trim();
  if (!key) return [];
  if (key === "last_ping_at") return ["timeline"];
  if (key === "signal_technology") return ["signal", "technology"];
  return [key];
}

function normalizePivotTableColumnOrder(order) {
  const rawOrder = Array.isArray(order) ? order : [];
  const normalized = [];
  const seen = new Set();
  for (const item of rawOrder) {
    const expandedKeys = expandPivotTableColumnOrderKey(item);
    for (const key of expandedKeys) {
      if (!PIVOT_TABLE_COLUMN_KEYS.includes(key)) continue;
      if (seen.has(key)) continue;
      seen.add(key);
      normalized.push(key);
    }
  }
  for (const key of PIVOT_TABLE_COLUMN_KEYS) {
    if (seen.has(key)) continue;
    normalized.push(key);
  }
  return normalized;
}

function ensurePivotTableColumnOrder() {
  state.pivotTableColumnOrder = normalizePivotTableColumnOrder(state.pivotTableColumnOrder);
}

async function loadPivotTableColumnPreference() {
  if (state.pivotTablePreferenceLoaded) return;
  ensurePivotTableColumnOrder();
  try {
    const payload = await getJson(buildPivotTableColumnsUrl());
    if (payload && payload.ok) {
      state.pivotTableColumnOrder = normalizePivotTableColumnOrder(payload.columns);
    }
  } catch (err) {
    state.pivotTableColumnOrder = normalizePivotTableColumnOrder(state.pivotTableColumnOrder);
  }
  state.pivotTablePreferenceLoaded = true;
}

async function savePivotTableColumnPreference() {
  ensurePivotTableColumnOrder();
  try {
    await putJson(buildPivotTableColumnsUrl(), { columns: state.pivotTableColumnOrder });
  } catch (err) {
    showToast("Nao foi possivel salvar a ordem das colunas.", "warn", 2600);
  }
}

function updateCardsPager(totalPages) {
  const pagerSignature = `${state.cardsPage}|${totalPages}`;
  if (state.cardsPagerSignature === pagerSignature) return;
  state.cardsPagerSignature = pagerSignature;
  ui.cardsPageInfo.textContent = `Página ${state.cardsPage}/${totalPages}`;
  ui.cardsPrev.disabled = state.cardsPage <= 1;
  ui.cardsNext.disabled = state.cardsPage >= totalPages;
}

function buildPivotCardSignature(pivot) {
  const safePivot = pivot || {};
  const status = getDisplayStatus(safePivot);
  const quality = getDisplayQuality(safePivot);
  const cloud2 = safePivot.last_cloud2 || {};
  const timelineSegments = resolvePivotTimelineMiniSegments(safePivot)
    .map((segment) => `${segment.state}:${segment.ratio.toFixed(4)}`)
    .join(",");
  return [
    text(safePivot.pivot_id, "").trim(),
    text(status.code, "gray"),
    text(status.label, "Inicial"),
    text(quality.code, "green"),
    text(quality.label, "Estável"),
    formatTimestampFromTsOrValue(safePivot.last_ping_ts, safePivot.last_ping_at, true),
    formatTimestampFromTsOrValue(safePivot.last_cloudv2_ts, safePivot.last_cloudv2_at, true),
    formatTimestampFromTsOrValue(safePivot.last_activity_ts, safePivot.last_activity_at, true),
    timelineSegments,
    safePivot.median_ready ? 1 : 0,
    Number(safePivot.median_sample_count || 0),
    Number(safePivot.median_cloudv2_interval_sec || 0),
    text(safePivot.signal, text(cloud2.rssi, "-")),
    text(safePivot.technology, text(pivotTechnologyValue(safePivot), "-")),
    text(cloud2.firmware, "-"),
  ].join("|");
}

function buildPivotCardHtml(pivot) {
  const safePivot = pivot || {};
  const pivotIdRaw = text((safePivot || {}).pivot_id, "pivo");
  const pivotId = escapeHtml(pivotIdRaw);
  const selectionEnabled = canCurrentUserDeletePivots();
  const isSelected = selectionEnabled && isPivotSelected(pivotIdRaw);

  const cells = state.pivotTableColumnOrder
    .map((columnKey) => {
      const status = getDisplayStatus(safePivot);
      const quality = getDisplayQuality(safePivot);
      const cloud2 = safePivot.last_cloud2 || {};

      if (columnKey === "pivot_id") {
        return `<td class="pivot-table-text strong">${escapeHtml(text(safePivot.pivot_id, "-"))}</td>`;
      }
      if (columnKey === "status") {
        return `<td><span class="badge ${escapeHtml(text(status.code, "gray"))}">${escapeHtml(text(status.label, "Inicial"))}</span></td>`;
      }
      if (columnKey === "connectivity") {
        return `<td><span class="badge ${escapeHtml(text(quality.code, "green"))}">${escapeHtml(text(quality.label, "Estavel"))}</span></td>`;
      }
      if (columnKey === "timeline") {
        return `<td class="pivot-timeline-cell">${buildTimelineMiniHtml(safePivot)}</td>`;
      }
      if (columnKey === "last_cloudv2_at") {
        return `<td class="pivot-table-text">${buildPivotTableDateTimeHtml(safePivot.last_cloudv2_at, safePivot.last_cloudv2_ts)}</td>`;
      }
      if (columnKey === "median") {
        const medianReady = !!safePivot.median_ready;
        const samples = Number(safePivot.median_sample_count || 0);
        const medianText = medianReady
          ? `${fmtDuration(safePivot.median_cloudv2_interval_sec)} (${samples} amostras)`
          : `${samples} amostras (em analise)`;
        return `<td class="pivot-table-text">${escapeHtml(medianText)}</td>`;
      }
      if (columnKey === "last_activity_at") {
        return `<td class="pivot-table-text">${buildPivotTableDateTimeHtml(safePivot.last_activity_at, safePivot.last_activity_ts)}</td>`;
      }
      if (columnKey === "signal") {
        return `<td class="pivot-table-text">${escapeHtml(text(pivotSignalValue(safePivot)))}</td>`;
      }
      if (columnKey === "technology") {
        return `<td class="pivot-table-text">${escapeHtml(text(pivotTechnologyColumnValue(safePivot)))}</td>`;
      }
      if (columnKey === "firmware") {
        return `<td class="pivot-table-text">${escapeHtml(text(cloud2.firmware))}</td>`;
      }
      return `<td class="pivot-table-text">-</td>`;
    })
    .join("");

  const selectionCell = selectionEnabled
    ? `<td class="pivot-table-select">
      <label class="pivot-table-check">
        <input
          type="checkbox"
          class="pivot-select-checkbox"
          data-pivot-id="${pivotId}"
          aria-label="Selecionar pivô ${pivotId}"
          ${state.bulkPivotActionInFlight ? "disabled" : ""}
          ${isSelected ? "checked" : ""}
        />
      </label>
    </td>`
    : "";

  return `<tr class="pivot-row${isSelected ? " is-selected" : ""}" data-pivot-id="${pivotId}">
    ${selectionCell}
    <td class="pivot-table-open">
      <button class="ghost open-pivot" data-pivot="${pivotId}">Abrir visao</button>
    </td>
    ${cells}
  </tr>`;
}

function reorderPivotTableColumns(fromKey, toKey) {
  const source = String(fromKey || "").trim();
  const target = String(toKey || "").trim();
  if (!source || !target || source === target) return false;
  ensurePivotTableColumnOrder();
  const current = [...state.pivotTableColumnOrder];
  const fromIndex = current.indexOf(source);
  const toIndex = current.indexOf(target);
  if (fromIndex < 0 || toIndex < 0) return false;
  current.splice(fromIndex, 1);
  current.splice(toIndex, 0, source);
  state.pivotTableColumnOrder = normalizePivotTableColumnOrder(current);
  return true;
}

function clearPivotTableHeaderDragClasses() {
  for (const el of ui.cardsGrid.querySelectorAll(".pivot-table-header-cell")) {
    el.classList.remove("dragging");
    el.classList.remove("drag-over");
  }
}

function bindPivotTableHeaderDnD() {
  const headers = [...ui.cardsGrid.querySelectorAll(".pivot-table-header-cell[data-col-key]")];
  for (const header of headers) {
    header.addEventListener("dragstart", (event) => {
      const key = String(header.getAttribute("data-col-key") || "").trim();
      if (!key) return;
      state.pivotTableDragKey = key;
      clearPivotTableHeaderDragClasses();
      header.classList.add("dragging");
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = "move";
        event.dataTransfer.setData("text/plain", key);
      }
    });

    header.addEventListener("dragover", (event) => {
      const targetKey = String(header.getAttribute("data-col-key") || "").trim();
      if (!state.pivotTableDragKey || !targetKey || targetKey === state.pivotTableDragKey) return;
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
      clearPivotTableHeaderDragClasses();
      header.classList.add("drag-over");
    });

    header.addEventListener("dragleave", () => {
      header.classList.remove("drag-over");
    });

    header.addEventListener("drop", (event) => {
      event.preventDefault();
      const targetKey = String(header.getAttribute("data-col-key") || "").trim();
      const sourceKey = String(state.pivotTableDragKey || "").trim();
      clearPivotTableHeaderDragClasses();
      state.pivotTableDragKey = "";
      if (!targetKey || !sourceKey || sourceKey === targetKey) return;
      if (!reorderPivotTableColumns(sourceKey, targetKey)) return;
      renderCards();
      void savePivotTableColumnPreference();
    });

    header.addEventListener("dragend", () => {
      state.pivotTableDragKey = "";
      clearPivotTableHeaderDragClasses();
    });
  }
}

function renderPivotCardsFull(pageItems, pageKey) {
  const selectionEnabled = canCurrentUserDeletePivots();
  const pagePivotIds = pageItems.map((pivot) => text((pivot || {}).pivot_id, "").trim()).filter(Boolean);
  const allPageSelected = selectionEnabled && pagePivotIds.length > 0 && pagePivotIds.every((pivotId) => isPivotSelected(pivotId));
  const somePageSelected = selectionEnabled && pagePivotIds.some((pivotId) => isPivotSelected(pivotId));
  const selectionHeader = selectionEnabled
    ? `<th scope="col" class="pivot-table-select-head">
            <label class="pivot-table-check">
              <input
                type="checkbox"
                class="pivot-select-all-checkbox"
                aria-label="Selecionar todos os pivôs desta página"
                ${state.bulkPivotActionInFlight ? "disabled" : ""}
                ${allPageSelected ? "checked" : ""}
              />
            </label>
          </th>`
    : "";
  const headerColumns = state.pivotTableColumnOrder
    .map((columnKey) => {
      const def = PIVOT_TABLE_COLUMNS.find((item) => item.key === columnKey);
      const label = def ? def.label : columnKey;
      const stackedClass = PIVOT_TABLE_HEADER_STACKS[columnKey] ? " is-stacked" : "";
      return `<th scope="col" class="pivot-table-header-cell${stackedClass}" data-col-key="${escapeHtml(columnKey)}" draggable="true">${buildPivotTableHeaderLabelHtml(columnKey, label)}</th>`;
    })
    .join("");
  const rowsHtml = pageItems.map((pivot) => buildPivotCardHtml(pivot)).join("");
  ui.cardsGrid.innerHTML = `
    <div class="pivot-table-wrap">
      <table class="pivot-table" aria-label="Tabela de pivos monitorados">
        <thead>
          <tr>
            ${selectionHeader}
            <th scope="col" class="pivot-table-open-head">Abrir visao</th>
            ${headerColumns}
          </tr>
        </thead>
        <tbody>${rowsHtml}</tbody>
      </table>
    </div>
  `;
  bindPivotTableHeaderDnD();
  if (selectionEnabled) {
    const selectAllInput = ui.cardsGrid.querySelector(".pivot-select-all-checkbox");
    if (selectAllInput) {
      selectAllInput.indeterminate = somePageSelected && !allPageSelected;
    }
  }
  syncBulkPivotActionControls();

  const nextSignatures = {};
  for (const pivot of pageItems) {
    const pivotId = text((pivot || {}).pivot_id, "").trim();
    if (!pivotId) continue;
    nextSignatures[pivotId] = buildPivotCardSignature(pivot);
  }
  state.cardsRenderSignaturesByPivotId = nextSignatures;
  state.cardsRenderPageKey = pageKey;
  state.cardsRenderIsEmpty = false;
}

function renderHeader() {
  const raw = state.rawState || {};
  const updatedAt = formatTimestampFromTsOrValue(raw.updated_at_ts, raw.updated_at);
  const counts = raw.counts || {};
  const runMeta = raw.run || state.panelRunMeta || null;
  const startedAt = formatTimestampFromTsOrValue((runMeta || {}).started_at_ts, (runMeta || {}).started_at).trim();
  const usingHistoricalRun = !!text(state.selectedRunId, "").trim();
  const sessionHintText = usingHistoricalRun
    ? (startedAt
      ? `Exibindo sessão restaurada automaticamente (${startedAt}).`
      : "Exibindo sessão restaurada automaticamente.")
    : "Este painel exibe automaticamente os dados mais recentes.";
  const headerSignature = [
    updatedAt,
    Number(counts.pivots || 0),
    Number(counts.duplicate_drops || 0),
    usingHistoricalRun ? 1 : 0,
    sessionHintText,
  ].join("|");
  if (state.headerRenderSignature === headerSignature) return;
  state.headerRenderSignature = headerSignature;

  ui.updatedAt.textContent = `Última atualização: ${updatedAt}`;
  ui.countsMeta.textContent = `${counts.pivots || 0} pivôs • ${counts.duplicate_drops || 0} duplicidades`;
  if (ui.sessionHint) {
    ui.sessionHint.textContent = sessionHintText;
  }
}

function renderStatusSummary() {
  const liveCounts = computeSummaryCardCountsFromPivots(state.pivots);
  if (state.qualityOverridesReady) {
    state.lastStableSummaryCardCounts = liveCounts;
  }
  const { stateCounts, qualityCounts } = (
    state.qualityOverridesReady || !state.lastStableSummaryCardCounts
      ? liveCounts
      : state.lastStableSummaryCardCounts
  );
  const settings = (state.rawState || {}).settings || {};
  const minSamplesRaw = Number(settings.cloudv2_min_samples ?? 5);
  const minSamples = Number.isFinite(minSamplesRaw) && minSamplesRaw >= 1 ? Math.round(minSamplesRaw) : 5;
  const attentionThresholdRaw = Number(settings.attention_disconnected_pct_threshold ?? 20);
  const attentionThreshold = Number.isFinite(attentionThresholdRaw)
    ? Math.max(0, Math.min(100, attentionThresholdRaw))
    : 20;
  const criticalThresholdRaw = Number(settings.critical_disconnected_pct_threshold ?? 50);
  const criticalThreshold = Number.isFinite(criticalThresholdRaw)
    ? Math.max(attentionThreshold, Math.min(100, criticalThresholdRaw))
    : 50;

  const summaryItems = [
    {
      key: "all",
      label: "Total",
      value: stateCounts.all,
      tone: "neutral",
      tooltip: "Quantidade total de pivos monitorados no painel.",
    },
    {
      key: "connected",
      label: "Conectados",
      value: stateCounts.green,
      tone: "green",
      tooltip:
        "Pivos com Status Conectado. O status fica Conectado quando ha comunicacao recente dentro do limite de desconexao do proprio pivo.",
    },
    {
      key: "disconnected",
      label: "Desconectados",
      value: stateCounts.red,
      tone: "red",
      tooltip:
        "Pivos com Status Desconectado. O status vira Desconectado quando o tempo sem comunicacao ultrapassa o limite de desconexao calculado para o pivo.",
    },
    {
      key: "initial",
      label: "Iniciais",
      value: stateCounts.gray,
      tone: "gray",
      tooltip:
        `Pivos com Status Inicial. Isso ocorre enquanto o sistema ainda esta coletando dados para definir o comportamento normal (minimo de ${minSamples} amostras cloudv2).`,
    },
    {
      key: "quality_green",
      label: "Conectividade estavel",
      value: qualityCounts.green,
      tone: "green",
      tooltip:
        `Conectividade dentro do esperado no periodo selecionado: percentual desconectado ate ${attentionThreshold.toFixed(1)}% e sem sinais de instabilidade.`,
    },
    {
      key: "quality_calculating",
      label: "Em analise",
      value: qualityCounts.calculating,
      tone: "calculating",
      tooltip:
        `Conectividade ainda em analise. Ocorre quando o pivo nao atingiu amostras suficientes (minimo ${minSamples}) para calcular a mediana de intervalo.`,
    },
    {
      key: "quality_yellow",
      label: "Conectividade instavel",
      value: qualityCounts.yellow,
      tone: "yellow",
      tooltip:
        `Conectividade com alerta. Ocorre quando o percentual desconectado supera ${attentionThreshold.toFixed(1)}% sem passar do limite critico (${criticalThreshold.toFixed(1)}%), ou quando so ha sinais auxiliares no periodo.`,
    },
    {
      key: "quality_critical",
      label: "Conectividade critica",
      value: qualityCounts.critical,
      tone: "critical",
      tooltip:
        `Conectividade critica no periodo selecionado. Ocorre quando o percentual desconectado fica acima de ${criticalThreshold.toFixed(1)}%.`,
    },
  ];
  const summarySignature = [
    stateCounts.all,
    stateCounts.green,
    stateCounts.red,
    stateCounts.gray,
    qualityCounts.green,
    qualityCounts.calculating,
    qualityCounts.yellow,
    qualityCounts.critical,
    minSamples,
    attentionThreshold.toFixed(1),
    criticalThreshold.toFixed(1),
    state.summaryCardsHistoryLoading ? "loading" : "idle",
    text(state.summaryCardsHistoryError, ""),
    text((state.summaryCardsHistoryPayload || {}).source, ""),
    Array.isArray((state.summaryCardsHistoryPayload || {}).points)
      ? (state.summaryCardsHistoryPayload || {}).points.length
      : 0,
    Array.isArray((state.summaryCardsHistoryPayload || {}).points) && (state.summaryCardsHistoryPayload || {}).points.length
      ? `${(state.summaryCardsHistoryPayload || {}).points[0]?.ts || 0}:${(state.summaryCardsHistoryPayload || {}).points.at(-1)?.ts || 0}:${(state.summaryCardsHistoryPayload || {}).points.at(-1)?.total_count || 0}`
      : "no-history",
  ].join("|");
  if (state.statusSummaryRenderSignature === summarySignature) return;
  state.statusSummaryRenderSignature = summarySignature;

  ui.statusSummary.innerHTML = summaryItems
    .map(
      (item, index, list) => {
        const tone = escapeHtml(text(item.tone, "neutral"));
        const alignClass = index >= (list.length - 2) ? " summary-pill-wrap--align-right" : "";
        const popoverWrapClass = SUMMARY_CARD_TEXT_ONLY_KEYS.has(item.key)
          ? "summary-pill-wrap--info"
          : "summary-pill-wrap--chart";
        const popoverHtml = SUMMARY_CARD_TEXT_ONLY_KEYS.has(item.key)
          ? buildSummaryCardInfoPopoverHtml(item)
          : buildSummaryCardHistoryPopoverHtml(item);
        return `
        <div class="summary-pill-wrap ${popoverWrapClass} summary-pill-wrap--${tone}${alignClass}" tabindex="0" aria-label="${escapeHtml(item.label + ": " + item.tooltip)}">
          <div class="summary-pill summary-pill--${tone}">
            <span>${escapeHtml(item.label)}</span>
            <strong>${item.value}</strong>
          </div>
          ${popoverHtml}
        </div>
      `;
      }
    )
    .join("");
}

function renderPending() {
  const raw = state.rawState || {};
  const enabled = !!(((raw.settings || {}).show_pending_ping_pivots));
  const pending = raw.pending_ping || [];
  const pendingSignature = !enabled
    ? "disabled"
    : `enabled|${pending
      .slice(0, 40)
      .map((item) => `${text(item.pivot_id)}:${Number(item.count || 0)}:${formatTimestampFromTsOrValue(item.last_seen_ts, item.last_seen_at, true)}`)
      .join("|")}`;
  if (state.pendingRenderSignature === pendingSignature) return;
  state.pendingRenderSignature = pendingSignature;

  if (!enabled) {
    ui.pendingPanel.hidden = true;
    return;
  }

  ui.pendingPanel.hidden = false;
  if (!pending.length) {
    ui.pendingList.innerHTML = `<div class="empty">Nenhum pivô com atividade inicial no momento.</div>`;
    return;
  }

  ui.pendingList.innerHTML = pending
    .slice(0, 40)
    .map((item) => {
      const id = escapeHtml(text(item.pivot_id));
      const count = Number(item.count || 0);
      const last = escapeHtml(formatTimestampFromTsOrValue(item.last_seen_ts, item.last_seen_at));
      return `<div class="list-item"><strong>${id}</strong> com ${count} registro(s) (última atualização em ${last})</div>`;
    })
    .join("");
}

function renderCards() {
  renderExpectedPivotAdmin();
  ensurePivotTableColumnOrder();
  const filtered = applyFilterSort();
  const totalPages = Math.max(1, Math.ceil(filtered.length / state.cardsPageSize));
  if (state.cardsPage > totalPages) state.cardsPage = totalPages;
  const start = (state.cardsPage - 1) * state.cardsPageSize;
  const pageItems = filtered.slice(start, start + state.cardsPageSize);
  const nextVisiblePivotIds = pageItems.map((pivot) => text((pivot || {}).pivot_id, "").trim()).filter(Boolean);
  state.visiblePivotIds = nextVisiblePivotIds;
  updateCardsPager(totalPages);

  if (!pageItems.length) {
    if (!state.cardsRenderIsEmpty) {
      ui.cardsGrid.innerHTML = `<div class="empty">Nenhum pivô encontrado para os filtros selecionados.</div>`;
      state.cardsRenderIsEmpty = true;
      state.cardsRenderPageKey = "empty";
      state.cardsRenderSignaturesByPivotId = {};
    }
    syncBulkPivotActionControls();
    return;
  }

  const pagePivotIds = nextVisiblePivotIds;
  const columnOrderSignature = state.pivotTableColumnOrder.join(",");
  const selectionSignature = canCurrentUserDeletePivots()
    ? pagePivotIds.map((pivotId) => (isPivotSelected(pivotId) ? "1" : "0")).join("")
    : "disabled";
  const bulkActionSignature = state.bulkPivotActionInFlight ? "busy" : "idle";
  const pageKey = `${state.cardsPage}|${totalPages}|${pagePivotIds.join(",")}|${columnOrderSignature}|${selectionSignature}|${bulkActionSignature}`;
  const nextSignatures = {};
  for (const pivot of pageItems) {
    const pivotId = text((pivot || {}).pivot_id, "").trim();
    if (!pivotId) continue;
    nextSignatures[pivotId] = buildPivotCardSignature(pivot);
  }
  const previousPivotIds = Object.keys(state.cardsRenderSignaturesByPivotId);
  const hasSamePivotSet = arraysShallowEqual(previousPivotIds, pagePivotIds);
  const hasSameSignatures = hasSamePivotSet
    && pagePivotIds.every((pivotId) => state.cardsRenderSignaturesByPivotId[pivotId] === nextSignatures[pivotId]);
  const hasSamePageKey = state.cardsRenderPageKey === pageKey;

  if (!(state.cardsRenderIsEmpty || !hasSamePageKey || !hasSamePivotSet || !hasSameSignatures)) {
    return;
  }

  renderPivotCardsFull(pageItems, pageKey);
}

function renderPivotMetrics(pivot, statusView = null, qualityView = null, connectivityView = null) {
  const summary = pivot.summary || {};
  const metrics = pivot.metrics || {};
  const status = statusView || summary.status || {};
  const statusCode = text(status.code, "gray");
  const statusLabel = text(status.label, (STATUS_META[statusCode] || STATUS_META.gray).label);
  const safeStatusReason = sanitizeUserText(status.reason) || statusReasonByCode(statusCode);
  const quality = qualityView || summary.quality || {};
  const qualityCode = text(quality.code, "green");
  const qualityLabel = (QUALITY_META[qualityCode] || QUALITY_META.green).label;
  const safeQualityReason = sanitizeUserText(quality.reason) || qualityReasonByCode(qualityCode);
  const probe = summary.probe || {};
  const sentCount = Number(probe.sent_count || 0);
  const responseCount = Number(probe.response_count || 0);
  const timeoutCount = Number(probe.timeout_count || 0);
  const latencySampleCount = Number(probe.latency_sample_count || 0);
  const responseCoverageText =
    sentCount > 0
      ? `${responseCount}/${sentCount} (${fmtPercent(probe.response_ratio_pct)})`
      : `${responseCount}/${sentCount}`;
  const connectedPctText =
    connectivityView && Number.isFinite(Number(connectivityView.connectedPct))
      ? fmtPercentWhole(connectivityView.connectedPct)
      : fmtPercent(summary.connected_pct);
  const disconnectedPctText =
    connectivityView && Number.isFinite(Number(connectivityView.disconnectedPct))
      ? fmtPercentWhole(connectivityView.disconnectedPct)
      : fmtPercent(summary.disconnected_pct);

  const cards = [
    { key: "status", label: "Status atual", value: statusLabel },
    { key: "status_reason", label: "Detalhe do status", value: safeStatusReason },
    { key: "connectivity", label: "Conectividade", value: qualityLabel },
    { key: "connectivity_reason", label: "Detalhe da conectividade", value: safeQualityReason },
    { key: "connected_pct", label: "% Conectado (janela)", value: connectedPctText },
    { key: "disconnected_pct", label: "% Desconectado (janela)", value: disconnectedPctText },
    {
      key: "last_ping",
      label: "Última atualização de conectividade",
      value: formatTimestampFromTsOrValue(summary.last_ping_ts, summary.last_ping_at),
    },
    {
      key: "last_cloudv2",
      label: "Última atualização de dados",
      value: formatTimestampFromTsOrValue(summary.last_cloudv2_ts, summary.last_cloudv2_at),
    },
    {
      key: "median_cloudv2",
      label: "Intervalo típico de atualização",
      value: summary.median_ready
        ? `${fmtDuration(summary.median_cloudv2_interval_sec)} (${summary.median_sample_count} amostras)`
        : `${summary.median_sample_count} amostras`,
    },
    { key: "drops_24h", label: "Desconexões (24h)", value: text(metrics.drops_24h, "0") },
    { key: "drops_7d", label: "Desconexões (7 dias)", value: text(metrics.drops_7d, "0") },
    { key: "last_drop_duration", label: "Duração da última desconexão", value: fmtDuration(metrics.last_drop_duration_sec) },
    { key: "last_rssi", label: "Último sinal (RSSI)", value: text(metrics.last_rssi) },
    {
      key: "last_technology",
      label: "Última tecnologia de rede",
      value: isPivotConcentrator(pivot) ? "concentrador" : text(metrics.last_technology),
    },
    { key: "firmware", label: "Firmware", value: text(metrics.last_firmware) },
    { key: "timeout_streak", label: "Falhas consecutivas de resposta", value: text(probe.timeout_streak, "0") },
    { key: "response_ratio", label: "Respostas/solicitações", value: responseCoverageText },
    { key: "timeout_total", label: "Total de falhas de resposta", value: text(timeoutCount, "0") },
    { key: "latency_last", label: "Latência da última resposta", value: fmtSecondsPrecise(probe.latency_last_sec) },
    { key: "latency_avg", label: "Latência média", value: fmtSecondsPrecise(probe.latency_avg_sec) },
    { key: "latency_median", label: "Latência mediana", value: fmtSecondsPrecise(probe.latency_median_sec) },
    {
      key: "latency_min_max",
      label: "Latência mínima/máxima",
      value: `${fmtSecondsPrecise(probe.latency_min_sec)} / ${fmtSecondsPrecise(probe.latency_max_sec)}`,
    },
    { key: "latency_samples", label: "Amostras de latência", value: text(latencySampleCount, "0") },
  ];

  const collapsedCardKeys = new Set([
    "status",
    "connectivity",
    "connected_pct",
    "disconnected_pct",
    "last_rssi",
    "last_technology",
  ]);
  const hasExtraCards = cards.length > collapsedCardKeys.size;
  const visibleCards = state.pivotMetricsExpanded
    ? cards
    : cards.filter((item) => collapsedCardKeys.has(String(item.key || "")));

  if (ui.pivotMoreInfoBtn) {
    ui.pivotMoreInfoBtn.hidden = !hasExtraCards;
    ui.pivotMoreInfoBtn.textContent = state.pivotMetricsExpanded ? "Menos informações" : "Mais informações";
    ui.pivotMoreInfoBtn.setAttribute("aria-expanded", state.pivotMetricsExpanded ? "true" : "false");
  }

  if (ui.pivotMetricsToggleBtn) {
    ui.pivotMetricsToggleBtn.hidden = !hasExtraCards;
    ui.pivotMetricsToggleBtn.textContent = state.pivotMetricsExpanded ? "^" : "v";
    ui.pivotMetricsToggleBtn.setAttribute("aria-expanded", state.pivotMetricsExpanded ? "true" : "false");
    ui.pivotMetricsToggleBtn.setAttribute(
      "aria-label",
      state.pivotMetricsExpanded ? "Recolher informações" : "Mostrar mais informações",
    );
  }

  ui.pivotMetrics.innerHTML = visibleCards
    .map((item) => {
      return `
      <div class="metric">
        <div class="label">${escapeHtml(item.label)}</div>
        <div class="value">${escapeHtml(item.value)}</div>
      </div>`;
    })
    .join("");
}

function connectivityRangeSeconds(presetValue) {
  if (presetValue === "24h") return 24 * 3600;
  if (presetValue === "48h") return 48 * 3600;
  if (presetValue === "7d") return 7 * 86400;
  if (presetValue === "15d") return 15 * 86400;
  if (presetValue === "30d") return 30 * 86400;
  return null;
}

function maxEventTs(events) {
  if (!Array.isArray(events)) return null;
  let maxTs = null;
  for (const event of events) {
    const ts = Number((event || {}).ts || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    if (maxTs === null || ts > maxTs) maxTs = ts;
  }
  return maxTs;
}

function resolveTimelineReferenceNowTs(pivot) {
  const wallClockNowTs = Math.floor(Date.now() / 1000);
  const safePivot = pivot || {};
  const payloadUpdatedTs = Number(safePivot.updated_at_ts || 0);
  const timelineLastTs = maxEventTs(safePivot.timeline);
  const probeLastTs = maxEventTs(safePivot.probe_events);
  const cloud2LastTs = maxEventTs(safePivot.cloud2_events);
  const pivotSessionUpdatedTs = Number(((safePivot.session || {}).updated_at_ts) || 0);
  const pivotRunUpdatedTs = Number(((safePivot.run || {}).updated_at_ts) || 0);
  const panelSessionUpdatedTs = Number(((state.panelSessionMeta || {}).updated_at_ts) || 0);
  const panelRunUpdatedTs = Number(((state.panelRunMeta || {}).updated_at_ts) || 0);
  const rawStateUpdatedTs = Number(((state.rawState || {}).updated_at_ts) || 0);

  let persistedLatestTs = 0;
  for (const candidate of [payloadUpdatedTs, timelineLastTs, probeLastTs, cloud2LastTs]) {
    const ts = Number(candidate || 0);
    if (Number.isFinite(ts) && ts > persistedLatestTs) persistedLatestTs = ts;
  }

  let serverReferenceNowTs = 0;
  for (const candidate of [
    payloadUpdatedTs,
    pivotSessionUpdatedTs,
    pivotRunUpdatedTs,
    panelSessionUpdatedTs,
    panelRunUpdatedTs,
    rawStateUpdatedTs,
  ]) {
    const ts = Number(candidate || 0);
    if (Number.isFinite(ts) && ts > serverReferenceNowTs) serverReferenceNowTs = ts;
  }

  const pivotRun = safePivot.run && typeof safePivot.run === "object" ? safePivot.run : null;
  const stateRun = (state.rawState || {}).run;
  const runMeta = pivotRun || state.panelRunMeta || stateRun || null;
  const isRunActive =
    runMeta && Object.prototype.hasOwnProperty.call(runMeta, "is_active")
      ? !!runMeta.is_active
      : String((state.rawState || {}).mode || "").toLowerCase() === "live";

  if (isRunActive) {
    const liveReferenceTs = Math.max(serverReferenceNowTs || 0, persistedLatestTs || 0);
    if (liveReferenceTs > 0) return liveReferenceTs;
    return Math.max(wallClockNowTs, persistedLatestTs || 0);
  }
  if (persistedLatestTs > 0) {
    return persistedLatestTs;
  }
  return wallClockNowTs;
}

function getConnectivityEventsPanelCapped(pivot) {
  return getConnectivityEventsPanelFiltered(pivot, "global");
}

function buildConnectivityEventsTxt(pivot, filterKey) {
  const events = getConnectivityEventsPanelFiltered(pivot, filterKey);
  return events
    .map((event) => {
      const timestamp = formatConnectivityEventTxtTimestamp(event);
      const payload = resolveConnectivityEventRawPayload(event) || "Payload nao disponivel para este evento.";
      return `${timestamp} -> ${payload}`;
    })
    .join("\n");
}

function downloadConnectivityEventsTxt(pivot, filterKey) {
  if (!HAS_DOM || !HAS_WINDOW) return false;
  const safePivotId = sanitizeDownloadFilenamePart((pivot || {}).pivot_id || "pivot");
  const safeTopic = sanitizeDownloadFilenamePart(normalizeConnectivityEventTopicFilter(filterKey));
  const content = buildConnectivityEventsTxt(pivot, filterKey);
  const blob = new Blob([content ? `${content}\n` : ""], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${safePivotId}_${safeTopic}_eventos.txt`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  return true;
}

function getSelectedPivotConfig() {
  const summary = (state.pivotData && typeof state.pivotData.summary === "object") ? state.pivotData.summary : {};
  const config = summary && typeof summary.pivot_config === "object" ? summary.pivot_config : {};
  return config || {};
}

function setPivotConfigField(element, value) {
  if (!element) return;
  element.value = value === null || value === undefined || value === "" ? "-" : String(value);
}

function renderPivotConfigModal() {
  const pivotId = String(state.selectedPivot || "").trim();
  const config = getSelectedPivotConfig();
  setPivotConfigField(ui.pivotConfigContactor, config.contactor);
  setPivotConfigField(ui.pivotConfigPressure, config.pressure);
  setPivotConfigField(ui.pivotConfigPressurizationTime, config.pressurization_time);
  setPivotConfigField(ui.pivotConfigOnTime, config.on_time);
  setPivotConfigField(ui.pivotConfigOffTime, config.off_time);
  setPivotConfigField(ui.pivotConfigReadTime, config.read_time);

  if (ui.pivotConfigHint) {
    const lastResponse = formatTimestampFromTsOrValue(config.last_response_ts, config.last_response_at);
    const lastRequest = formatTimestampFromTsOrValue(config.last_request_ts, config.last_request_at);
    if (!pivotId) {
      ui.pivotConfigHint.textContent = "Abra a visão de um pivô para pedir configuração.";
    } else if (config.pending) {
      ui.pivotConfigHint.textContent = lastRequest && lastRequest !== "-"
        ? `Pedido enviado em ${lastRequest}. Aguardando resposta.`
        : "Pedido enviado. Aguardando resposta.";
    } else if (lastResponse && lastResponse !== "-") {
      ui.pivotConfigHint.textContent = `Última resposta recebida em ${lastResponse}.`;
    } else {
      ui.pivotConfigHint.textContent = "Peça a configuração para preencher os campos.";
    }
  }

  if (ui.requestPivotConfigBtn) {
    ui.requestPivotConfigBtn.disabled = !pivotId || state.pivotConfigRequestInFlight;
    ui.requestPivotConfigBtn.textContent = state.pivotConfigRequestInFlight ? "Pedindo..." : "Pedir configuração";
  }
}

function openSettingsModal() {
  if (!ui.settingsModal) return;
  renderPivotConfigModal();
  ui.settingsModal.hidden = false;
  ui.settingsModalClose?.focus();
}

function closeSettingsModal() {
  if (!ui.settingsModal) return;
  ui.settingsModal.hidden = true;
  ui.pivotSettingsBtn?.focus();
}

function normalizeRange(pivot) {
  const nowTs = resolveTimelineReferenceNowTs(pivot);
  const timeline = Array.isArray((pivot || {}).timeline) ? pivot.timeline : [];
  let minTs = nowTs;
  for (const event of timeline) {
    const ts = Number(event.ts || 0);
    if (Number.isFinite(ts) && ts > 0 && ts < minTs) minTs = ts;
  }
  if (!timeline.length) minTs = nowTs - 24 * 3600;

  let startTs = minTs;
  let endTs = nowTs;

  const preset = state.connPreset;
  if (preset === "custom") {
    const parsedFrom = parseDateTimeLocal(state.connCustomFrom);
    const parsedTo = parseDateTimeLocal(state.connCustomTo);
    if (parsedFrom !== null) startTs = parsedFrom;
    if (parsedTo !== null) endTs = parsedTo;
  } else if (preset !== "total") {
    const windowSec = connectivityRangeSeconds(preset);
    if (windowSec) startTs = nowTs - windowSec;
  }

  if (startTs < minTs) startTs = minTs;
  if (endTs > nowTs) endTs = nowTs;
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || startTs >= endTs) {
    startTs = Math.max(minTs, nowTs - 24 * 3600);
    endTs = nowTs;
  }

  return { startTs, endTs, minTs, nowTs };
}

function normalizeProbeDelayRange(pivot) {
  const nowTs = resolveTimelineReferenceNowTs(pivot);
  const probeEvents = Array.isArray((pivot || {}).probe_events) ? pivot.probe_events : [];
  let minTs = nowTs;

  for (const event of probeEvents) {
    if (String(event.type || "").toLowerCase() !== "response") continue;
    const ts = Number(event.ts || 0);
    if (Number.isFinite(ts) && ts > 0 && ts < minTs) minTs = ts;
  }

  if (minTs === nowTs) minTs = Math.max(0, nowTs - 24 * 3600);

  let startTs = minTs;
  let endTs = nowTs;

  const preset = state.probeDelayPreset;
  if (preset === "custom") {
    const parsedFrom = parseDateTimeLocal(state.probeDelayCustomFrom);
    const parsedTo = parseDateTimeLocal(state.probeDelayCustomTo);
    if (parsedFrom !== null) startTs = parsedFrom;
    if (parsedTo !== null) endTs = parsedTo;
  } else if (preset !== "total") {
    const windowSec = connectivityRangeSeconds(preset);
    if (windowSec) startTs = nowTs - windowSec;
  }

  if (startTs < minTs) startTs = minTs;
  if (endTs > nowTs) endTs = nowTs;
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || startTs >= endTs) {
    startTs = Math.max(minTs, nowTs - 24 * 3600);
    endTs = nowTs;
  }

  return { startTs, endTs, minTs, nowTs };
}

function buildProbeDelaySeries(pivot, startTs, endTs) {
  const persistedPoints = (pivot.probe_delay_points || [])
    .map((point) => ({
      ts: Number(point.ts || 0),
      latencySec: Number(point.latency_sec),
      sampleCount: Number(point.sample_count || 0),
      avgLatencySec: Number(point.avg_latency_sec),
    }))
    .filter(
      (point) =>
        Number.isFinite(point.ts) &&
        point.ts >= startTs &&
        point.ts <= endTs &&
        Number.isFinite(point.latencySec) &&
        point.latencySec >= 0 &&
        Number.isFinite(point.avgLatencySec) &&
        point.avgLatencySec >= 0
    )
    .sort((a, b) => a.ts - b.ts);

  if (persistedPoints.length) {
    return {
      points: persistedPoints,
      responseCount: persistedPoints.length,
    };
  }

  const responses = (pivot.probe_events || [])
    .filter((event) => String(event.type || "").toLowerCase() === "response")
    .map((event) => ({
      ts: Number(event.ts || 0),
      latencySec: Number(event.latency_sec),
    }))
    .filter(
      (event) =>
        Number.isFinite(event.ts) &&
        event.ts >= startTs &&
        event.ts <= endTs &&
        Number.isFinite(event.latencySec) &&
        event.latencySec >= 0
    )
    .sort((a, b) => a.ts - b.ts);

  const points = [];
  let sum = 0;
  let count = 0;
  for (const response of responses) {
    count += 1;
    sum += response.latencySec;
    points.push({
      ts: response.ts,
      latencySec: response.latencySec,
      sampleCount: count,
      avgLatencySec: sum / count,
    });
  }

  return {
    points,
    responseCount: responses.length,
  };
}

function isValidRssiValue(value) {
  return Number.isFinite(value) && value >= 0 && value <= 31;
}

function collectRssiChartPoints(pivot) {
  const seen = new Set();
  const points = [];
  const sources = [
    Array.isArray((pivot || {}).rssiHistorySeries) ? pivot.rssiHistorySeries : [],
    Array.isArray((pivot || {}).rssiSeries) ? pivot.rssiSeries : [],
  ];

  for (const source of sources) {
    for (const item of source) {
      const ts = Number((item || {}).ts || 0);
      const rssi = Number((item || {}).rssi);
      if (!Number.isFinite(ts) || ts <= 0 || !isValidRssiValue(rssi)) continue;
      const key = `${ts}:${rssi}`;
      if (seen.has(key)) continue;
      seen.add(key);
      points.push({ ts, rssi });
    }
  }

  return points.sort((a, b) => a.ts - b.ts);
}

function shouldRenderRssiPanel(pivot) {
  if (!pivot || typeof pivot !== "object") return false;
  if (pivot.hasRssi === true) return true;
  return collectRssiChartPoints(pivot).length > 0;
}

function normalizeRssiRange(pivot) {
  const nowTs = resolveTimelineReferenceNowTs(pivot);
  const points = collectRssiChartPoints(pivot);
  let minTs = nowTs;

  for (const point of points) {
    if (point.ts < minTs) minTs = point.ts;
  }

  if (minTs === nowTs) minTs = Math.max(0, nowTs - 24 * 3600);

  let startTs = minTs;
  let endTs = nowTs;

  const preset = state.rssiPreset;
  if (preset === "custom") {
    const parsedFrom = parseDateTimeLocal(state.rssiCustomFrom);
    const parsedTo = parseDateTimeLocal(state.rssiCustomTo);
    if (parsedFrom !== null) startTs = parsedFrom;
    if (parsedTo !== null) endTs = parsedTo;
  } else if (preset !== "total") {
    const windowSec = connectivityRangeSeconds(preset);
    if (windowSec) startTs = nowTs - windowSec;
  }

  if (startTs < minTs) startTs = minTs;
  if (endTs > nowTs) endTs = nowTs;
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || startTs >= endTs) {
    startTs = Math.max(minTs, nowTs - 24 * 3600);
    endTs = nowTs;
  }

  return { startTs, endTs, minTs, nowTs };
}

function buildRssiSeries(pivot, startTs, endTs) {
  const points = collectRssiChartPoints(pivot)
    .filter(
      (point) =>
        Number.isFinite(point.ts) &&
        point.ts >= startTs &&
        point.ts <= endTs &&
        isValidRssiValue(point.rssi)
    )
    .sort((a, b) => a.ts - b.ts);

  return {
    points,
    sampleCount: points.length,
  };
}

function resolveRssiChartDomain(points, fallbackStartTs, fallbackEndTs) {
  const safePoints = Array.isArray(points)
    ? points
      .map((point) => ({
        ts: Number((point || {}).ts || 0),
        rssi: Number((point || {}).rssi),
      }))
      .filter((point) => Number.isFinite(point.ts) && point.ts > 0 && isValidRssiValue(point.rssi))
      .sort((a, b) => a.ts - b.ts)
    : [];

  if (safePoints.length >= 2) {
    const firstTs = safePoints[0].ts;
    const lastTs = safePoints[safePoints.length - 1].ts;
    if (Number.isFinite(firstTs) && Number.isFinite(lastTs) && lastTs > firstTs) {
      return { startTs: firstTs, endTs: lastTs };
    }
  }

  if (safePoints.length === 1) {
    const centerTs = safePoints[0].ts;
    return {
      startTs: centerTs - 30,
      endTs: centerTs + 30,
    };
  }

  const startTs = Number(fallbackStartTs || 0);
  const endTs = Number(fallbackEndTs || 0);
  if (Number.isFinite(startTs) && Number.isFinite(endTs) && endTs > startTs) {
    return { startTs, endTs };
  }

  const nowTs = Math.floor(Date.now() / 1000);
  return { startTs: nowTs - 3600, endTs: nowTs };
}

function resolveDisconnectThresholdSec(summary, settings) {
  const safeSummary = summary || {};
  const safeSettings = settings || {};
  const tolerance = Number(safeSettings.tolerance_factor || 3) || 3;
  const monitoredTopics = ["cloudv2", "cloudv2-ping", "cloudv2-info", "cloudv2-network"];
  const expectedByTopic = safeSummary.expected_by_topic_sec || {};

  const candidates = [];
  for (const topic of monitoredTopics) {
    const value = Number(expectedByTopic[topic]);
    if (Number.isFinite(value) && value > 0) candidates.push(value);
  }

  const maxExpectedFromSummary = Number(safeSummary.max_expected_interval_sec || 0);
  const maxExpectedIntervalSec =
    Number.isFinite(maxExpectedFromSummary) && maxExpectedFromSummary > 0
      ? maxExpectedFromSummary
      : candidates.length
      ? Math.max(...candidates)
      : Number(safeSettings.ping_expected_sec || 180);

  const thresholdFromSummary = Number(safeSummary.disconnect_threshold_sec || 0);
  const disconnectThresholdSec =
    Number.isFinite(thresholdFromSummary) && thresholdFromSummary > 0
      ? thresholdFromSummary
      : Math.max(30, maxExpectedIntervalSec * tolerance);

  return {
    disconnectThresholdSec,
    maxExpectedIntervalSec,
  };
}

function buildConnectivitySegments(pivot, startTs, endTs, settings = null) {
  const summary = (pivot || {}).summary || {};
  const monitoredTopics = ["cloudv2", "cloudv2-ping", "cloudv2-info", "cloudv2-network"];
  const { disconnectThresholdSec, maxExpectedIntervalSec } = resolveDisconnectThresholdSec(summary, settings);

  const events = (pivot.timeline || [])
    .filter((event) => monitoredTopics.includes(String(event.topic || "")))
    .map((event) => ({ topic: String(event.topic || ""), ts: Number(event.ts || 0) }))
    .filter((event) => Number.isFinite(event.ts) && event.ts > 0 && event.ts <= endTs)
    .sort((a, b) => a.ts - b.ts);

  let hasPrincipalPayloadInWindow = false;
  let hasAuxPayloadInWindow = false;
  for (const event of events) {
    if (event.ts < startTs || event.ts > endTs) continue;
    if (event.topic === "cloudv2") hasPrincipalPayloadInWindow = true;
    if (event.topic === "cloudv2-ping" || event.topic === "cloudv2-network" || event.topic === "cloudv2-info") {
      hasAuxPayloadInWindow = true;
    }
  }

  const segments = [];
  const safeStartTs = Number(startTs || 0);
  const safeEndTs = Number(endTs || 0);
  const durationRange = Math.max(1, safeEndTs - safeStartTs);
  const safeThreshold = Number(disconnectThresholdSec);

  const messageTs = events.map((event) => event.ts);
  const lastMessageTs = messageTs.length ? messageTs[messageTs.length - 1] : null;

  if (!Number.isFinite(safeThreshold) || safeThreshold <= 0 || !messageTs.length) {
    segments.push({
      state: "disconnected",
      start: safeStartTs,
      end: safeEndTs,
      duration: Math.max(1, safeEndTs - safeStartTs),
    });
    return {
      segments,
      disconnectThresholdSec: safeThreshold,
      maxExpectedIntervalSec,
      lastMessageTs,
      hasPrincipalPayloadInWindow,
      hasAuxPayloadInWindow,
      durationRange,
    };
  }

  const intervals = messageTs
    .map((ts) => ({ start: ts, end: ts + safeThreshold }))
    .sort((a, b) => a.start - b.start);

  const merged = [];
  for (const interval of intervals) {
    if (!merged.length) {
      merged.push({ start: interval.start, end: interval.end });
      continue;
    }
    const last = merged[merged.length - 1];
    if (interval.start <= last.end) {
      if (interval.end > last.end) last.end = interval.end;
      continue;
    }
    merged.push({ start: interval.start, end: interval.end });
  }

  let cursor = safeStartTs;
  for (const interval of merged) {
    if (interval.end <= safeStartTs) continue;
    if (interval.start >= safeEndTs) break;

    const segStart = Math.max(cursor, Math.max(safeStartTs, interval.start));
    const segEnd = Math.min(safeEndTs, interval.end);
    if (segEnd <= segStart) continue;

    if (segStart > cursor) {
      segments.push({
        state: "disconnected",
        start: cursor,
        end: segStart,
        duration: segStart - cursor,
      });
    }

    segments.push({
      state: "connected",
      start: segStart,
      end: segEnd,
      duration: segEnd - segStart,
    });
    cursor = segEnd;
    if (cursor >= safeEndTs) break;
  }

  if (cursor < safeEndTs) {
    segments.push({
      state: "disconnected",
      start: cursor,
      end: safeEndTs,
      duration: safeEndTs - cursor,
    });
  }

  const compacted = segments.filter((segment) => Number(segment.duration || 0) > 0);
  if (!compacted.length) {
    compacted.push({
      state: "disconnected",
      start: safeStartTs,
      end: safeEndTs,
      duration: Math.max(1, safeEndTs - safeStartTs),
    });
  }

  return {
    segments: compacted,
    disconnectThresholdSec: safeThreshold,
    maxExpectedIntervalSec,
    lastMessageTs,
    hasPrincipalPayloadInWindow,
    hasAuxPayloadInWindow,
    durationRange,
  };
}

function summarizeConnectivitySegments(segments, durationSec) {
  let connectedSec = 0;
  let disconnectedSec = 0;
  const safeDurationSec = Math.max(1, Number(durationSec || 0));

  for (const segment of segments || []) {
    const segmentDurationSec = Math.max(0, Number(segment.duration || 0));
    if (segmentDurationSec <= 0) continue;
    if (segment.state === "connected") connectedSec += segmentDurationSec;
    else disconnectedSec += segmentDurationSec;
  }

  const connectedPct = Math.max(0, Math.min(100, Math.round((connectedSec / safeDurationSec) * 100)));
  const disconnectedPct = Math.max(0, Math.min(100, 100 - connectedPct));

  return {
    connectedSec,
    disconnectedSec,
    connectedPct,
    disconnectedPct,
  };
}

function buildConnectivityQualityInput(connData, durationSec) {
  const summary = summarizeConnectivitySegments(connData.segments, durationSec);
  return {
    connectedSec: summary.connectedSec,
    disconnectedSec: summary.disconnectedSec,
    connectedPct: summary.connectedPct,
    disconnectedPct: summary.disconnectedPct,
    hasPrincipalPayloadInWindow: !!connData.hasPrincipalPayloadInWindow,
    hasAuxPayloadInWindow: !!connData.hasAuxPayloadInWindow,
  };
}

function buildConnectivityStatus(connData, referenceTs) {
  const lastSeenAtTs = connData ? connData.lastMessageTs : null;
  const thresholdSec = Number((connData || {}).disconnectThresholdSec);
  const ref = Number(referenceTs || 0);

  const connected = lastSeenAtTs !== null && Number.isFinite(ref) && ref - lastSeenAtTs <= thresholdSec;
  const code = connected ? "green" : "red";
  const meta = STATUS_META[code] || STATUS_META.gray;

  return {
    code,
    label: meta.label,
    reason: statusReasonByCode(code),
    rank: Number(meta.rank ?? 99),
    state: connected ? "connected" : "disconnected",
    referenceTs: ref,
    lastSeenAtTs,
    disconnectThresholdSec: thresholdSec,
  };
}

function computeConnectivityFromRange(pivot, startTs, endTs, settings = null) {
  const safeStart = Number(startTs || 0);
  const safeEnd = Number(endTs || 0);
  const durationSec = Math.max(1, safeEnd - safeStart);
  const connData = buildConnectivitySegments(pivot, safeStart, safeEnd, settings);
  const connectivityQualityInput = buildConnectivityQualityInput(connData, durationSec);
  const status = buildConnectivityStatus(connData, safeEnd);

  return {
    startTs: safeStart,
    endTs: safeEnd,
    durationSec,
    connData,
    segments: connData.segments,
    connectivityQualityInput,
    status,
  };
}

function computeConnectivityView(pivot) {
  const range = normalizeRange(pivot);
  const settings = ((state.rawState || {}).settings) || {};
  const view = computeConnectivityFromRange(pivot, range.startTs, range.endTs, settings);

  return {
    range,
    startTs: view.startTs,
    endTs: view.endTs,
    durationSec: view.durationSec,
    connData: view.connData,
    segments: view.segments,
    connectivityQualityInput: view.connectivityQualityInput,
    status: view.status,
  };
}

function buildQualityFromConnectivity(pivot, connectivitySummary) {
  const summary = pivot.summary || {};
  const fallbackQuality = summary.quality || {};
  const settings = (state.rawState || {}).settings || {};
  const minSamplesRaw = Number(settings.cloudv2_min_samples ?? 5);
  const minSamples = Number.isFinite(minSamplesRaw) && minSamplesRaw >= 1 ? Math.round(minSamplesRaw) : 5;
  const sampleCount = Math.max(0, Number(summary.median_sample_count || 0));
  const medianReady = !!summary.median_ready && sampleCount >= minSamples;
  const status = summary.status || {};
  const statusCode = text(status.code, "gray");
  const statusReason = text(status.reason, "");

  if (!medianReady) {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: `Aguardando amostras de cloudv2 para estimar mediana (${sampleCount}/${minSamples}).`,
      rank: QUALITY_META.calculating.rank,
    };
  }

  if (statusCode === "gray") {
    return {
      code: "calculating",
      label: QUALITY_META.calculating.label,
      reason: sanitizeUserText(statusReason) || qualityReasonByCode("calculating"),
      rank: QUALITY_META.calculating.rank,
    };
  }

  if (!connectivitySummary) {
    const fallbackCode = text(fallbackQuality.code, "green");
    return {
      code: fallbackCode,
      label: (QUALITY_META[fallbackCode] || QUALITY_META.green).label,
      reason: sanitizeUserText(fallbackQuality.reason) || qualityReasonByCode(fallbackCode),
      rank: Number(fallbackQuality.rank ?? (QUALITY_META[fallbackCode] || QUALITY_META.green).rank),
    };
  }

  const attentionThresholdRaw = Number(
    summary.attention_disconnected_pct_threshold ?? settings.attention_disconnected_pct_threshold ?? 20
  );
  const attentionThreshold = Number.isFinite(attentionThresholdRaw)
    ? Math.max(0, Math.min(100, attentionThresholdRaw))
    : 20;
  const criticalThresholdRaw = Number(
    summary.critical_disconnected_pct_threshold ?? settings.critical_disconnected_pct_threshold ?? 50
  );
  const criticalThreshold = Number.isFinite(criticalThresholdRaw)
    ? Math.max(attentionThreshold, Math.min(100, criticalThresholdRaw))
    : 50;

  const disconnectedPct = Math.max(0, Math.min(100, Math.round(Number(connectivitySummary.disconnectedPct || 0))));
  const criticalByDisconnectedPct = disconnectedPct > criticalThreshold;
  const attentionByDisconnectedPct = disconnectedPct > attentionThreshold;
  const attentionByOnlyAuxTopics =
    !connectivitySummary.hasPrincipalPayloadInWindow && connectivitySummary.hasAuxPayloadInWindow;

  let qualityCode = "green";
  let qualityReason = "Conectividade dentro da faixa esperada no período selecionado.";

  if (criticalByDisconnectedPct) {
    qualityCode = "critical";
    qualityReason =
      "Percentual desconectado no período " +
      `(${disconnectedPct}%) acima do limite crítico (${criticalThreshold.toFixed(1)}%).`;
  } else if (attentionByDisconnectedPct) {
    qualityCode = "yellow";
    qualityReason =
      "Percentual desconectado no período " +
      `(${disconnectedPct}%) acima do limite de instabilidade (${attentionThreshold.toFixed(1)}%).`;
  } else if (attentionByOnlyAuxTopics) {
    qualityCode = "yellow";
    qualityReason = "Sinais parciais de conectividade detectados no período selecionado.";
  }

  return {
    code: qualityCode,
    label: (QUALITY_META[qualityCode] || QUALITY_META.green).label,
    reason: qualityReason,
    rank: (QUALITY_META[qualityCode] || QUALITY_META.green).rank,
  };
}

function hideConnectivitySegmentCard() {
  ui.connSegmentCard.hidden = true;
  ui.connSegmentCard.innerHTML = "";
}

function clearConnectivitySegmentSelection() {
  state.connSelectedSegmentKey = null;
  for (const item of ui.connTrack.querySelectorAll(".conn-segment.selected")) {
    item.classList.remove("selected");
  }
  hideConnectivitySegmentCard();
}

function positionConnectivitySegmentCard(segmentEl) {
  const wrapEl = ui.connTrack.parentElement;
  if (!wrapEl || !segmentEl || ui.connSegmentCard.hidden) return;

  const segmentRect = segmentEl.getBoundingClientRect();
  const wrapRect = wrapEl.getBoundingClientRect();
  const centerX = segmentRect.left - wrapRect.left + segmentRect.width / 2;
  const topY = segmentRect.top - wrapRect.top;

  ui.connSegmentCard.style.top = `${Math.max(0, topY)}px`;
  ui.connSegmentCard.style.left = `${centerX}px`;

  const cardWidth = ui.connSegmentCard.offsetWidth || 0;
  const halfWidth = cardWidth / 2;
  const minCenter = halfWidth + 6;
  const maxCenter = Math.max(minCenter, wrapRect.width - halfWidth - 6);
  const clampedCenter = Math.min(maxCenter, Math.max(minCenter, centerX));
  ui.connSegmentCard.style.left = `${clampedCenter}px`;
}

function showConnectivitySegmentCard(segmentEl) {
  const segmentState = String(segmentEl.dataset.state || "disconnected");
  const startTs = Number(segmentEl.dataset.startTs || 0);
  const endTs = Number(segmentEl.dataset.endTs || 0);
  const durationSec = Number(segmentEl.dataset.durationSec || 0);
  const stateLabel = segmentState === "connected" ? "Conectado" : "Desconectado";

  ui.connSegmentCard.innerHTML = `
    <div class="conn-segment-card-title ${escapeHtml(segmentState)}">${escapeHtml(stateLabel)}</div>
    <div class="conn-segment-card-row"><span>Início</span><strong>${escapeHtml(formatShortDateTime(startTs))}</strong></div>
    <div class="conn-segment-card-row"><span>Fim</span><strong>${escapeHtml(formatShortDateTime(endTs))}</strong></div>
    <div class="conn-segment-card-row"><span>Duração</span><strong>${escapeHtml(fmtDuration(durationSec))}</strong></div>
  `;
  ui.connSegmentCard.hidden = false;
  positionConnectivitySegmentCard(segmentEl);
}

function restoreConnectivitySegmentSelection() {
  if (!state.connSelectedSegmentKey) {
    hideConnectivitySegmentCard();
    return;
  }

  const selectedEl = [...ui.connTrack.querySelectorAll(".conn-segment")].find(
    (item) => (item.dataset.key || "") === state.connSelectedSegmentKey
  );
  if (!selectedEl) {
    state.connSelectedSegmentKey = null;
    hideConnectivitySegmentCard();
    return;
  }

  selectedEl.classList.add("selected");
  showConnectivitySegmentCard(selectedEl);
}

function renderConnectivityTimeline(pivot) {
  const view = computeConnectivityView(pivot);
  const range = view.range;
  const startTs = view.startTs;
  const endTs = view.endTs;
  const duration = view.durationSec;
  const connData = view.connData;
  const segments = view.segments;
  const connectivitySummary = view.connectivityQualityInput;

  ui.connTrack.innerHTML = segments
    .map((segment) => {
      const leftPct = ((segment.start - startTs) / duration) * 100;
      const widthPct = Math.max(0.15, (segment.duration / duration) * 100);
      const segmentKey = `${segment.state}:${segment.start}:${segment.end}`;
      const selectedClass = state.connSelectedSegmentKey === segmentKey ? " selected" : "";
      return `<div class="conn-segment ${segment.state}${selectedClass}" style="left:${leftPct}%;width:${widthPct}%;" data-key="${escapeHtml(
        segmentKey
      )}" data-state="${escapeHtml(segment.state)}" data-start-ts="${segment.start}" data-end-ts="${segment.end}" data-duration-sec="${segment.duration}"></div>`;
    })
    .join("");

  ui.connSummary.innerHTML = `
    <span class="conn-pill connected">Conectado: <strong>${fmtDuration(connectivitySummary.connectedSec)}</strong> (${connectivitySummary.connectedPct}%)</span>
    <span class="conn-pill disconnected">Desconectado: <strong>${fmtDuration(connectivitySummary.disconnectedSec)}</strong> (${connectivitySummary.disconnectedPct}%)</span>
    <span class="conn-pill">Limite para desconexão: <strong>${fmtDuration(connData.disconnectThresholdSec)}</strong></span>
  `;
  ui.connStartLabel.textContent = formatShortDateTime(startTs);
  ui.connEndLabel.textContent = formatShortDateTime(endTs);

  if (!state.connCustomFrom) {
    state.connCustomFrom = toDateTimeLocal(Math.max(range.minTs, endTs - 24 * 3600));
  }
  if (!state.connCustomTo) {
    state.connCustomTo = toDateTimeLocal(endTs);
  }
  ui.connFrom.value = state.connCustomFrom;
  ui.connTo.value = state.connCustomTo;
  restoreConnectivitySegmentSelection();
  return view;
}

function renderProbeDelayChart(pivot) {
  const summary = pivot.summary || {};
  const probe = summary.probe || {};
  const enabled = !!probe.enabled;

  const range = normalizeProbeDelayRange(pivot);
  const startTs = range.startTs;
  const endTs = range.endTs;

  if (!state.probeDelayCustomFrom) {
    state.probeDelayCustomFrom = toDateTimeLocal(Math.max(range.minTs, endTs - 24 * 3600));
  }
  if (!state.probeDelayCustomTo) {
    state.probeDelayCustomTo = toDateTimeLocal(endTs);
  }

  ui.probeDelayPreset.value = state.probeDelayPreset;
  ui.probeDelayFrom.value = state.probeDelayCustomFrom;
  ui.probeDelayTo.value = state.probeDelayCustomTo;
  const custom = state.probeDelayPreset === "custom";
  ui.probeDelayRange.hidden = !custom;
  ui.probeDelayFromWrap.hidden = !custom;
  ui.probeDelayToWrap.hidden = !custom;

  ui.probeDelayStartLabel.textContent = formatShortDateTime(startTs);
  ui.probeDelayEndLabel.textContent = formatShortDateTime(endTs);

  if (!enabled) {
    ui.probeDelayHint.textContent = "Monitoramento de latência desativado para este pivô.";
    ui.probeDelayChart.innerHTML = `<div class="empty">Ative o monitoramento de latência para acompanhar os dados no período.</div>`;
    return;
  }

  const series = buildProbeDelaySeries(pivot, startTs, endTs);
  const points = series.points;
  if (!points.length) {
    ui.probeDelayHint.textContent = "Nenhum dado de latência no período selecionado.";
    ui.probeDelayChart.innerHTML = `<div class="empty">Nenhum dado disponível para este período.</div>`;
    return;
  }

  const width = 980;
  const height = 250;
  const padLeft = 52;
  const padRight = 14;
  const padTop = 16;
  const padBottom = 30;
  const innerWidth = width - padLeft - padRight;
  const innerHeight = height - padTop - padBottom;
  const duration = Math.max(1, endTs - startTs);

  const avgValues = points.map((item) => item.avgLatencySec);
  let minY = Math.min(...avgValues);
  let maxY = Math.max(...avgValues);
  let spread = maxY - minY;
  if (!Number.isFinite(spread) || spread <= 0) {
    spread = Math.max(0.05, maxY * 0.1);
  }
  const domainMin = Math.max(0, minY - spread * 0.2);
  let domainMax = maxY + spread * 0.2;
  if (!(domainMax > domainMin)) domainMax = domainMin + 0.1;
  const domainSpan = domainMax - domainMin;

  const xForTs = (ts) => padLeft + ((ts - startTs) / duration) * innerWidth;
  const yForDelay = (delaySec) => padTop + ((domainMax - delaySec) / domainSpan) * innerHeight;

  const pathData = points
    .map((point, idx) => `${idx === 0 ? "M" : "L"}${xForTs(point.ts).toFixed(2)} ${yForDelay(point.avgLatencySec).toFixed(2)}`)
    .join(" ");

  const yTickCount = 4;
  const yTicks = [];
  for (let idx = 0; idx <= yTickCount; idx += 1) {
    const ratio = idx / yTickCount;
    const value = domainMax - ratio * domainSpan;
    const y = padTop + ratio * innerHeight;
    yTicks.push({ value, y });
  }
  const lastPoint = points[points.length - 1];

  ui.probeDelayChart.innerHTML = `
    <svg class="probe-delay-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Gráfico de latência média ao longo do tempo">
      ${yTicks
        .map(
          (tick) => `
            <line class="probe-delay-grid" x1="${padLeft}" y1="${tick.y.toFixed(2)}" x2="${(padLeft + innerWidth).toFixed(
              2
            )}" y2="${tick.y.toFixed(2)}"></line>
            <text class="probe-delay-ylabel" x="${(padLeft - 8).toFixed(2)}" y="${(tick.y + 4).toFixed(2)}">${escapeHtml(
            fmtSecondsPrecise(tick.value)
          )}</text>
          `
        )
        .join("")}
      <path class="probe-delay-line" d="${pathData}"></path>
    </svg>
  `;

  ui.probeDelayHint.textContent =
    `Registros no período: ${series.responseCount} | ` +
    `Latência média final: ${fmtSecondsPrecise(lastPoint.avgLatencySec)} | ` +
    `Última medição: ${fmtSecondsPrecise(lastPoint.latencySec)} (${formatShortDateTime(lastPoint.ts)})`;
}

function renderRssiChart(pivot) {
  if (!ui.pivotRssiPanel) return;

  const hasRssi = shouldRenderRssiPanel(pivot);
  ui.pivotRssiPanel.hidden = !hasRssi;
  if (!hasRssi) return;

  const range = normalizeRssiRange(pivot);
  const startTs = range.startTs;
  const endTs = range.endTs;

  if (!state.rssiCustomFrom) {
    state.rssiCustomFrom = toDateTimeLocal(Math.max(range.minTs, endTs - 24 * 3600));
  }
  if (!state.rssiCustomTo) {
    state.rssiCustomTo = toDateTimeLocal(endTs);
  }

  ui.rssiPreset.value = state.rssiPreset;
  ui.rssiFrom.value = state.rssiCustomFrom;
  ui.rssiTo.value = state.rssiCustomTo;
  const custom = state.rssiPreset === "custom";
  ui.rssiRange.hidden = !custom;
  ui.rssiFromWrap.hidden = !custom;
  ui.rssiToWrap.hidden = !custom;

  ui.rssiStartLabel.textContent = formatShortDateTime(startTs);
  ui.rssiEndLabel.textContent = formatShortDateTime(endTs);

  const series = buildRssiSeries(pivot, startTs, endTs);
  const points = series.points;
  if (!points.length) {
    ui.rssiHint.textContent = "Nenhum dado de RSSI no periodo selecionado.";
    ui.rssiChart.innerHTML = `<div class="empty">Nenhum dado disponivel para este periodo.</div>`;
    return;
  }

  const width = 980;
  const height = 250;
  const padLeft = 52;
  const padRight = 14;
  const padTop = 16;
  const padBottom = 30;
  const innerWidth = width - padLeft - padRight;
  const innerHeight = height - padTop - padBottom;
  const chartDomain = resolveRssiChartDomain(points, startTs, endTs);
  const chartStartTs = chartDomain.startTs;
  const chartEndTs = chartDomain.endTs;
  const duration = Math.max(1, chartEndTs - chartStartTs);

  const domainMax = 31;
  const domainSpan = domainMax;

  const xForTs = (ts) => padLeft + ((ts - chartStartTs) / duration) * innerWidth;
  const yForRssi = (rssi) => padTop + ((domainMax - rssi) / domainSpan) * innerHeight;

  const pathData = points
    .map((point, idx) => `${idx === 0 ? "M" : "L"}${xForTs(point.ts).toFixed(2)} ${yForRssi(point.rssi).toFixed(2)}`)
    .join(" ");

  const yTicks = [31, 24, 16, 8, 0].map((value) => ({
    value,
    y: yForRssi(value),
  }));
  const lastPoint = points[points.length - 1];
  const averageRssi = points.reduce((sum, point) => sum + point.rssi, 0) / points.length;

  ui.rssiChart.innerHTML = `
    <svg class="probe-delay-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Grafico de RSSI ao longo do tempo">
      ${yTicks
        .map(
          (tick) => `
            <line class="probe-delay-grid" x1="${padLeft}" y1="${tick.y.toFixed(2)}" x2="${(padLeft + innerWidth).toFixed(
              2
            )}" y2="${tick.y.toFixed(2)}"></line>
            <text class="probe-delay-ylabel" x="${(padLeft - 8).toFixed(2)}" y="${(tick.y + 4).toFixed(2)}">${escapeHtml(
            String(tick.value)
          )}</text>
          `
        )
        .join("")}
      <path class="probe-delay-line" d="${pathData}"></path>
    </svg>
  `;

  ui.rssiStartLabel.textContent = formatShortDateTime(chartStartTs);
  ui.rssiEndLabel.textContent = formatShortDateTime(chartEndTs);

  ui.rssiHint.textContent =
    `Registros no periodo: ${series.sampleCount} | ` +
    `RSSI medio: ${averageRssi.toFixed(2)} | ` +
    `Ultima medicao: ${lastPoint.rssi.toFixed(0)} (${formatShortDateTime(lastPoint.ts)})`;
}

function getTimelinePageForFilter(filterKey) {
  const key = normalizeConnectivityEventTopicFilter(filterKey);
  const page = Number((state.timelinePageByTopic || {})[key]);
  if (Number.isFinite(page) && page >= 1) return Math.floor(page);
  return key === "global" ? Math.max(1, Math.floor(Number(state.timelinePage) || 1)) : 1;
}

function setTimelinePageForFilter(filterKey, page) {
  const key = normalizeConnectivityEventTopicFilter(filterKey);
  const safePage = Math.max(1, Math.floor(Number(page) || 1));
  state.timelinePageByTopic = state.timelinePageByTopic || {};
  state.timelinePageByTopic[key] = safePage;
  if (key === "global") state.timelinePage = safePage;
}

function renderTimeline(pivot) {
  const activeFilter = normalizeConnectivityEventTopicFilter(state.timelineTopicFilter);
  state.timelineTopicFilter = activeFilter;
  if (ui.timelineTopicSelect && ui.timelineTopicSelect.value !== activeFilter) {
    ui.timelineTopicSelect.value = activeFilter;
  }

  const allEvents = getConnectivityEventsPanelFiltered(pivot, activeFilter);
  const totalPages = Math.max(1, Math.ceil(allEvents.length / state.timelinePageSize));
  const activePage = Math.min(getTimelinePageForFilter(activeFilter), totalPages);
  setTimelinePageForFilter(activeFilter, activePage);
  const start = (activePage - 1) * state.timelinePageSize;
  const pageEvents = allEvents.slice(start, start + state.timelinePageSize);
  if (ui.timelineDownloadTxt) {
    ui.timelineDownloadTxt.disabled = allEvents.length <= 0;
    ui.timelineDownloadTxt.title = allEvents.length
      ? "Baixar os eventos das 5 paginas deste topico"
      : "Nenhum evento para baixar";
  }

  if (!pageEvents.length) {
    ui.timelineList.innerHTML = `<div class="empty">Nenhum evento disponível para este período.</div>`;
  } else {
    ui.timelineList.innerHTML = pageEvents
      .map((event) => {
        const type = text(event.type, "event");
        const title = buildConnectivityEventTitle(event);
        const sourceTopic = resolveConnectivityEventSourceTopic(event);
        const rawPayload = resolveConnectivityEventRawPayload(event);
        const detailsText = buildConnectivityEventDetailsText(event);
        const payloadText = rawPayload || "Payload nao disponivel para este evento.";
        return `
          <article class="event ${escapeHtml(type)}">
            <div class="event-head">
              <div class="event-title">${escapeHtml(title)}</div>
              <div class="event-time">${escapeHtml(formatTimestampFromTsOrValue(event.ts, event.at, true))}</div>
            </div>
            <div class="event-topic">Origem: ${escapeHtml(sourceTopic)}</div>
            <div>${escapeHtml(payloadText)}</div>
            <details class="event-details">
              <summary>Informações adicionais</summary>
              <pre>${escapeHtml(detailsText)}</pre>
            </details>
          </article>
        `;
      })
      .join("");
  }

  ui.timelinePageInfo.textContent = `Página ${activePage}/${totalPages}`;
  ui.timelinePrev.disabled = activePage <= 1;
  ui.timelineNext.disabled = activePage >= totalPages;
}

function renderCloud2Table(pivot) {
  const rows = (pivot.cloud2_events || []).slice(0, 20);
  if (!rows.length) {
    ui.cloud2Table.innerHTML = `<tr><td colspan="5">Nenhum dado disponível para este período.</td></tr>`;
    return;
  }

  ui.cloud2Table.innerHTML = rows
    .map((row) => {
      return `
      <tr>
        <td>${escapeHtml(formatTimestampFromTsOrValue(row.ts, row.at, true))}</td>
        <td>${escapeHtml(text(row.rssi))}</td>
        <td>${escapeHtml(text(row.technology))}</td>
        <td>${escapeHtml(fmtDuration(row.drop_duration_sec))}</td>
        <td>${escapeHtml(text(row.firmware))}</td>
      </tr>
      `;
    })
    .join("");
}

function renderPivotView() {
  const pivot = state.pivotData;
  if (!pivot || !state.selectedPivot) {
    ui.pivotView.hidden = true;
    syncPivotDeleteControl();
    syncPivotTechnologyControl();
    clearConnectivitySegmentSelection();
    renderPivotConfigModal();
    return;
  }

  ui.pivotView.hidden = false;
  syncPivotDeleteControl();
  syncPivotTechnologyControl();
  const summary = pivot.summary || {};
  const probe = summary.probe || {};
  const connectivityView = renderConnectivityTimeline(pivot);
  const connectivitySummary = connectivityView.connectivityQualityInput;
  const quality = buildQualityFromConnectivity(pivot, connectivitySummary);
  const displayStatus = connectivityView.status;
  const pivotId = text(pivot.pivot_id, "").trim();
  if (pivotId) {
    state.qualityOverridesByPivotId[pivotId] = quality;
    state.statusOverridesByPivotId[pivotId] = {
      code: text(displayStatus.code, "gray"),
      label: text(displayStatus.label, "Inicial"),
      reason: text(displayStatus.reason, ""),
      rank: Number(displayStatus.rank ?? 99),
    };
    state.connectivitySummaryByPivotId[pivotId] = {
      connectedPct: Math.max(0, Math.min(100, Math.round(Number(connectivitySummary.connectedPct || 0)))),
      disconnectedPct: Math.max(0, Math.min(100, Math.round(Number(connectivitySummary.disconnectedPct || 0)))),
    };
    state.connectivityMiniSegmentsByPivotId[pivotId] = buildTimelineMiniSegmentsFromConnectivity(
      connectivityView.segments,
      connectivityView.durationSec
    );
  }

  ui.pivotTitle.textContent = text(pivot.pivot_id, "Pivô");
  ui.pivotStatus.textContent = `Status: ${text(displayStatus.label, "Inicial")}`;
  ui.pivotStatus.className = `badge ${text(displayStatus.code, "gray")}`;
  if (ui.pivotQuality) {
    ui.pivotQuality.textContent = `Conectividade: ${text(quality.label, "Estável")}`;
    ui.pivotQuality.className = `badge ${text(quality.code, "green")}`;
  }

  ui.probeEnabled.checked = !!probe.enabled;
  ui.probeInterval.value = Number(probe.interval_sec || 0);
  const sentCount = Number(probe.sent_count || 0);
  const responseCount = Number(probe.response_count || 0);
  const responseCoverageText =
    sentCount > 0
      ? `${responseCount}/${sentCount} (${fmtPercent(probe.response_ratio_pct)})`
      : `${responseCount}/${sentCount}`;
  ui.probeStatLastSent.textContent = formatTimestampFromTsOrValue(probe.last_sent_ts, probe.last_sent_at);
  ui.probeStatLastResponse.textContent = formatTimestampFromTsOrValue(probe.last_response_ts, probe.last_response_at);
  ui.probeStatTimeoutStreak.textContent = text(probe.timeout_streak, "0");
  ui.probeStatResponseRatio.textContent = responseCoverageText;
  ui.probeStatDelayLast.textContent = fmtSecondsPrecise(probe.latency_last_sec);
  ui.probeStatDelayAvg.textContent = fmtSecondsPrecise(probe.latency_avg_sec);
  renderProbeResponseInfoRows(probe);
  ui.probeHint.textContent = "";
  if (ui.pivotConcentratorToggle) {
    ui.pivotConcentratorToggle.checked = isPivotConcentrator(pivot);
  }
  const coordinates = extractPivotCoordinates(pivot);
  const coordinatesText = formatPivotCoordinates(coordinates.latitude, coordinates.longitude);
  if (ui.pivotLocationInput) {
    ui.pivotLocationInput.value = coordinatesText;
  }
  if (ui.pivotLocationHint) {
    ui.pivotLocationHint.textContent = coordinatesText ? "" : "Localização não configurada.";
  }
  if (ui.pivotTechnologyHint) {
    ui.pivotTechnologyHint.textContent = "";
  }
  syncPivotTechnologyControl();
  checkPendingModemResetAcks();

  renderStatusSummary();
  renderCards();
  renderPivotMetrics(pivot, displayStatus, quality, connectivitySummary);
  renderProbeDelayChart(pivot);
  renderRssiChart(pivot);
  renderTimeline(pivot);
  renderCloud2Table(pivot);
  renderPivotConfigModal();
}

async function loadUiConfig() {
  try {
    const config = await getJson("data/ui_config.json");
    const refreshSec = Number(config.refresh_sec || 5);
    if (Number.isFinite(refreshSec) && refreshSec >= 1) {
      state.refreshMs = refreshSec * 1000;
    }
  } catch (err) {
    state.refreshMs = 5000;
  }
}

async function refreshSummaryCardsHistory(options = {}) {
  const skipRender = !!options.skipRender;
  const forceRefresh = !!options.force;
  const nowMs = Date.now();
  const refreshIntervalMs = Math.max(60000, Number(state.summaryCardsHistoryRefreshIntervalMs || 0));

  if (!forceRefresh && state.summaryCardsHistoryFetchPromise) {
    return state.summaryCardsHistoryFetchPromise;
  }
  if (
    !forceRefresh
    && state.summaryCardsHistoryLoadedAtMs > 0
    && (nowMs - Number(state.summaryCardsHistoryLoadedAtMs || 0) < refreshIntervalMs)
  ) {
    return state.summaryCardsHistoryPayload;
  }

  state.summaryCardsHistoryLoading = true;
  state.summaryCardsHistoryError = "";
  if (!skipRender) renderStatusSummary();

  const promise = getJson(buildSummaryCardsHistoryUrl(), {
    allowStale: true,
    timeoutMs: 18000,
    retryDelaysMs: [0, 1200],
    staleTtlMs: 30 * 60 * 1000,
  })
    .then((payload) => {
      state.summaryCardsHistoryPayload = payload;
      state.summaryCardsHistoryLoadedAtMs = Date.now();
      state.summaryCardsHistoryError = "";
      return payload;
    })
    .catch((err) => {
      if (!state.summaryCardsHistoryPayload) {
        state.summaryCardsHistoryError = "Nao foi possivel carregar o histórico agora.";
      }
      throw err;
    })
    .finally(() => {
      if (state.summaryCardsHistoryFetchPromise === promise) {
        state.summaryCardsHistoryFetchPromise = null;
      }
      state.summaryCardsHistoryLoading = false;
      if (!skipRender) renderStatusSummary();
    });

  state.summaryCardsHistoryFetchPromise = promise;
  return promise;
}

async function refreshState(options = {}) {
  const skipRender = !!options.skipRender;
  const requestedRunId = normalizeRunId(state.selectedRunId) || null;
  const data = await getJson(buildStateUrl(requestedRunId), {
    allowStale: true,
    timeoutMs: 25000,
    retryDelaysMs: [0, 1500],
    staleTtlMs: 180000,
  });
  state.rawState = data;
  state.pivots = data.pivots || [];
  syncCloud2FilterOptions(data);
  state.panelRunMeta = data.run || null;
  let selectedRunChanged = false;
  const payloadRunId = normalizeRunId(data?.run_id);
  if (!requestedRunId && payloadRunId) {
    state.selectedRunId = payloadRunId;
    selectedRunChanged = true;
  }
  const currentPivotIds = new Set(state.pivots.map((item) => text(item.pivot_id, "").trim()).filter(Boolean));
  setSelectedPivotIds(getSelectedPivotIds().filter((pivotId) => currentPivotIds.has(pivotId)));
  for (const pivotId of Object.keys(state.qualityOverridesByPivotId)) {
    if (!currentPivotIds.has(pivotId)) {
      delete state.qualityOverridesByPivotId[pivotId];
    }
  }
  for (const pivotId of Object.keys(state.statusOverridesByPivotId)) {
    if (!currentPivotIds.has(pivotId)) {
      delete state.statusOverridesByPivotId[pivotId];
    }
  }
  for (const pivotId of Object.keys(state.qualitySourceSignatureByPivotId)) {
    if (!currentPivotIds.has(pivotId)) {
      delete state.qualitySourceSignatureByPivotId[pivotId];
    }
  }
  for (const pivotId of Object.keys(state.connectivitySummaryByPivotId)) {
    if (!currentPivotIds.has(pivotId)) {
      delete state.connectivitySummaryByPivotId[pivotId];
    }
  }
  for (const pivotId of Object.keys(state.connectivityMiniSegmentsByPivotId)) {
    if (!currentPivotIds.has(pivotId)) {
      delete state.connectivityMiniSegmentsByPivotId[pivotId];
    }
  }
  state.qualityOverridesReady = hasCompleteQualityOverridesForPivots(state.pivots);
  if (Array.isArray(state.visiblePivotIds) && state.visiblePivotIds.length) {
    state.visiblePivotIds = state.visiblePivotIds.filter((pivotId) => currentPivotIds.has(String(pivotId || "").trim()));
  }
  for (const pivotId of Object.keys(state.pendingModemResetAcks || {})) {
    if (!currentPivotIds.has(pivotId) && String(state.selectedPivot || "").trim() !== pivotId) {
      delete state.pendingModemResetAcks[pivotId];
    }
  }
  if (state.selectedPivot && !currentPivotIds.has(state.selectedPivot)) {
    if (skipRender) {
      state.connSelectedSegmentKey = null;
      state.selectedPivot = null;
      state.pivotData = null;
      state.panelSessionMeta = null;
      state.panelRunMeta = data.run || null;
      setHashPivot("");
    } else {
      closePivot();
    }
  }
  checkPendingModemResetAcks();
  if (!skipRender) {
    renderHeader();
    renderStatusSummary();
    renderPending();
    renderCards();
  }
  return { selectedRunChanged };
}

async function refreshPivot(options = {}) {
  const skipRender = !!options.skipRender;
  if (!state.selectedPivot) {
    state.pivotData = null;
    state.panelSessionMeta = null;
    state.panelRunMeta = state.rawState?.run || null;
    if (!skipRender) {
      renderSessionControls();
      renderPivotView();
    }
    return;
  }

  const pivotId = String(state.selectedPivot || "").trim();
  const selectedRunId = text(state.selectedRunId, "").trim() || null;
  const previousPivotData = state.pivotData;

  try {
    state.pivotData = await getJson(buildPivotPanelUrl(pivotId, selectedRunId), {
      allowStale: true,
      timeoutMs: 30000,
      retryDelaysMs: [0, 1500],
      staleTtlMs: 180000,
    });
  } catch (err) {
    state.pivotData = previousPivotData;
  }

  if (state.pivotData) {
    state.panelSessionMeta = state.pivotData.session || null;
    state.panelRunMeta = state.pivotData.run || state.rawState?.run || null;
  } else {
    state.panelSessionMeta = null;
    state.panelRunMeta = state.rawState?.run || null;
  }
  checkPendingModemResetAcks();
  if (!skipRender) {
    renderSessionControls();
    renderPivotView();
  }
}

async function refreshQualityOverrides(options = {}) {
  const skipRender = !!options.skipRender;
  const forceRefresh = !!options.force;
  const visibleOnly = !!options.visibleOnly;
  const pivots = state.pivots || [];
  if (!pivots.length) {
    state.qualityOverridesByPivotId = {};
    state.statusOverridesByPivotId = {};
    state.qualitySourceSignatureByPivotId = {};
    state.connectivitySummaryByPivotId = {};
    state.connectivityMiniSegmentsByPivotId = {};
    state.qualityOverridesReady = true;
    state.qualityOverridesLastRefreshMs = Date.now();
    return;
  }

  const nowMs = Date.now();
  const refreshIntervalMs = Math.max(30000, Number(state.qualityOverrideRefreshIntervalMs || 45000));
  const seq = Number(state.qualityRefreshSeq || 0) + 1;
  state.qualityRefreshSeq = seq;

  const pivotMetaById = new Map();
  for (const pivot of pivots) {
    const pivotId = text((pivot || {}).pivot_id, "").trim();
    if (!pivotId) continue;
    const signature = buildQualitySourceSignature(pivot);
    const hasQuality = Object.prototype.hasOwnProperty.call(state.qualityOverridesByPivotId, pivotId);
    const hasStatus = Object.prototype.hasOwnProperty.call(state.statusOverridesByPivotId, pivotId);
    const hasConnectivity = Object.prototype.hasOwnProperty.call(state.connectivitySummaryByPivotId, pivotId);
    const hasMiniTimeline = Object.prototype.hasOwnProperty.call(state.connectivityMiniSegmentsByPivotId, pivotId);
    const previousSignature = state.qualitySourceSignatureByPivotId[pivotId];
    pivotMetaById.set(pivotId, {
      signature,
      changed: signature !== previousSignature,
      missing: !(hasQuality && hasStatus && hasConnectivity && hasMiniTimeline),
    });
  }

  if (!pivotMetaById.size) {
    state.qualityOverridesReady = hasCompleteQualityOverridesForPivots(pivots);
    return;
  }

  const duePeriodicRefresh =
    forceRefresh || (nowMs - Number(state.qualityOverridesLastRefreshMs || 0) >= refreshIntervalMs);
  const visiblePivotIds = resolveVisiblePivotIdsForRefresh();
  const selectedPivotId = text(state.selectedPivot, "").trim();

  const prioritySet = new Set();
  const priorityOrderedIds = [];
  const pushPriority = (pivotId) => {
    const id = String(pivotId || "").trim();
    if (!id || prioritySet.has(id) || !pivotMetaById.has(id)) return;
    prioritySet.add(id);
    priorityOrderedIds.push(id);
  };
  pushPriority(selectedPivotId);
  for (const pivotId of visiblePivotIds) pushPriority(pivotId);

  const allPivotIds = visibleOnly ? [] : [...pivotMetaById.keys()];
  const orderedPivotIds = [
    ...priorityOrderedIds,
    ...allPivotIds.filter((pivotId) => !prioritySet.has(pivotId)),
  ];

  const targetPivotIds = [];
  for (const pivotId of orderedPivotIds) {
    const meta = pivotMetaById.get(pivotId);
    if (!meta) continue;
    const isPriority = prioritySet.has(pivotId);
    const shouldFetch = forceRefresh
      || meta.missing
      || (isPriority && meta.changed)
      || (duePeriodicRefresh && meta.changed);
    if (shouldFetch) targetPivotIds.push(pivotId);
  }

  if (!targetPivotIds.length) {
    if (duePeriodicRefresh) {
      state.qualityOverridesLastRefreshMs = nowMs;
    }
    state.qualityOverridesReady = hasCompleteQualityOverridesForPivots(pivots);
    return;
  }

  const nextOverrides = { ...state.qualityOverridesByPivotId };
  const nextStatusOverrides = { ...state.statusOverridesByPivotId };
  const nextSignatures = { ...state.qualitySourceSignatureByPivotId };
  const nextConnectivitySummaryByPivotId = { ...state.connectivitySummaryByPivotId };
  const nextConnectivityMiniSegmentsByPivotId = { ...state.connectivityMiniSegmentsByPivotId };
  let qualityPayload = null;
  try {
    qualityPayload = await getJson(buildQualityLiteUrl(state.selectedRunId, targetPivotIds), {
      allowStale: true,
      timeoutMs: 25000,
      retryDelaysMs: [0, 1500],
      staleTtlMs: 180000,
    });
  } catch (err) {
    return;
  }

  const qualityPivots = Array.isArray(qualityPayload?.pivots) ? qualityPayload.pivots : [];
  const qualityPivotById = new Map();
  for (const pivot of qualityPivots) {
    const pivotId = text((pivot || {}).pivot_id, "").trim();
    if (!pivotId) continue;
    qualityPivotById.set(pivotId, pivot);
  }

  for (const pivotId of targetPivotIds) {
    const pivotData = qualityPivotById.get(pivotId);
    if (!pivotData) continue;

    const view = computeConnectivityView(pivotData);
    const connectivitySummary = view.connectivityQualityInput || {};
    const quality = buildQualityFromConnectivity(pivotData, view.connectivityQualityInput);
    nextOverrides[pivotId] = quality;
    const detailStatus = view.status || {};
    nextStatusOverrides[pivotId] = {
      code: text(detailStatus.code, "gray"),
      label: text(detailStatus.label, "Inicial"),
      reason: text(detailStatus.reason, ""),
      rank: Number(detailStatus.rank ?? 99),
    };
    nextConnectivitySummaryByPivotId[pivotId] = {
      connectedPct: Math.max(0, Math.min(100, Math.round(Number(connectivitySummary.connectedPct || 0)))),
      disconnectedPct: Math.max(0, Math.min(100, Math.round(Number(connectivitySummary.disconnectedPct || 0)))),
    };
    nextConnectivityMiniSegmentsByPivotId[pivotId] = buildTimelineMiniSegmentsFromConnectivity(
      view.segments,
      view.durationSec
    );
    const meta = pivotMetaById.get(pivotId);
    if (meta) {
      nextSignatures[pivotId] = meta.signature;
    }
  }

  if (seq !== state.qualityRefreshSeq) return;

  state.qualityOverridesByPivotId = nextOverrides;
  state.statusOverridesByPivotId = nextStatusOverrides;
  state.qualitySourceSignatureByPivotId = nextSignatures;
  state.connectivitySummaryByPivotId = nextConnectivitySummaryByPivotId;
  state.connectivityMiniSegmentsByPivotId = nextConnectivityMiniSegmentsByPivotId;
  state.qualityOverridesReady = hasCompleteQualityOverridesForPivots(pivots, {
    qualityOverridesByPivotId: nextOverrides,
    statusOverridesByPivotId: nextStatusOverrides,
    connectivitySummaryByPivotId: nextConnectivitySummaryByPivotId,
    connectivityMiniSegmentsByPivotId: nextConnectivityMiniSegmentsByPivotId,
  });
  state.qualityOverridesLastRefreshMs = nowMs;
  if (!skipRender) {
    renderStatusSummary();
    renderCards();
  }
}

function renderDashboardFromCurrentState() {
  renderHeader();
  renderStatusSummary();
  renderPending();
  renderCards();
  renderSessionControls();
  renderPivotView();
}

function refreshDashboardSupportData(options = {}) {
  const forceQuality = !!options.forceQuality;
  const skipRender = !!options.skipRender;
  if (state.auxRefreshPromise) return state.auxRefreshPromise;

  const promise = Promise.allSettled([
    refreshQualityOverrides({ skipRender: true, force: forceQuality }),
    refreshPivot({ skipRender: true }),
    refreshSummaryCardsHistory({ skipRender: true, force: forceQuality }),
  ])
    .then((results) => {
      if (!skipRender) {
        renderDashboardFromCurrentState();
      }
      return results;
    })
    .finally(() => {
      if (state.auxRefreshPromise === promise) {
        state.auxRefreshPromise = null;
      }
    });

  state.auxRefreshPromise = promise;
  return promise;
}

async function refreshInitialDashboardState() {
  const stateResult = await refreshState({ skipRender: true });
  if (stateResult?.selectedRunChanged) {
    await refreshState({ skipRender: true });
  }
  if (!state.pivots.length) {
    const resolved = await autoResolveRunIdFromBackend({ allowOverride: true });
    if (resolved) {
      await refreshState({ skipRender: true });
    }
  }
  const supportPromise = Promise.allSettled([
    refreshSummaryCardsHistory({ skipRender: true, force: true }),
    refreshPivot({ skipRender: true }),
  ]);
  await refreshQualityOverrides({ skipRender: true, force: true });
  renderDashboardFromCurrentState();
  void supportPromise.then(() => renderDashboardFromCurrentState());
}

async function refreshAll(options = {}) {
  const suppressInterimRender = !!options.suppressInterimRender;
  if (state.refreshInFlight) return;
  state.refreshInFlight = true;
  try {
    const stateResult = await refreshState({ skipRender: true });
    if (stateResult?.selectedRunChanged) {
      await refreshState({ skipRender: true });
    }
    if (!state.pivots.length) {
      const resolved = await autoResolveRunIdFromBackend({ allowOverride: true });
      if (resolved) {
        await refreshState({ skipRender: true });
      }
    }
    await refreshQualityOverrides({ skipRender: true, force: !state.qualityOverridesReady });
    if (!suppressInterimRender) {
      renderDashboardFromCurrentState();
    }
    void refreshDashboardSupportData({ skipRender: suppressInterimRender });
    state.lastRefreshToastAtMs = 0;
  } catch (err) {
    const hasRenderableData = Array.isArray(state.pivots) && state.pivots.length > 0;
    if (!hasRenderableData) {
      ui.cardsGrid.innerHTML = `<div class="empty">Nao foi possivel carregar os dados. Tente novamente.</div>`;
    }
    const now = Date.now();
    if (now - Number(state.lastRefreshToastAtMs || 0) > 15000) {
      showToast("Nao foi possivel carregar os dados. Tente novamente.", "error", 3800);
      state.lastRefreshToastAtMs = now;
    }
  } finally {
    state.refreshInFlight = false;
  }
}

async function openPivot(pivotId) {
  const id = String(pivotId || "").trim();
  if (!id) return;
  state.connSelectedSegmentKey = null;
  state.pivotMetricsExpanded = false;
  state.selectedPivot = id;
  state.panelSessionMeta = null;
  state.timelinePage = 1;
  state.timelineTopicFilter = "global";
  state.timelinePageByTopic = {};
  setHashPivot(id);
  await refreshPivot();
  ui.pivotView.scrollIntoView({ behavior: "smooth", block: "start" });
}

function closePivot() {
  state.connSelectedSegmentKey = null;
  state.pivotMetricsExpanded = false;
  state.selectedPivot = null;
  state.pivotData = null;
  state.panelSessionMeta = null;
  state.panelRunMeta = state.rawState?.run || null;
  setHashPivot("");
  renderSessionControls();
  renderPivotView();
}

function getSelectedPivotIds() {
  const normalized = normalizePivotIdList(state.selectedPivotIds);
  state.selectedPivotIds = normalized;
  return normalized;
}

function findPivotById(pivotId) {
  const normalizedPivotId = String(pivotId || "").trim();
  if (!normalizedPivotId) return null;
  return (state.pivots || []).find((pivot) => String((pivot || {}).pivot_id || "").trim() === normalizedPivotId) || null;
}

function isPivotSelected(pivotId) {
  const normalizedPivotId = String(pivotId || "").trim();
  if (!normalizedPivotId) return false;
  return getSelectedPivotIds().includes(normalizedPivotId);
}

function setSelectedPivotIds(pivotIds) {
  state.selectedPivotIds = normalizePivotIdList(pivotIds);
  syncBulkPivotActionControls();
}

function toggleSelectedPivotId(pivotId, shouldSelect) {
  if (state.bulkPivotActionInFlight) return;
  const normalizedPivotId = String(pivotId || "").trim();
  if (!normalizedPivotId) return;
  const current = new Set(getSelectedPivotIds());
  if (shouldSelect) {
    current.add(normalizedPivotId);
  } else {
    current.delete(normalizedPivotId);
  }
  setSelectedPivotIds([...current]);
  renderCards();
}

function toggleVisiblePivotSelection(shouldSelect) {
  if (state.bulkPivotActionInFlight) return;
  const current = new Set(getSelectedPivotIds());
  for (const rawPivotId of state.visiblePivotIds || []) {
    const pivotId = String(rawPivotId || "").trim();
    if (!pivotId) continue;
    if (shouldSelect) {
      current.add(pivotId);
    } else {
      current.delete(pivotId);
    }
  }
  setSelectedPivotIds([...current]);
  renderCards();
}

function syncBulkPivotActionControls() {
  const allowed = canCurrentUserDeletePivots();
  if (!allowed) {
    state.selectedPivotIds = [];
  }

  const selectedPivotIds = allowed ? getSelectedPivotIds() : [];
  const selectedCount = selectedPivotIds.length;
  const showBulkActions = allowed && selectedCount > 1;
  const disableActions = state.bulkPivotActionInFlight || selectedCount < 2;
  const pivotLabel = selectedCount === 1 ? "pivô" : "pivôs";

  if (ui.bulkPivotActions) {
    ui.bulkPivotActions.hidden = !showBulkActions;
  }
  if (ui.bulkPivotSelectionLabel) {
    ui.bulkPivotSelectionLabel.textContent = `${selectedCount} ${pivotLabel} selecionados`;
  }
  if (ui.bulkResetPivotsBtn) {
    ui.bulkResetPivotsBtn.disabled = disableActions;
  }
  if (ui.bulkDeletePivotsBtn) {
    ui.bulkDeletePivotsBtn.disabled = disableActions;
  }
}

function canCurrentUserDeletePivots() {
  if (state.pivotDeleteAllowed === true) return true;
  const role = String(state.authUserRole || "user").trim().toLowerCase();
  const email = String(state.authUserEmail || "").trim().toLowerCase();
  return role === "admin" && email === FIXED_PIVOT_DELETE_EMAIL;
}

function syncPivotDeleteControl() {
  if (!ui.deletePivotBtn) return;
  const allowed = canCurrentUserDeletePivots();
  ui.deletePivotBtn.hidden = !allowed;
  if (!allowed) {
    ui.deletePivotBtn.disabled = true;
  } else {
    ui.deletePivotBtn.disabled = false;
  }
}

function canCurrentUserManageExpectedPivots() {
  const role = String(state.authUserRole || "user").trim().toLowerCase();
  return role === "admin";
}

function toggleExpectedPivotPanel(forceExpanded = null) {
  const nextExpanded = typeof forceExpanded === "boolean"
    ? forceExpanded
    : !state.expectedPivotPanelExpanded;
  state.expectedPivotPanelExpanded = nextExpanded;
  renderExpectedPivotAdmin();
}

function renderExpectedPivotAdmin() {
  const allowed = canCurrentUserManageExpectedPivots();
  const pendingItems = allowed ? getExpectedPivotsPending() : [];
  const pendingCount = pendingItems.length;
  const toggleLabel = state.expectedPivotPanelExpanded
    ? `Ocultar fila (${pendingCount})`
    : `Ver fila (${pendingCount})`;

  if (ui.expectedPivotAdminDock) {
    ui.expectedPivotAdminDock.hidden = !allowed;
  }
  if (!allowed) {
    state.expectedPivotPanelExpanded = false;
  }
  if (ui.expectedPivotInput) {
    ui.expectedPivotInput.disabled = !allowed || state.expectedPivotActionInFlight;
  }
  if (ui.addExpectedPivotBtn) {
    ui.addExpectedPivotBtn.disabled = !allowed || state.expectedPivotActionInFlight;
  }
  if (ui.toggleExpectedPivotPanelBtn) {
    ui.toggleExpectedPivotPanelBtn.disabled = !allowed;
    ui.toggleExpectedPivotPanelBtn.textContent = toggleLabel;
    ui.toggleExpectedPivotPanelBtn.setAttribute("aria-expanded", state.expectedPivotPanelExpanded ? "true" : "false");
  }
  if (ui.expectedPivotPanel) {
    ui.expectedPivotPanel.hidden = !allowed || !state.expectedPivotPanelExpanded;
  }

  const signature = [
    allowed ? 1 : 0,
    state.expectedPivotPanelExpanded ? 1 : 0,
    state.expectedPivotActionInFlight ? 1 : 0,
    pendingItems.map((item) => `${item.pivot_id}:${item.added_at}`).join("|"),
  ].join("|");
  if (state.expectedPivotPanelSignature === signature) return;
  state.expectedPivotPanelSignature = signature;

  if (!ui.expectedPivotList || !allowed || !state.expectedPivotPanelExpanded) return;

  if (!pendingItems.length) {
    ui.expectedPivotList.innerHTML = `<div class="empty">Nenhum pivô aguardando descoberta na cloudv2.</div>`;
    return;
  }

  ui.expectedPivotList.innerHTML = pendingItems
    .map((item) => {
      const pivotId = escapeHtml(text(item.pivot_id, ""));
      const addedAt = escapeHtml(formatTimestampFromTsOrValue(item.added_at_ts, item.added_at));
      return `
        <div class="list-item expected-pivot-item">
          <div class="expected-pivot-item-main">
            <strong>${pivotId}</strong>
            <span class="expected-pivot-item-meta">Na fila desde ${addedAt}</span>
          </div>
          <button
            class="ghost"
            type="button"
            data-expected-pivot-remove="${pivotId}"
            ${state.expectedPivotActionInFlight ? "disabled" : ""}
          >
            Remover
          </button>
        </div>
      `;
    })
    .join("");
}

function canCurrentUserManagePivotTechnology() {
  return canCurrentUserDeletePivots();
}

function syncPivotTechnologyControl() {
  if (!ui.pivotAdminTech) return;
  const allowed = canCurrentUserManagePivotTechnology();
  const hasPivotSelected = !!String(state.selectedPivot || "").trim();
  ui.pivotAdminTech.hidden = !hasPivotSelected;

  const disableTechControl = !allowed || !hasPivotSelected;
  if (ui.pivotTechControls) {
    ui.pivotTechControls.hidden = !allowed;
  }
  if (ui.pivotConcentratorToggle) {
    ui.pivotConcentratorToggle.disabled = disableTechControl;
  }
  if (ui.savePivotTechnologyBtn) {
    ui.savePivotTechnologyBtn.disabled = disableTechControl;
  }

  if (ui.pivotLocationInput) {
    ui.pivotLocationInput.disabled = !hasPivotSelected;
    ui.pivotLocationInput.readOnly = !allowed;
  }
  if (ui.savePivotLocationBtn) {
    ui.savePivotLocationBtn.hidden = !allowed;
    ui.savePivotLocationBtn.disabled = !allowed || !hasPivotSelected;
  }
  if (ui.resetPivotModemBtn) {
    ui.resetPivotModemBtn.hidden = !hasPivotSelected;
    ui.resetPivotModemBtn.disabled = !hasPivotSelected;
  }

  if (ui.pivotTechnologyHint && disableTechControl) {
    ui.pivotTechnologyHint.textContent = "";
  }
  if (ui.pivotLocationHint && !hasPivotSelected) {
    ui.pivotLocationHint.textContent = "";
  }
}

async function savePivotTechnologySetting() {
  if (!canCurrentUserManagePivotTechnology()) {
    syncPivotTechnologyControl();
    showToast("Apenas o administrador principal pode alterar a tecnologia do pivô.", "error", 3800);
    return;
  }

  const pivotId = String(state.selectedPivot || "").trim();
  if (!pivotId || !ui.pivotConcentratorToggle) return;

  if (ui.savePivotTechnologyBtn) ui.savePivotTechnologyBtn.disabled = true;
  try {
    const response = await fetch(buildApiUrl("/api/pivot-technology"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        pivot_id: pivotId,
        is_concentrator: !!ui.pivotConcentratorToggle.checked,
      }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }
    if (ui.pivotTechnologyHint) {
      ui.pivotTechnologyHint.textContent = "Tecnologia atualizada com sucesso.";
    }
    showToast("Tecnologia do pivô salva com sucesso.", "success", 3200);
    await refreshAll();
  } catch (err) {
    if (ui.pivotTechnologyHint) {
      ui.pivotTechnologyHint.textContent = "Não foi possível salvar a tecnologia. Tente novamente.";
    }
    showToast("Não foi possível salvar a tecnologia do pivô.", "error", 4200);
  } finally {
    if (ui.savePivotTechnologyBtn) ui.savePivotTechnologyBtn.disabled = false;
  }
}

async function savePivotLocationSetting() {
  if (!canCurrentUserManagePivotTechnology()) {
    syncPivotTechnologyControl();
    showToast("Apenas o administrador principal pode alterar a localização do pivô.", "error", 3800);
    return;
  }

  const pivotId = String(state.selectedPivot || "").trim();
  if (!pivotId || !ui.pivotLocationInput) return;

  const parsed = parsePivotCoordinatesInput(ui.pivotLocationInput.value);
  if (!parsed.ok) {
    if (ui.pivotLocationHint) ui.pivotLocationHint.textContent = parsed.error;
    showToast(parsed.error, "error", 3800);
    return;
  }

  if (ui.savePivotLocationBtn) ui.savePivotLocationBtn.disabled = true;
  try {
    const response = await fetch(buildApiUrl("/api/pivot-location"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        pivot_id: pivotId,
        latitude: parsed.latitude,
        longitude: parsed.longitude,
      }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }
    if (ui.pivotLocationHint) {
      ui.pivotLocationHint.textContent = "Localização atualizada com sucesso.";
    }
    showToast("Localização do pivô salva com sucesso.", "success", 3200);
    await refreshAll();
  } catch (err) {
    if (ui.pivotLocationHint) {
      ui.pivotLocationHint.textContent = "Não foi possível salvar a localização. Tente novamente.";
    }
    showToast("Não foi possível salvar a localização do pivô.", "error", 4200);
  } finally {
    if (ui.savePivotLocationBtn) ui.savePivotLocationBtn.disabled = false;
  }
}

async function resetSelectedPivotModem() {
  const pivotId = String(state.selectedPivot || "").trim();
  if (!pivotId) return;

  const firmware = getPivotFirmwareVersionForReset(state.pivotData || findPivotById(pivotId));
  const expectsAck = supportsModemResetAckByFirmware(firmware);
  const confirmed = window.confirm(`Enviar comando de reset (#92$) para o pivô ${pivotId}?`);
  if (!confirmed) return;

  if (ui.resetPivotModemBtn) ui.resetPivotModemBtn.disabled = true;
  try {
    const response = await fetch(buildApiUrl("/api/pivot-reset-modem"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        pivot_id: pivotId,
      }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }

    const resetCommand = data.reset_command && typeof data.reset_command === "object" ? data.reset_command : {};
    const commandTs = Number(resetCommand.command_ts);
    if (expectsAck && Number.isFinite(commandTs)) {
      queuePendingModemResetAck(pivotId, commandTs);
      showToast("Comando #92$ enviado. Aguardando confirmação de restart do modem.", "success", 4200);
      await refreshPivot();
    } else {
      delete state.pendingModemResetAcks[pivotId];
      showToast("Comando #92$ enviado para o modem.", "success", 3200);
      window.alert("Versao de firmware desatualizada, nao é possivel saber se o reset foi feito com sucesso");
      await refreshPivot();
    }
  } catch (err) {
    delete state.pendingModemResetAcks[pivotId];
    showToast("Não foi possível enviar o reset do modem.", "error", 4200);
  } finally {
    if (ui.resetPivotModemBtn) {
      const hasPivotSelected = !!String(state.selectedPivot || "").trim();
      ui.resetPivotModemBtn.disabled = !hasPivotSelected;
    }
  }
}

async function requestSelectedPivotConfig() {
  const pivotId = String(state.selectedPivot || "").trim();
  if (!pivotId || state.pivotConfigRequestInFlight) return;

  state.pivotConfigRequestInFlight = true;
  renderPivotConfigModal();
  try {
    const response = await fetch(buildApiUrl("/api/pivot-config-request"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ pivot_id: pivotId }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }
    showToast("Pedido de configuração enviado.", "success", 3200);
    await refreshPivot();
  } catch (err) {
    showToast("Não foi possível pedir a configuração.", "error", 4200);
  } finally {
    state.pivotConfigRequestInFlight = false;
    renderPivotConfigModal();
  }
}

async function deleteSelectedPivot() {
  if (!canCurrentUserDeletePivots()) {
    syncPivotDeleteControl();
    showToast("Apenas o administrador principal pode deletar pivos.", "error", 3800);
    return;
  }

  const pivotId = String(state.selectedPivot || "").trim();
  if (!pivotId) return;

  const confirmed = window.confirm(`Tem certeza que deseja deletar o pivô ${pivotId}?`);
  if (!confirmed) {
    return;
  }

  if (ui.deletePivotBtn) ui.deletePivotBtn.disabled = true;
  try {
    const response = await fetch(buildApiUrl(`/api/pivot/${encodeURIComponent(pivotId)}/delete`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ source: "ui" }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    showToast("Pivô deletado com sucesso.", "success", 3200);
    closePivot();
    await refreshAll();
  } catch (err) {
    showToast("Não foi possível deletar o pivô. Tente novamente.", "error", 4200);
  } finally {
    if (ui.deletePivotBtn) ui.deletePivotBtn.disabled = false;
  }
}

async function resetSelectedPivotsBulk() {
  if (!canCurrentUserDeletePivots()) {
    syncBulkPivotActionControls();
    showToast("Apenas o administrador principal pode resetar varios pivos.", "error", 3800);
    return;
  }

  const pivotIds = getSelectedPivotIds();
  if (pivotIds.length < 2) {
    syncBulkPivotActionControls();
    return;
  }

  const confirmed = window.confirm(
    `Enviar comando de reset (#92$) para ${pivotIds.length} pivôs?\n\n${buildPivotListPreview(pivotIds, 10)}`
  );
  if (!confirmed) return;

  state.bulkPivotActionInFlight = true;
  syncBulkPivotActionControls();
  renderCards();
  try {
    const response = await fetch(buildApiUrl("/api/pivots/reset-modem"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        pivot_ids: pivotIds,
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }

    const results = Array.isArray(data.results) ? data.results : [];
    const successIds = [];
    const queuedAckIds = [];
    const noAckIds = [];
    const failedIds = [];

    for (const item of results) {
      const pivotId = String(item?.pivot_id || "").trim();
      if (!pivotId || !item?.ok) {
        if (pivotId) failedIds.push(pivotId);
        continue;
      }
      successIds.push(pivotId);

      const pivot = findPivotById(pivotId) || (String(state.selectedPivot || "").trim() === pivotId ? state.pivotData : null);
      const expectsAck = supportsModemResetAckByFirmware(getPivotFirmwareVersionForReset(pivot));
      const commandTs = Number((item.reset_command || {}).command_ts);
      if (expectsAck && Number.isFinite(commandTs)) {
        queuePendingModemResetAck(pivotId, commandTs);
        queuedAckIds.push(pivotId);
      } else {
        delete state.pendingModemResetAcks[pivotId];
        noAckIds.push(pivotId);
      }
    }

    if (successIds.length) {
      const waitSuffix = queuedAckIds.length ? ` Aguardando confirmação de ${queuedAckIds.length}.` : "";
      showToast(`Comando #92$ enviado para ${successIds.length} pivôs.${waitSuffix}`, "success", 4800);
    }
    if (noAckIds.length) {
      window.alert(`Firmware sem confirmação automática de restart para: ${buildPivotListPreview(noAckIds, 8)}.`);
    }
    if (failedIds.length) {
      showToast(`Não foi possível enviar reset para ${failedIds.length} pivô(s).`, "error", 4800);
    }
    if (!successIds.length && !failedIds.length) {
      throw new Error("Nenhum resultado retornado");
    }
    await refreshAll();
  } catch (err) {
    showToast("Não foi possível enviar o reset em lote.", "error", 4200);
  } finally {
    state.bulkPivotActionInFlight = false;
    syncBulkPivotActionControls();
    renderCards();
  }
}

async function deleteSelectedPivotsBulk() {
  if (!canCurrentUserDeletePivots()) {
    syncBulkPivotActionControls();
    showToast("Apenas o administrador principal pode deletar varios pivos.", "error", 3800);
    return;
  }

  const pivotIds = getSelectedPivotIds();
  if (pivotIds.length < 2) {
    syncBulkPivotActionControls();
    return;
  }

  const confirmed = window.confirm(
    `Tem certeza que deseja deletar ${pivotIds.length} pivôs?\n\n${buildPivotListPreview(pivotIds, 10)}`
  );
  if (!confirmed) return;

  state.bulkPivotActionInFlight = true;
  syncBulkPivotActionControls();
  renderCards();
  try {
    const response = await fetch(buildApiUrl("/api/pivots/delete"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        pivot_ids: pivotIds,
        source: "ui-bulk",
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }

    const results = Array.isArray(data.results) ? data.results : [];
    const deletedIds = [];
    const failedIds = [];

    for (const item of results) {
      const pivotId = String(item?.pivot_id || "").trim();
      if (!pivotId || !item?.ok) {
        if (pivotId) failedIds.push(pivotId);
        continue;
      }
      deletedIds.push(pivotId);
      delete state.pendingModemResetAcks[pivotId];
    }

    if (deletedIds.length) {
      const deletedSet = new Set(deletedIds);
      setSelectedPivotIds(getSelectedPivotIds().filter((pivotId) => !deletedSet.has(pivotId)));
      showToast(`${deletedIds.length} pivô(s) deletado(s) com sucesso.`, "success", 3600);
    }
    if (failedIds.length) {
      showToast(`Não foi possível deletar ${failedIds.length} pivô(s).`, "error", 4800);
    }
    if (!deletedIds.length && !failedIds.length) {
      throw new Error("Nenhum resultado retornado");
    }
    await refreshAll();
  } catch (err) {
    showToast("Não foi possível deletar os pivôs selecionados.", "error", 4200);
  } finally {
    state.bulkPivotActionInFlight = false;
    syncBulkPivotActionControls();
    renderCards();
  }
}

async function addExpectedPivots() {
  if (!canCurrentUserManageExpectedPivots()) {
    renderExpectedPivotAdmin();
    showToast("Apenas administradores podem configurar novos pivos.", "error", 3800);
    return;
  }

  const pivotIds = parsePivotIdBatchInput(ui.expectedPivotInput?.value || "");
  if (!pivotIds.length) {
    showToast("Informe um ou mais pivot_ids para descoberta.", "warn", 3200);
    return;
  }

  state.expectedPivotActionInFlight = true;
  renderExpectedPivotAdmin();
  try {
    const response = await fetch(buildApiUrl("/api/pivots/expected"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        pivot_ids: pivotIds,
        source: "ui",
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }

    const results = Array.isArray(data.results) ? data.results : [];
    const added = summarizeExpectedPivotResults(results, "added");
    const alreadyExists = summarizeExpectedPivotResults(results, "already_exists");
    const alreadyPending = summarizeExpectedPivotResults(results, "already_pending");
    const invalid = summarizeExpectedPivotResults(results, "invalid");

    if (added.length && ui.expectedPivotInput) {
      ui.expectedPivotInput.value = "";
    }
    if (added.length) {
      state.expectedPivotPanelExpanded = true;
      showToast(`${added.length} pivot_id(s) adicionados para descoberta.`, "success", 3600);
    }
    if (alreadyExists.length) {
      showToast(`Ja registrados: ${buildPivotListPreview(alreadyExists.map((item) => item.pivot_id), 5)}.`, "warn", 4600);
    }
    if (alreadyPending.length) {
      showToast(
        `Ja aguardando descoberta: ${buildPivotListPreview(alreadyPending.map((item) => item.pivot_id), 5)}.`,
        "warn",
        4600
      );
    }
    if (invalid.length) {
      showToast(`Pivot_id invalido: ${buildPivotListPreview(invalid.map((item) => item.pivot_id), 5)}.`, "error", 4800);
    }
    if (Array.isArray(data.pending)) {
      setExpectedPivotsPendingState(data.pending);
      renderExpectedPivotAdmin();
    }
    await refreshAll();
  } catch (err) {
    showToast("Nao foi possivel atualizar a fila de descoberta.", "error", 4200);
  } finally {
    state.expectedPivotActionInFlight = false;
    renderExpectedPivotAdmin();
  }
}

async function removeExpectedPivot(pivotId) {
  if (!canCurrentUserManageExpectedPivots()) {
    renderExpectedPivotAdmin();
    showToast("Apenas administradores podem configurar novos pivos.", "error", 3800);
    return;
  }

  const normalizedPivotId = String(pivotId || "").trim();
  if (!normalizedPivotId) return;

  state.expectedPivotActionInFlight = true;
  renderExpectedPivotAdmin();
  try {
    const response = await fetch(buildApiUrl("/api/pivots/expected/remove"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        pivot_id: normalizedPivotId,
        source: "ui",
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || data.message || `HTTP ${response.status}`);
    }

    if (data.ok) {
      removeExpectedPivotPendingState(normalizedPivotId);
      renderExpectedPivotAdmin();
      showToast(`Pivot_id ${normalizedPivotId} removido da fila.`, "success", 3200);
    } else {
      showToast(`Pivot_id ${normalizedPivotId} nao estava na fila.`, "warn", 3200);
    }
    await refreshAll();
  } catch (err) {
    showToast("Nao foi possivel remover o pivot_id da fila.", "error", 4200);
  } finally {
    state.expectedPivotActionInFlight = false;
    renderExpectedPivotAdmin();
  }
}

function syncAdminControls() {
  const role = String(state.authUserRole || "user").trim().toLowerCase();
  const isAdmin = role === "admin";
  if (ui.adminCreateAccountLink) {
    ui.adminCreateAccountLink.hidden = !isAdmin;
  }
  if (ui.adminUsersPanel) {
    ui.adminUsersPanel.hidden = !isAdmin;
  }
  if (!isAdmin) {
    state.adminUsers = [];
    renderAdminUsers();
  }
  syncPivotDeleteControl();
  syncPivotTechnologyControl();
  syncBulkPivotActionControls();
  renderExpectedPivotAdmin();
}

function renderAdminUsers() {
  if (!ui.adminUsersTable || !ui.adminUsersEmpty) return;

  const role = String(state.authUserRole || "user").trim().toLowerCase();
  if (role !== "admin") {
    ui.adminUsersTable.innerHTML = "";
    ui.adminUsersEmpty.hidden = true;
    return;
  }

  const users = Array.isArray(state.adminUsers) ? state.adminUsers : [];
  if (!users.length) {
    ui.adminUsersTable.innerHTML = "";
    ui.adminUsersEmpty.hidden = false;
    return;
  }
  ui.adminUsersEmpty.hidden = true;

  ui.adminUsersTable.innerHTML = users
    .map((user) => {
      const userId = String(user.id || "").trim();
      const email = text(user.email, "-");
      const name = text(user.name, "-");
      const status = text(user.status, "-");
      const roleText = text(user.role, "user");
      const lastLogin = formatDateTimeValue(user.last_login_at);
      return `
        <tr>
          <td>${escapeHtml(email)}</td>
          <td>${escapeHtml(name)}</td>
          <td>${escapeHtml(roleText)}</td>
          <td>${escapeHtml(status)}</td>
          <td>${escapeHtml(lastLogin)}</td>
          <td>
            <button
              type="button"
              class="danger-inline"
              data-admin-delete-user-id="${escapeHtml(userId)}"
              data-admin-delete-user-email="${escapeHtml(email)}"
            >
              Deletar
            </button>
          </td>
        </tr>
      `;
    })
    .join("");
}

async function refreshAdminUsers() {
  const role = String(state.authUserRole || "user").trim().toLowerCase();
  if (role !== "admin") return;

  if (ui.adminUsersRefresh) ui.adminUsersRefresh.disabled = true;
  try {
    const response = await fetch(buildApiUrl("/admin/users"), {
      method: "GET",
      credentials: "include",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.message || data.error || `HTTP ${response.status}`);
    }
    state.adminUsers = Array.isArray(data.users) ? data.users : [];
    renderAdminUsers();
  } catch (err) {
    showToast("Nao foi possivel carregar as contas de usuario.", "error", 4200);
  } finally {
    if (ui.adminUsersRefresh) ui.adminUsersRefresh.disabled = false;
  }
}

async function deleteManagedUser(targetUserId, targetUserEmail) {
  const userId = String(targetUserId || "").trim();
  const email = String(targetUserEmail || "").trim();
  if (!userId) return;
  const confirmed = window.confirm(`Tem certeza que deseja deletar a conta ${email || userId}?`);
  if (!confirmed) return;

  try {
    const response = await fetch(buildApiUrl("/admin/users/delete"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ user_id: userId }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.message || data.error || `HTTP ${response.status}`);
    }
    showToast("Conta deletada com sucesso.", "success", 3200);
    await refreshAdminUsers();
  } catch (err) {
    const message = String((err && err.message) || "").trim();
    showToast(message || "Nao foi possivel deletar a conta.", "error", 4200);
  }
}

async function resolveViewerRole() {
  try {
    const response = await fetch(buildApiUrl("/auth/me"), {
      method: "GET",
      credentials: "include",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    const data = await response.json();
    if (!response.ok || !data.ok || !data.authenticated) {
      window.location.assign(buildAppUrl("/login"));
      return false;
    }
    const user = ((data || {}).user || {});
    const role = String(user.role || "user").trim().toLowerCase();
    const email = String(user.email || "").trim().toLowerCase();
    const apiDeleteAllowed = Boolean((data || {}).pivot_delete_allowed);
    state.authUserRole = role || "user";
    state.authUserEmail = email;
    state.pivotDeleteAllowed = apiDeleteAllowed || (state.authUserRole === "admin" && state.authUserEmail === FIXED_PIVOT_DELETE_EMAIL);
    syncAdminControls();
    return true;
  } catch (err) {
    window.location.assign(buildAppUrl("/login"));
    return false;
  }
}

async function logoutDashboard() {
  if (ui.logoutBtn) ui.logoutBtn.disabled = true;
  try {
    await fetch(buildApiUrl("/auth/logout"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({}),
    });
  } catch (err) {
    // no-op
  } finally {
    window.location.assign(buildAppUrl("/login"));
  }
}

async function saveProbeSetting() {
  if (!state.selectedPivot) return;

  const payload = {
    pivot_id: state.selectedPivot,
    enabled: !!ui.probeEnabled.checked,
    interval_sec: Number(ui.probeInterval.value || 0),
  };

  ui.saveProbe.disabled = true;
  try {
    const response = await fetch(buildApiUrl("/api/probe-config"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    ui.probeHint.textContent = "Configuração de monitoramento de latência salva com sucesso.";
    showToast("Configuração salva.", "success", 3000);
    await refreshAll();
  } catch (err) {
    ui.probeHint.textContent = "Não foi possível salvar a configuração. Tente novamente.";
    showToast("Não foi possível salvar a configuração. Tente novamente.", "error", 4200);
  } finally {
    ui.saveProbe.disabled = false;
  }
}

function wireEvents() {
  ui.searchInput.addEventListener("input", () => {
    state.search = ui.searchInput.value || "";
    state.cardsPage = 1;
    renderCards();
  });

  ui.sortSelect.addEventListener("change", () => {
    state.sort = ui.sortSelect.value || "critical";
    state.cardsPage = 1;
    renderCards();
  });

  if (ui.technologyFilter) {
    ui.technologyFilter.addEventListener("change", () => {
      state.technologyFilter = normalizeFilterKey(ui.technologyFilter.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.firmwareFilter) {
    ui.firmwareFilter.addEventListener("change", () => {
      state.firmwareFilter = normalizeFilterKey(ui.firmwareFilter.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.statusFilterSelect) {
    ui.statusFilterSelect.addEventListener("change", () => {
      state.statusFilter = normalizeFilterKey(ui.statusFilterSelect.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.connectivityFilterSelect) {
    ui.connectivityFilterSelect.addEventListener("change", () => {
      state.connectivityFilter = normalizeFilterKey(ui.connectivityFilterSelect.value) || "all";
      state.cardsPage = 1;
      renderCards();
    });
  }

  if (ui.clearFilters) {
    ui.clearFilters.addEventListener("click", () => {
      resetAllFilters();
    });
  }

  if (ui.logoutBtn) {
    ui.logoutBtn.addEventListener("click", logoutDashboard);
  }

  if (ui.adminUsersRefresh) {
    ui.adminUsersRefresh.addEventListener("click", refreshAdminUsers);
  }

  if (ui.adminUsersTable) {
    ui.adminUsersTable.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const button = target.closest("button[data-admin-delete-user-id]");
      if (!button) return;
      const userId = String(button.getAttribute("data-admin-delete-user-id") || "").trim();
      const userEmail = String(button.getAttribute("data-admin-delete-user-email") || "").trim();
      deleteManagedUser(userId, userEmail);
    });
  }

  if (ui.addExpectedPivotBtn) {
    ui.addExpectedPivotBtn.addEventListener("click", addExpectedPivots);
  }
  if (ui.toggleExpectedPivotPanelBtn) {
    ui.toggleExpectedPivotPanelBtn.addEventListener("click", () => {
      toggleExpectedPivotPanel();
    });
  }
  if (ui.expectedPivotList) {
    ui.expectedPivotList.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const button = target.closest("button[data-expected-pivot-remove]");
      if (!button) return;
      const pivotId = String(button.getAttribute("data-expected-pivot-remove") || "").trim();
      removeExpectedPivot(pivotId);
    });
  }

  ui.cardsGrid.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const button = target.closest(".open-pivot");
    if (!button || !ui.cardsGrid.contains(button)) return;
    openPivot(button.dataset.pivot || "");
  });

  ui.cardsGrid.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.classList.contains("pivot-select-all-checkbox")) {
      toggleVisiblePivotSelection(!!target.checked);
      return;
    }
    if (target.classList.contains("pivot-select-checkbox")) {
      toggleSelectedPivotId(target.dataset.pivotId || "", !!target.checked);
    }
  });

  if (ui.pivotMetricsToggleBtn) {
    ui.pivotMetricsToggleBtn.addEventListener("click", () => {
      state.pivotMetricsExpanded = !state.pivotMetricsExpanded;
      renderPivotView();
    });
  }

  ui.cardsPrev.addEventListener("click", () => {
    state.cardsPage = Math.max(1, state.cardsPage - 1);
    renderCards();
    queueVisibleCardsConnectivityRefresh();
  });

  ui.cardsNext.addEventListener("click", () => {
    const total = Math.max(1, Math.ceil(applyFilterSort().length / state.cardsPageSize));
    state.cardsPage = Math.min(total, state.cardsPage + 1);
    renderCards();
    queueVisibleCardsConnectivityRefresh();
  });

  ui.closePivot.addEventListener("click", closePivot);
  if (ui.deletePivotBtn) {
    ui.deletePivotBtn.addEventListener("click", deleteSelectedPivot);
  }
  if (ui.pivotMoreInfoBtn) {
    ui.pivotMoreInfoBtn.addEventListener("click", () => {
      state.pivotMetricsExpanded = !state.pivotMetricsExpanded;
      renderPivotView();
    });
  }
  if (ui.savePivotTechnologyBtn) {
    ui.savePivotTechnologyBtn.addEventListener("click", savePivotTechnologySetting);
  }
  if (ui.savePivotLocationBtn) {
    ui.savePivotLocationBtn.addEventListener("click", savePivotLocationSetting);
  }
  if (ui.resetPivotModemBtn) {
    ui.resetPivotModemBtn.addEventListener("click", resetSelectedPivotModem);
  }
  if (ui.pivotSettingsBtn) {
    ui.pivotSettingsBtn.addEventListener("click", openSettingsModal);
  }
  if (ui.settingsModalClose) {
    ui.settingsModalClose.addEventListener("click", closeSettingsModal);
  }
  if (ui.requestPivotConfigBtn) {
    ui.requestPivotConfigBtn.addEventListener("click", requestSelectedPivotConfig);
  }
  if (ui.settingsModal) {
    ui.settingsModal.addEventListener("click", (event) => {
      if (event.target === ui.settingsModal) closeSettingsModal();
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && ui.settingsModal && !ui.settingsModal.hidden) {
      closeSettingsModal();
    }
  });
  if (ui.bulkResetPivotsBtn) {
    ui.bulkResetPivotsBtn.addEventListener("click", resetSelectedPivotsBulk);
  }
  if (ui.bulkDeletePivotsBtn) {
    ui.bulkDeletePivotsBtn.addEventListener("click", deleteSelectedPivotsBulk);
  }
  ui.saveProbe.addEventListener("click", saveProbeSetting);

  ui.connPreset.addEventListener("change", () => {
    state.connPreset = ui.connPreset.value || "30d";
    const custom = state.connPreset === "custom";
    ui.connFromWrap.hidden = !custom;
    ui.connToWrap.hidden = !custom;
    renderPivotView();
    void refreshQualityOverrides({ force: true });
  });

  ui.connApply.addEventListener("click", () => {
    state.connPreset = ui.connPreset.value || "30d";
    state.connCustomFrom = ui.connFrom.value || "";
    state.connCustomTo = ui.connTo.value || "";
    const custom = state.connPreset === "custom";
    ui.connFromWrap.hidden = !custom;
    ui.connToWrap.hidden = !custom;
    renderPivotView();
    void refreshQualityOverrides({ force: true });
  });

  ui.probeDelayPreset.addEventListener("change", () => {
    state.probeDelayPreset = ui.probeDelayPreset.value || "30d";
    const custom = state.probeDelayPreset === "custom";
    ui.probeDelayRange.hidden = !custom;
    ui.probeDelayFromWrap.hidden = !custom;
    ui.probeDelayToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.probeDelayApply.addEventListener("click", () => {
    state.probeDelayPreset = ui.probeDelayPreset.value || "30d";
    state.probeDelayCustomFrom = ui.probeDelayFrom.value || "";
    state.probeDelayCustomTo = ui.probeDelayTo.value || "";
    const custom = state.probeDelayPreset === "custom";
    ui.probeDelayRange.hidden = !custom;
    ui.probeDelayFromWrap.hidden = !custom;
    ui.probeDelayToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.rssiPreset.addEventListener("change", () => {
    state.rssiPreset = ui.rssiPreset.value || "30d";
    const custom = state.rssiPreset === "custom";
    ui.rssiRange.hidden = !custom;
    ui.rssiFromWrap.hidden = !custom;
    ui.rssiToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.rssiApply.addEventListener("click", () => {
    state.rssiPreset = ui.rssiPreset.value || "30d";
    state.rssiCustomFrom = ui.rssiFrom.value || "";
    state.rssiCustomTo = ui.rssiTo.value || "";
    const custom = state.rssiPreset === "custom";
    ui.rssiRange.hidden = !custom;
    ui.rssiFromWrap.hidden = !custom;
    ui.rssiToWrap.hidden = !custom;
    renderPivotView();
  });

  ui.connTrack.addEventListener("click", (event) => {
    const segmentEl = event.target.closest(".conn-segment");
    if (!segmentEl || !ui.connTrack.contains(segmentEl)) return;

    const clickedKey = segmentEl.dataset.key || "";
    if (!clickedKey) return;

    if (state.connSelectedSegmentKey === clickedKey) {
      clearConnectivitySegmentSelection();
      return;
    }

    for (const item of ui.connTrack.querySelectorAll(".conn-segment.selected")) {
      item.classList.remove("selected");
    }
    state.connSelectedSegmentKey = clickedKey;
    segmentEl.classList.add("selected");
    showConnectivitySegmentCard(segmentEl);
  });

  document.addEventListener("click", (event) => {
    if (!state.connSelectedSegmentKey) return;
    const wrapEl = ui.connTrack.parentElement;
    if (wrapEl && wrapEl.contains(event.target)) return;
    clearConnectivitySegmentSelection();
  });

  ui.timelinePrev.addEventListener("click", () => {
    const filterKey = normalizeConnectivityEventTopicFilter(state.timelineTopicFilter);
    setTimelinePageForFilter(filterKey, getTimelinePageForFilter(filterKey) - 1);
    renderPivotView();
  });
  ui.timelineNext.addEventListener("click", () => {
    const filterKey = normalizeConnectivityEventTopicFilter(state.timelineTopicFilter);
    const total = Math.max(
      1,
      Math.ceil(getConnectivityEventsPanelFiltered(state.pivotData || {}, filterKey).length / state.timelinePageSize)
    );
    setTimelinePageForFilter(filterKey, Math.min(total, getTimelinePageForFilter(filterKey) + 1));
    renderPivotView();
  });
  if (ui.timelineTopicSelect) {
    ui.timelineTopicSelect.addEventListener("change", () => {
      const filterKey = normalizeConnectivityEventTopicFilter(ui.timelineTopicSelect.value);
      state.timelineTopicFilter = filterKey;
      setTimelinePageForFilter(filterKey, 1);
      renderPivotView();
    });
  }
  if (ui.timelineDownloadTxt) {
    ui.timelineDownloadTxt.addEventListener("click", () => {
      const filterKey = normalizeConnectivityEventTopicFilter(state.timelineTopicFilter);
      downloadConnectivityEventsTxt(state.pivotData || {}, filterKey);
    });
  }

  window.addEventListener("hashchange", () => {
    const hashPivot = parseHashPivot();
    if (!hashPivot) {
      closePivot();
      return;
    }
    openPivot(hashPivot);
  });

  window.addEventListener("resize", () => {
    if (!state.connSelectedSegmentKey || ui.connSegmentCard.hidden) return;
    const selectedEl = [...ui.connTrack.querySelectorAll(".conn-segment")].find(
      (item) => (item.dataset.key || "") === state.connSelectedSegmentKey
    );
    if (!selectedEl) {
      clearConnectivitySegmentSelection();
      return;
    }
    positionConnectivitySegmentCard(selectedEl);
  });
}

async function boot() {
  if (ensureCanonicalAppOrigin()) return;
  const authorized = await resolveViewerRole();
  if (!authorized) return;
  setInitialLoading(true, "Buscando pivos e calculando conectividade inicial...");
  wireEvents();
  const shouldLoadAdminUsers = String(state.authUserRole || "").trim().toLowerCase() === "admin";
  startDevAutoReload();
  await loadUiConfig();
  ensurePivotTableColumnOrder();
  await loadPivotTableColumnPreference();
  ui.connPreset.value = state.connPreset;
  ui.connFromWrap.hidden = true;
  ui.connToWrap.hidden = true;
  ui.probeDelayPreset.value = state.probeDelayPreset;
  ui.probeDelayRange.hidden = true;
  ui.probeDelayFromWrap.hidden = true;
  ui.probeDelayToWrap.hidden = true;
  ui.rssiPreset.value = state.rssiPreset;
  ui.rssiRange.hidden = true;
  ui.rssiFromWrap.hidden = true;
  ui.rssiToWrap.hidden = true;
  const hashPivot = parseHashPivot();
  if (hashPivot) {
    state.selectedPivot = hashPivot;
  }
  try {
    await refreshInitialDashboardState();
  } catch (err) {
    const hasRenderableData = Array.isArray(state.pivots) && state.pivots.length > 0;
    if (!hasRenderableData) {
      ui.cardsGrid.innerHTML = `<div class="empty">Nao foi possivel carregar os dados. Tente novamente.</div>`;
    }
    const now = Date.now();
    if (now - Number(state.lastRefreshToastAtMs || 0) > 15000) {
      showToast("Nao foi possivel carregar os dados. Tente novamente.", "error", 3800);
      state.lastRefreshToastAtMs = now;
    }
  } finally {
    setInitialLoading(false);
  }
  if (shouldLoadAdminUsers) {
    void refreshAdminUsers();
  }
  setInterval(refreshAll, state.refreshMs);
}

if (HAS_DOM) {
  boot();
}

// Exports for Node-based regression tests (no DOM execution).
if (typeof module !== "undefined" && module.exports) {
  module.exports = {
    _test: {
      pivotSampleCount,
      compareBySamplesDesc,
      parseSortablePct,
      pivotConnectedPct,
      pivotDisconnectedPct,
      compareByConnectedPctDesc,
      compareByDisconnectedPctDesc,
      isProbeMonitoringEnabled,
      pivotProbeResponseRatioPct,
      compareByProbeResponseRatioDesc,
      parseSignalTechnologyCombined,
      pivotSignalValue,
      pivotTechnologyColumnValue,
      normalizeTimelineMiniSegments,
      buildTimelineMiniSegmentsFromConnectivity,
      resolvePivotTimelineMiniSegments,
      normalizePivotTableColumnOrder,
      expandPivotTableColumnOrderKey,
      buildQualitySourceSignature,
      isPivotConcentrator,
      pivotTechnologyValue,
      getDisplayStatus,
      getDisplayQuality,
      buildQualityFromConnectivity,
      resolveTimelineReferenceNowTs,
      getConnectivityEventsPanelCapped,
      resolveDisconnectThresholdSec,
      buildConnectivitySegments,
      summarizeConnectivitySegments,
      buildConnectivityQualityInput,
      buildConnectivityStatus,
      computeConnectivityFromRange,
      getConnectivityEventsPanelFiltered,
      buildConnectivityEventsTxt,
      resolveConnectivityEventRawPayload,
      normalizePivotIdList,
      parsePivotIdBatchInput,
      getPivotFirmwareVersionForReset,
      getPivotModemResetSummary,
      collectConfirmedModemResetAcks,
      getExpectedPivotsPending,
      setExpectedPivotsPendingState,
      removeExpectedPivotPendingState,
      hasPivotCoordinates,
      shouldRenderRssiPanel,
      collectRssiChartPoints,
      resolveRssiChartDomain,
      getProbeResponseInfoDisplayRows,
      formatProbeConfiguredNetworks,
      normalizeSummaryCardsHistoryPoints,
      computeSummaryCardCountsFromPivots,
      hasCompleteQualityOverridesForPivots,
      isRecentSummaryHistoryWarmupOutlier,
      buildSummaryCardsDisplayHistoryPoints,
      sampleSummaryCardsHistoryPoints,
      buildSummaryCardsHistorySvgModel,
      formatShortDateTime,
      formatCompactDateTime,
      formatTimestampFromTsOrValue,
      state,
    },
  };
}
