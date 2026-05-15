const test = require("node:test");
const assert = require("node:assert/strict");

const { _test } = require("../frontend/dashboard.js");

test("connectivity: timeline end state matches status at referenceTime (regression for 100% connected vs disconnected)", () => {
  const pivot = {
    summary: {
      // Force a known threshold so the test is independent from expected interval heuristics.
      disconnect_threshold_sec: 70,
    },
    timeline: [
      // Single message at the window start. Should disconnect before the window end.
      { topic: "cloudv2", ts: 1000 },
    ],
  };

  const view = _test.computeConnectivityFromRange(pivot, 1000, 1100, {});

  // At endTs=1100, lastSeenAt=1000, threshold=70 => 1100-1000=100 > 70 => disconnected.
  assert.equal(view.status.state, "disconnected");
  assert.equal(view.status.code, "red");
  assert.equal(view.segments.at(-1).state, "disconnected");
  assert.notEqual(view.connectivityQualityInput.connectedPct, 100);
});

test("connectivity: status is derived from the same timeline rule (ignores backend summary.status for filtered ranges)", () => {
  const pivot = {
    summary: {
      disconnect_threshold_sec: 70,
      // Simulates backend/current status being offline, while a past window can still be online.
      status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
    },
    timeline: [{ topic: "cloudv2", ts: 1000 }],
  };

  const view = _test.computeConnectivityFromRange(pivot, 1000, 1050, {});

  // endTs=1050, lastSeenAt=1000, threshold=70 => connected
  assert.equal(view.status.state, "connected");
  assert.equal(view.status.code, "green");
  assert.equal(view.segments.at(-1).state, "connected");
  assert.equal(view.connectivityQualityInput.connectedPct, 100);
});

test("connectivity: active run prefers backend reference time over client clock drift", () => {
  const originalNow = Date.now;
  const originalRawState = _test.state.rawState;
  const originalPanelRunMeta = _test.state.panelRunMeta;
  Date.now = () => 2_000_000;
  try {
    _test.state.rawState = {
      updated_at_ts: 1500,
      mode: "live",
    };
    _test.state.panelRunMeta = { is_active: true, updated_at_ts: 1500 };
    const referenceTs = _test.resolveTimelineReferenceNowTs({
      updated_at_ts: 1200,
      timeline: [{ topic: "cloudv2", ts: 1200 }],
      run: { is_active: true },
    });
    assert.equal(referenceTs, 1500);
  } finally {
    _test.state.rawState = originalRawState;
    _test.state.panelRunMeta = originalPanelRunMeta;
    Date.now = originalNow;
  }
});

test("connectivity: active run falls back to persisted event time when backend reference is unavailable", () => {
  const originalNow = Date.now;
  const originalRawState = _test.state.rawState;
  const originalPanelRunMeta = _test.state.panelRunMeta;
  Date.now = () => 2_000_000;
  try {
    _test.state.rawState = null;
    _test.state.panelRunMeta = null;
    const referenceTs = _test.resolveTimelineReferenceNowTs({
      updated_at_ts: 1200,
      timeline: [{ topic: "cloudv2", ts: 1300 }],
      run: { is_active: true },
    });
    assert.equal(referenceTs, 1300);
  } finally {
    _test.state.rawState = originalRawState;
    _test.state.panelRunMeta = originalPanelRunMeta;
    Date.now = originalNow;
  }
});

test("connectivity: historical run keeps persisted timestamp as the current reference", () => {
  const originalNow = Date.now;
  Date.now = () => 5_000_000;
  try {
    const referenceTs = _test.resolveTimelineReferenceNowTs({
      updated_at_ts: 1200,
      timeline: [{ topic: "cloudv2", ts: 1300 }],
      run: { is_active: false },
    });
    assert.equal(referenceTs, 1300);
  } finally {
    Date.now = originalNow;
  }
});

test("display: compact timestamp formatting uses America/Sao_Paulo and prefers ts over stale text", () => {
  const ts = 1_735_689_600; // 2025-01-01 00:00:00 UTC
  assert.equal(_test.formatCompactDateTime(ts), "2024-12-31 21:00:00");
  assert.equal(_test.formatTimestampFromTsOrValue(ts, "2025-01-01 00:00:00", true), "2024-12-31 21:00:00");
});

test("display: status is forced to inicial when connectivity is em analise", () => {
  const item = {
    pivot_id: "PivotA",
    status: { code: "green", reason: "Conectividade dentro do esperado." },
    quality: { code: "calculating", reason: "Coletando dados para avaliar a conectividade." },
  };

  const status = _test.getDisplayStatus(item);
  assert.equal(status.code, "gray");
  assert.equal(status.label, "Inicial");
});

test("display: status keeps timeline-derived value when connectivity is not em analise", () => {
  const item = {
    pivot_id: "PivotB",
    status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
    quality: { code: "yellow", reason: "Conectividade instÃ¡vel no perÃ­odo selecionado." },
  };

  const status = _test.getDisplayStatus(item);
  assert.equal(status.code, "red");
});

test("display: quality stays em analise when median samples are not ready, even with high disconnected pct", () => {
  const pivot = {
    summary: {
      status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
      median_ready: false,
      median_sample_count: 0,
    },
  };
  const connectivitySummary = {
    disconnectedPct: 100,
    hasPrincipalPayloadInWindow: false,
    hasAuxPayloadInWindow: false,
  };

  const quality = _test.buildQualityFromConnectivity(pivot, connectivitySummary);
  assert.equal(quality.code, "calculating");
});

test("display: quality can be critical after median is ready", () => {
  const pivot = {
    summary: {
      status: { code: "red", reason: "Sem comunicaÃ§Ã£o recente." },
      median_ready: true,
      median_sample_count: 5,
      attention_disconnected_pct_threshold: 20,
      critical_disconnected_pct_threshold: 50,
    },
  };
  const connectivitySummary = {
    disconnectedPct: 80,
    hasPrincipalPayloadInWindow: true,
    hasAuxPayloadInWindow: true,
  };

  const quality = _test.buildQualityFromConnectivity(pivot, connectivitySummary);
  assert.equal(quality.code, "critical");
});

test("sorting: pivots with more samples come first", () => {
  const a = { pivot_id: "A", median_sample_count: 2, last_activity_ts: 100 };
  const b = { pivot_id: "B", median_sample_count: 9, last_activity_ts: 90 };
  const compare = _test.compareBySamplesDesc(a, b);
  assert.ok(compare > 0);
});

test("sorting: identifica pivos sem latitude e longitude", () => {
  assert.equal(_test.hasPivotCoordinates({ pivot_id: "A", latitude: 0, longitude: 0 }), false);
  assert.equal(_test.hasPivotCoordinates({ pivot_id: "B", latitude: -22.1, longitude: -45.2 }), true);
  assert.equal(_test.hasPivotCoordinates({ pivot_id: "C", latitude: null, longitude: -45.2 }), false);
  assert.equal(_test.hasPivotCoordinates({ pivot_id: "D", summary: { latitude: -22.1, longitude: -45.2 } }), true);
});

test("sorting: % conectado ordena do maior para o menor com desempate por pivô", () => {
  const pivots = [
    { pivot_id: "B" },
    { pivot_id: "A" },
    { pivot_id: "C" },
  ];
  const summaryByPivotId = {
    A: { connectedPct: 70 },
    B: { connectedPct: 70 },
    C: { connectedPct: 90 },
  };

  const ordered = [...pivots].sort((a, b) => _test.compareByConnectedPctDesc(a, b, undefined, undefined, summaryByPivotId));
  assert.deepEqual(
    ordered.map((item) => item.pivot_id),
    ["C", "A", "B"]
  );
});

test("sorting: % desconectado usa fallback 0 sem quebrar o desempate", () => {
  const pivots = [
    { pivot_id: "B" },
    { pivot_id: "A" },
    { pivot_id: "C" },
  ];
  const summaryByPivotId = {
    A: { disconnectedPct: 10 },
  };

  const ordered = [...pivots].sort((a, b) => _test.compareByDisconnectedPctDesc(a, b, undefined, undefined, summaryByPivotId));
  assert.deepEqual(
    ordered.map((item) => item.pivot_id),
    ["A", "B", "C"]
  );
});

test("sorting: percentuais em string com % também ordenam corretamente", () => {
  const pivots = [
    { pivot_id: "B" },
    { pivot_id: "A" },
    { pivot_id: "C" },
  ];
  const summaryByPivotId = {
    A: { connectedPct: "87,5%" },
    B: { connectedPct: "12%" },
    C: { connectedPct: "3%" },
  };

  const ordered = [...pivots].sort((a, b) => _test.compareByConnectedPctDesc(a, b, undefined, undefined, summaryByPivotId));
  assert.deepEqual(
    ordered.map((item) => item.pivot_id),
    ["A", "B", "C"]
  );
});

test("sorting: sample count fallback reads nested summary", () => {
  const value = _test.pivotSampleCount({ summary: { median_sample_count: 7 } });
  assert.equal(value, 7);
});

test("sorting: latência ativa prioriza maior relação respostas/solicitações", () => {
  const a = {
    pivot_id: "A",
    probe: { enabled: true, response_ratio_pct: 52.0, sent_count: 25, response_count: 13 },
    last_activity_ts: 100,
  };
  const b = {
    pivot_id: "B",
    probe: { enabled: true, response_ratio_pct: 80.0, sent_count: 5, response_count: 4 },
    last_activity_ts: 90,
  };

  const compare = _test.compareByProbeResponseRatioDesc(a, b);
  assert.ok(compare > 0);
});

test("sorting: relação respostas/solicitações é calculada quando percentual não veio no payload", () => {
  const ratio = _test.pivotProbeResponseRatioPct({
    probe: { enabled: true, sent_count: 10, response_count: 7 },
  });
  assert.equal(ratio, 70);
});

test("display: concentrador override tem prioridade na tecnologia do pivô", () => {
  const technology = _test.pivotTechnologyValue({
    is_concentrator: true,
    last_cloud2: { technology: "LTE" },
  });
  assert.equal(technology, "concentrador");
});

test("display: tecnologia do cloud2 é usada quando pivô não é concentrador", () => {
  const technology = _test.pivotTechnologyValue({
    is_concentrator: false,
    last_cloud2: { technology: "LTE CAT" },
  });
  assert.equal(technology, "LTE CAT");
});

test("display: probe response info rows include parsed modem payload fields", () => {
  const rows = _test.getProbeResponseInfoDisplayRows({
    last_response_info: {
      rssi: 20,
      technology: "LTE",
      board_timestamp_ts: 1773171016,
      operator: "Unknown Operator",
      modem_name: "AT+CGMM A7608SA-H",
      imei: "862733060786376",
      keep_alive_sec: 180,
      socket_timeout_sec: 180,
      apn_time_ms: 5000,
      apn: "APNNAME1",
      networks: 8,
      esp_temp_c: 81.7,
      firmware: "v2.8.7",
    },
  });

  assert.equal(rows.length, 13);
  assert.equal(rows.find((row) => row.label === "Modelo do modem").value, "AT+CGMM A7608SA-H");
  assert.equal(rows.find((row) => row.label === "Firmware reportado").value, "v2.8.7");
  assert.equal(rows.find((row) => row.label === "Redes configuradas").value, "LTE, Automático");
  assert.match(rows.find((row) => row.label === "Timestamp da placa").value, /\(1773171016\)$/);
});

test("display: configured networks follow the modem firmware mapping", () => {
  assert.equal(_test.formatProbeConfiguredNetworks(1), "LTE, WCDMA");
  assert.equal(_test.formatProbeConfiguredNetworks(2), "WCDMA, LTE, GSM");
  assert.equal(_test.formatProbeConfiguredNetworks(6), "GSM, LTE, WCDMA");
  assert.equal(_test.formatProbeConfiguredNetworks(7), "Automático");
  assert.equal(_test.formatProbeConfiguredNetworks(8), "LTE, Automático");
  assert.equal(_test.formatProbeConfiguredNetworks(99), "LTE, WCDMA, GSM");
});

test("quality signature: changes when session or activity changes", () => {
  const base = {
    run_id: "run-1",
    session_id: "session-1",
    last_activity_ts: 100,
    last_ping_ts: 90,
    last_cloudv2_ts: 95,
    last_cloud2: { ts: 98 },
    median_sample_count: 5,
    median_ready: true,
    status: { code: "green" },
    quality: { code: "green" },
    probe: { sent_count: 10, response_count: 9 },
  };
  const sigA = _test.buildQualitySourceSignature(base);
  const sigB = _test.buildQualitySourceSignature({ ...base, session_id: "session-2" });
  const sigC = _test.buildQualitySourceSignature({ ...base, last_activity_ts: 101 });
  assert.notEqual(sigA, sigB);
  assert.notEqual(sigA, sigC);
});

test("ui: pivot sem RSSI nao exibe o card de RSSI", () => {
  const shouldRender = _test.shouldRenderRssiPanel({
    hasRssi: false,
    rssiSeries: [],
  });
  assert.equal(shouldRender, false);
});

test("ui: pivot com RSSI exibe o card de RSSI", () => {
  const shouldRender = _test.shouldRenderRssiPanel({
    hasRssi: true,
    rssiSeries: [{ ts: 1000, rssi: 12 }],
  });
  assert.equal(shouldRender, true);
});

test("ui: grafico RSSI combina historico horario e pontos recentes", () => {
  const points = _test.collectRssiChartPoints({
    rssiHistorySeries: [
      { ts: 1000, rssi: 12 },
      { ts: 2000, rssi: 13 },
    ],
    rssiSeries: [
      { ts: 2000, rssi: 13 },
      { ts: 2100, rssi: 14 },
    ],
  });

  assert.deepEqual(points, [
    { ts: 1000, rssi: 12 },
    { ts: 2000, rssi: 13 },
    { ts: 2100, rssi: 14 },
  ]);
});

test("ui: grafico RSSI usa janela real dos pontos para preencher a largura", () => {
  const domain = _test.resolveRssiChartDomain(
    [
      { ts: 1_000, rssi: 12 },
      { ts: 1_050, rssi: 18 },
      { ts: 1_100, rssi: 16 },
    ],
    0,
    10_000
  );

  assert.deepEqual(domain, { startTs: 1_000, endTs: 1_100 });
});

test("ui: grafico RSSI cria dominio minimo para ponto unico", () => {
  const domain = _test.resolveRssiChartDomain([{ ts: 1_000, rssi: 12 }], 0, 10_000);

  assert.equal(domain.startTs, 970);
  assert.equal(domain.endTs, 1030);
});

test("ui: ordem antiga de colunas é migrada para timeline + sinal/tecnologia", () => {
  const normalized = _test.normalizePivotTableColumnOrder([
    "pivot_id",
    "status",
    "last_ping_at",
    "signal_technology",
    "firmware",
  ]);
  const timelineIndex = normalized.indexOf("timeline");
  const signalIndex = normalized.indexOf("signal");
  const technologyIndex = normalized.indexOf("technology");
  assert.ok(timelineIndex >= 0);
  assert.ok(signalIndex >= 0);
  assert.ok(technologyIndex >= 0);
});

test("ui: sinal e tecnologia usam novos campos e fallback combinado", () => {
  assert.equal(
    _test.pivotSignalValue({ signal: "21", signal_technology: "10 / LTE", last_cloud2: { rssi: "9" } }),
    "21"
  );
  assert.equal(
    _test.pivotTechnologyColumnValue({ technology: "LTE", signal_technology: "10 / GSM", last_cloud2: { technology: "EDGE" } }),
    "LTE"
  );
  assert.equal(
    _test.pivotSignalValue({ signal_technology: "18 / 4G" }),
    "18"
  );
  assert.equal(
    _test.pivotTechnologyColumnValue({ signal_technology: "18 / 4G" }),
    "4G"
  );
});

test("ui: timeline mini normaliza segmentos inválidos e mantém soma positiva", () => {
  const segments = _test.normalizeTimelineMiniSegments([
    { state: "online", ratio: 0.5 },
    { state: "offline", ratio: 0.5 },
    { state: "invalid", ratio: 0.8 },
    { state: "online", ratio: -1 },
  ]);
  assert.equal(segments.length, 2);
  const total = segments.reduce((acc, item) => acc + item.ratio, 0);
  assert.ok(total > 0.99 && total < 1.01);
});

test("ui: timeline mini derivada da conectividade espelha segmentos connected/disconnected", () => {
  const segments = _test.buildTimelineMiniSegmentsFromConnectivity(
    [
      { state: "connected", duration: 30 },
      { state: "connected", duration: 10 },
      { state: "disconnected", duration: 60 },
    ],
    100
  );

  assert.deepEqual(segments, [
    { state: "online", ratio: 0.4 },
    { state: "offline", ratio: 0.6 },
  ]);
});

test("ui: selecao multipla de pivôs remove vazios e duplicados", () => {
  assert.deepEqual(_test.normalizePivotIdList([" PivotA ", "", "PivotB", "PivotA", null]), ["PivotA", "PivotB"]);
});

test("ui: timeline mini usa fallback do payload enquanto override ainda nao chegou", () => {
  const segments = _test.resolvePivotTimelineMiniSegments({
    pivot_id: "PivotA",
    timeline_mini: [
      { state: "online", ratio: 0.25 },
      { state: "offline", ratio: 0.75 },
    ],
  });

  assert.deepEqual(segments, [
    { state: "online", ratio: 0.25 },
    { state: "offline", ratio: 0.75 },
  ]);
});

test("ui: summary cards history normaliza, ordena e filtra pontos invalidos", () => {
  const points = _test.normalizeSummaryCardsHistoryPoints([
    { ts: 200, connected_count: 8, disconnected_count: 2, initial_count: 0, total_count: 20 },
    { ts: 100, connected_count: 5, disconnected_count: 4, initial_count: 1, total_count: 20 },
    { ts: "invalid", connected_count: 3, total_count: 20 },
  ]);

  assert.equal(points.length, 2);
  assert.equal(points[0].ts, 100);
  assert.equal(points[1].connected_count, 8);
});

test("ui: connected pivots history chart model gera paths para o gráfico", () => {
  const model = _test.buildSummaryCardsHistorySvgModel([
    { ts: 100, connected_count: 4, disconnected_count: 3, initial_count: 3, total_count: 10 },
    { ts: 200, connected_count: 7, disconnected_count: 2, initial_count: 1, total_count: 10 },
    { ts: 300, connected_count: 6, disconnected_count: 4, initial_count: 0, total_count: 10 },
  ], "disconnected");

  assert.ok(model);
  assert.match(model.linePath, /^M /);
  assert.match(model.areaPath, /^M /);
  assert.equal(model.firstTs, 100);
  assert.equal(model.lastTs, 300);
});

test("ui: painel de eventos mantém apenas os 125 eventos mais recentes", () => {
  const timeline = Array.from({ length: 160 }, (_, index) => ({
    ts: 2_000 - index,
    topic: "cloudv2",
  }));

  const events = _test.getConnectivityEventsPanelCapped({ timeline });

  assert.equal(events.length, 125);
  assert.equal(events[0].ts, 2_000);
  assert.equal(events.at(-1).ts, 1_876);
});

test("ui: firmware para reset usa summary do painel e fallback da tabela", () => {
  assert.equal(
    _test.getPivotFirmwareVersionForReset({ summary: { last_cloud2: { firmware: "2.8.5" } } }),
    "2.8.5"
  );
  assert.equal(
    _test.getPivotFirmwareVersionForReset({ last_cloud2: { firmware: "2.8.6" } }),
    "2.8.6"
  );
});

test("ui: confirma ACK de reset pendente a partir do estado resumido dos pivôs", () => {
  const result = _test.collectConfirmedModemResetAcks(
    {
      PivotA: { pivotId: "PivotA", commandTs: 150 },
      PivotB: { pivotId: "PivotB", commandTs: 250 },
    },
    [
      { pivot_id: "PivotA", modem_reset: { last_ack_ts: 160 } },
      { pivot_id: "PivotB", modem_reset: { last_ack_ts: 200 } },
    ]
  );

  assert.deepEqual(result.confirmedPivotIds, ["PivotA"]);
  assert.deepEqual(Object.keys(result.remainingPendingByPivotId), ["PivotB"]);
});

test("ui: entrada de fila de descoberta separa pivot_ids por virgula ou quebra de linha", () => {
  const parsed = _test.parsePivotIdBatchInput(" PivotA_1,\nPivotB_2 ; PivotC_3  ");
  assert.deepEqual(parsed, ["PivotA_1", "PivotB_2", "PivotC_3"]);
});

test("ui: fila de descoberta atualiza localmente sem depender do refresh completo", () => {
  _test.setExpectedPivotsPendingState([
    {
      pivot_id: "PivotA_1",
      added_at: "2026-03-11 10:00:00",
      added_at_ts: 1,
      source: "ui",
    },
  ]);

  assert.deepEqual(
    _test.getExpectedPivotsPending().map((item) => item.pivot_id),
    ["PivotA_1"]
  );

  _test.removeExpectedPivotPendingState("PivotA_1");
  assert.deepEqual(_test.getExpectedPivotsPending(), []);
});

test("ui: summary cards history usa a contagem atual do card no ponto mais recente", () => {
  _test.state.pivots = [
    { pivot_id: "PivotA", status: { code: "green" }, quality: { code: "green" } },
    { pivot_id: "PivotB", status: { code: "green" }, quality: { code: "green" } },
    { pivot_id: "PivotC", status: { code: "red" }, quality: { code: "critical" } },
  ];
  _test.state.qualityOverridesByPivotId = {};
  _test.state.statusOverridesByPivotId = {};
  _test.state.rawState = { updated_at_ts: 300 };

  const currentCounts = _test.computeSummaryCardCountsFromPivots(_test.state.pivots);
  const points = _test.buildSummaryCardsDisplayHistoryPoints([
    { ts: 100, connected_count: 6, disconnected_count: 1, initial_count: 0, total_count: 7 },
    { ts: 200, connected_count: 6, disconnected_count: 1, initial_count: 0, total_count: 7 },
  ], currentCounts);

  assert.equal(points.length, 2);
  assert.equal(points.at(-1).connected_count, 2);
  assert.equal(points.at(-1).disconnected_count, 1);
  assert.equal(points.at(-1).total_count, 3);
  assert.equal(points.at(-1).ts, 300);
});

test("ui: summary cards history nao injeta contagem crua antes da qualidade carregar", () => {
  const points = _test.buildSummaryCardsDisplayHistoryPoints([
    { ts: 100, connected_count: 55, disconnected_count: 35, initial_count: 9, total_count: 99 },
    { ts: 200, connected_count: 55, disconnected_count: 35, initial_count: 9, total_count: 99 },
  ], null);

  assert.equal(points.at(-1).connected_count, 55);
  assert.equal(points.at(-1).disconnected_count, 35);
  assert.equal(points.at(-1).total_count, 99);
});

test("ui: qualidade completa exige override de todos os pivos", () => {
  const pivots = [{ pivot_id: "A" }, { pivot_id: "B" }];
  const maps = {
    qualityOverridesByPivotId: { A: {}, B: {} },
    statusOverridesByPivotId: { A: {}, B: {} },
    connectivitySummaryByPivotId: { A: {}, B: {} },
    connectivityMiniSegmentsByPivotId: { A: [] },
  };

  assert.equal(_test.hasCompleteQualityOverridesForPivots(pivots, maps), false);
  maps.connectivityMiniSegmentsByPivotId.B = [];
  assert.equal(_test.hasCompleteQualityOverridesForPivots(pivots, maps), true);
});

test("ui: summary cards history remove outlier recente de warm-up", () => {
  const currentCounts = {
    historyPoint: {
      ts: 7_200,
      total_count: 99,
      connected_count: 60,
      disconnected_count: 33,
      initial_count: 6,
      quality_green_count: 55,
      quality_calculating_count: 6,
      quality_yellow_count: 3,
      quality_critical_count: 35,
    },
  };

  const points = _test.buildSummaryCardsDisplayHistoryPoints(
    [
      {
        ts: 3_600,
        total_count: 99,
        connected_count: 61,
        disconnected_count: 37,
        initial_count: 1,
        quality_green_count: 1,
        quality_calculating_count: 1,
        quality_yellow_count: 53,
        quality_critical_count: 44,
      },
      {
        ts: 7_200,
        total_count: 99,
        connected_count: 61,
        disconnected_count: 37,
        initial_count: 1,
        quality_green_count: 1,
        quality_calculating_count: 1,
        quality_yellow_count: 53,
        quality_critical_count: 44,
      },
    ],
    currentCounts,
    { payload: { updated_at_ts: 7_200, bucket_sec: 3_600 } }
  );

  assert.equal(points[0].quality_green_count, 55);
  assert.equal(points[0].quality_yellow_count, 3);
  assert.equal(points.at(-1).quality_green_count, 55);
  assert.equal(points.at(-1).quality_critical_count, 35);
});
