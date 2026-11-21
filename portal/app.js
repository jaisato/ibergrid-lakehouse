/* IberGrid Lakehouse Interactive Observatory (static R2/Pages + live FastAPI /portal) */
(function () {
  "use strict";

  const MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];

  const charts = {};
  const state = {
    summary: null,
    windows: null,
    daily: null,
    blackout: null,
    systems: null,
    benchmarks: null,
    apiLive: false,
  };

  function fmtNum(val, digits = 0) {
    if (val === null || val === undefined || Number.isNaN(Number(val))) return "—";
    return Number(val).toLocaleString("en-US", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }

  function fmtBytes(bytes) {
    if (!bytes) return "0 B";
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KiB`;
    if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(2)} MiB`;
    return `${(bytes / 1024 ** 3).toFixed(2)} GiB`;
  }

  async function fetchJson(url) {
    const resp = await fetch(url, { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status} for ${url}`);
    return resp.json();
  }

  function upsertChart(id, config) {
    const canvas = document.getElementById(id);
    if (!canvas || typeof Chart === "undefined") return;
    if (charts[id]) charts[id].destroy();
    charts[id] = new Chart(canvas.getContext("2d"), config);
  }

  function chartBaseOptions(yTitle) {
    return {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { labels: { color: "#cbd5e1" } },
      },
      scales: {
        x: { ticks: { color: "#94a3b8" }, grid: { color: "rgba(148,163,184,0.12)" } },
        y: {
          ticks: { color: "#94a3b8" },
          grid: { color: "rgba(148,163,184,0.12)" },
          title: yTitle ? { display: true, text: yTitle, color: "#94a3b8" } : undefined,
        },
      },
    };
  }

  function setupTabs() {
    const buttons = document.querySelectorAll(".tab");
    buttons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const target = btn.dataset.tab;
        buttons.forEach((b) => {
          const active = b === btn;
          b.classList.toggle("active", active);
          b.setAttribute("aria-selected", String(active));
        });
        document.querySelectorAll(".panel").forEach((panel) => {
          const match = panel.id === `panel-${target}`;
          panel.classList.toggle("active", match);
          panel.hidden = !match;
        });
      });
    });
  }

  function renderHeaderAndKpis() {
    const s = state.summary;
    if (!s) return;

    const cov = document.getElementById("coverage-badge");
    if (cov && s.coverage_start && s.coverage_end) {
      cov.textContent = `${s.coverage_start} → ${s.coverage_end} (${fmtNum(s.total_days)} days)`;
    }
    const upd = document.getElementById("updated-banner");
    if (upd) {
      upd.textContent = `Last updated: ${s.exported_at_utc} · ${s.source_attribution}`;
    }

    const inv = s.metadata_inventory || {};
    document.getElementById("kpi-rows").textContent = fmtNum(inv.total_records || 0);
    document.getElementById("kpi-footprint").textContent =
      `${inv.total_tables || 0} Iceberg tables · ${fmtBytes(inv.total_data_bytes || 0)} Parquet (0 B metadata scan)`;

    const annual = s.annual_mibel || [];
    const firstYr = annual[0];
    const latestYr = annual[annual.length - 1];
    if (latestYr) {
      document.getElementById("kpi-zneg").textContent = `${fmtNum(latestYr.zero_or_negative_es_hours)} h`;
      if (firstYr && firstYr.zero_or_negative_es_hours > 0) {
        const mult = (latestYr.zero_or_negative_es_hours / firstYr.zero_or_negative_es_hours).toFixed(1);
        document.getElementById("kpi-zneg-sub").textContent =
          `${mult}× vs ${firstYr.year} (${fmtNum(firstYr.zero_or_negative_es_hours)} h)`;
      }
    }
    const totalDec = annual.reduce((acc, r) => acc + (r.decoupled_hours || 0), 0);
    document.getElementById("kpi-decoupled").textContent = `${fmtNum(totalDec)} h`;
  }

  function heatColor(price, minP, maxP) {
    const ratio = maxP > minP ? Math.min(1, Math.max(0, (price - minP) / (maxP - minP))) : 0.5;
    if (ratio < 0.35) return "rgba(16, 185, 129, 0.72)";
    if (ratio < 0.65) return "rgba(56, 189, 248, 0.55)";
    if (ratio < 0.85) return "rgba(245, 158, 11, 0.68)";
    return "rgba(244, 63, 94, 0.74)";
  }

  function renderDuckCurve() {
    const items = state.windows?.items || [];
    if (!items.length) return;

    const monthSel = document.getElementById("month-select");
    const daySel = document.getElementById("daytype-select");
    if (!monthSel.options.length) {
      MONTHS.forEach((name, idx) => {
        const opt = document.createElement("option");
        opt.value = String(idx + 1);
        opt.textContent = name;
        if (idx === 4) opt.selected = true; // May showcases the deepest Iberian solar duck curve
        monthSel.appendChild(opt);
      });
      monthSel.addEventListener("change", renderDuckCurve);
      daySel.addEventListener("change", renderDuckCurve);
    }

    const month = Number(monthSel.value);
    const dayType = daySel.value;
    const filtered = items
      .filter((r) => r.calendar_month === month && r.day_type === dayType)
      .sort((a, b) => a.madrid_hour - b.madrid_hour);

    upsertChart("chart-duck-curve", {
      type: "line",
      data: {
        labels: filtered.map((r) => `${String(r.madrid_hour).padStart(2, "0")}:00`),
        datasets: [
          {
            label: "Mean ES Price (€/MWh)",
            data: filtered.map((r) => r.price_es_mean_eur_mwh),
            borderColor: "#38bdf8",
            backgroundColor: "rgba(56, 189, 248, 0.16)",
            fill: true,
            tension: 0.25,
            pointBackgroundColor: filtered.map((r) => (r.is_optimal_4h_window ? "#f59e0b" : "#38bdf8")),
            pointRadius: filtered.map((r) => (r.is_optimal_4h_window ? 6 : 3)),
          },
          {
            label: "p10 (€/MWh)",
            data: filtered.map((r) => r.price_es_p10_eur_mwh),
            borderColor: "#10b981",
            borderDash: [5, 5],
            tension: 0.25,
            pointRadius: 0,
          },
          {
            label: "p90 (€/MWh)",
            data: filtered.map((r) => r.price_es_p90_eur_mwh),
            borderColor: "#f43f5e",
            borderDash: [5, 5],
            tension: 0.25,
            pointRadius: 0,
          },
          {
            label: "≤ 0 €/MWh Share (%)",
            data: filtered.map((r) => r.zero_or_negative_es_pct),
            borderColor: "#a78bfa",
            yAxisID: "y1",
            tension: 0.25,
            pointRadius: 2,
          },
        ],
      },
      options: {
        ...chartBaseOptions("€/MWh"),
        scales: {
          ...chartBaseOptions("€/MWh").scales,
          y1: {
            position: "right",
            min: 0,
            max: 100,
            ticks: { color: "#a78bfa" },
            grid: { drawOnChartArea: false },
            title: { display: true, text: "% Hours ≤ 0 €/MWh", color: "#a78bfa" },
          },
        },
      },
    });

    // Render 12x24 heatmap for chosen day_type
    const subset = items.filter((r) => r.day_type === dayType);
    const prices = subset.map((r) => r.price_es_mean_eur_mwh);
    const minP = Math.min(...prices);
    const maxP = Math.max(...prices);
    const byKey = new Map(subset.map((r) => [`${r.calendar_month}-${r.madrid_hour}`, r]));

    const tbl = document.createElement("table");
    tbl.className = "heatmap-table";
    const thead = document.createElement("thead");
    const hrow = document.createElement("tr");
    const corner = document.createElement("th");
    corner.textContent = "Month";
    hrow.appendChild(corner);
    for (let h = 0; h < 24; h++) {
      const th = document.createElement("th");
      th.textContent = String(h).padStart(2, "0");
      hrow.appendChild(th);
    }
    thead.appendChild(hrow);
    tbl.appendChild(thead);

    const tbody = document.createElement("tbody");
    for (let m = 1; m <= 12; m++) {
      const tr = document.createElement("tr");
      const mth = document.createElement("th");
      mth.textContent = MONTHS[m - 1].slice(0, 3);
      tr.appendChild(mth);
      for (let h = 0; h < 24; h++) {
        const td = document.createElement("td");
        const cell = byKey.get(`${m}-${h}`);
        if (cell) {
          td.className = `heatmap-cell${cell.is_optimal_4h_window ? " optimal" : ""}`;
          td.style.background = heatColor(cell.price_es_mean_eur_mwh, minP, maxP);
          td.textContent = cell.price_es_mean_eur_mwh.toFixed(0);
          td.title =
            `${MONTHS[m - 1]} ${String(h).padStart(2, "0")}:00 (${dayType}) — ` +
            `mean ${cell.price_es_mean_eur_mwh.toFixed(2)} €/MWh, ` +
            `≤0 €: ${cell.zero_or_negative_es_pct.toFixed(1)}%` +
            (cell.is_optimal_4h_window ? " [OPTIMAL 4H WINDOW]" : "");
        } else {
          td.textContent = "—";
        }
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    tbl.appendChild(tbody);
    const container = document.getElementById("heatmap-container");
    container.replaceChildren(tbl);
  }

  function renderAnnualAndDailyMibel() {
    const annual = state.summary?.annual_mibel || [];
    const tbody = document.querySelector("#table-annual-mibel tbody");
    tbody.replaceChildren();
    annual.forEach((r) => {
      const tr = document.createElement("tr");
      const cells = [
        r.year,
        fmtNum(r.days),
        fmtNum(r.price_es_mean_eur_mwh, 2),
        fmtNum(r.price_pt_mean_eur_mwh, 2),
        fmtNum(r.zero_or_negative_es_hours),
        fmtNum(r.decoupled_hours),
        r.peninsular_renewable_share_pct !== null ? `${fmtNum(r.peninsular_renewable_share_pct, 1)}%` : "—",
      ];
      cells.forEach((val) => {
        const td = document.createElement("td");
        td.textContent = String(val);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    upsertChart("chart-annual-mibel", {
      type: "bar",
      data: {
        labels: annual.map((r) => String(r.year)),
        datasets: [
          {
            label: "≤ 0 €/MWh Hours (ES)",
            data: annual.map((r) => r.zero_or_negative_es_hours),
            backgroundColor: "rgba(16, 185, 129, 0.72)",
            yAxisID: "y",
          },
          {
            label: "Mean ES Price (€/MWh)",
            data: annual.map((r) => r.price_es_mean_eur_mwh),
            type: "line",
            borderColor: "#f59e0b",
            backgroundColor: "#f59e0b",
            yAxisID: "y1",
            tension: 0.2,
          },
        ],
      },
      options: {
        ...chartBaseOptions("Hours ≤ 0 €/MWh"),
        scales: {
          ...chartBaseOptions("Hours ≤ 0 €/MWh").scales,
          y1: {
            position: "right",
            ticks: { color: "#f59e0b" },
            grid: { drawOnChartArea: false },
            title: { display: true, text: "Mean ES €/MWh", color: "#f59e0b" },
          },
        },
      },
    });

    const yearSel = document.getElementById("year-filter");
    const drawDaily = () => {
      const allDaily = state.daily?.items || [];
      const yr = yearSel.value;
      const rows = yr === "all" ? allDaily : allDaily.filter((r) => String(r.delivery_date).startsWith(yr));
      upsertChart("chart-daily-mibel", {
        type: "line",
        data: {
          labels: rows.map((r) => r.delivery_date),
          datasets: [
            {
              label: "Daily Mean ES (€/MWh)",
              data: rows.map((r) => r.price_es_mean_eur_mwh),
              borderColor: "#38bdf8",
              pointRadius: 0,
              borderWidth: 1.5,
            },
            {
              label: "Zero/Negative Hours",
              data: rows.map((r) => r.zero_or_negative_es_hours),
              borderColor: "#10b981",
              yAxisID: "y1",
              pointRadius: 0,
              borderWidth: 1.3,
            },
          ],
        },
        options: {
          ...chartBaseOptions("€/MWh"),
          scales: {
            ...chartBaseOptions("€/MWh").scales,
            y1: {
              position: "right",
              min: 0,
              max: 24,
              ticks: { color: "#10b981" },
              grid: { drawOnChartArea: false },
              title: { display: true, text: "Hours ≤ 0 €/MWh", color: "#10b981" },
            },
          },
        },
      });
    };
    yearSel.addEventListener("change", drawDaily);
    drawDaily();
  }

  function renderBlackoutAndSpread() {
    const daily = state.blackout?.daily || [];
    const hourly = state.blackout?.hourly || [];

    const kpiBox = document.getElementById("blackout-kpis");
    kpiBox.replaceChildren();
    daily.forEach((d) => {
      const card = document.createElement("article");
      card.className = "kpi-card";
      const label = document.createElement("span");
      label.className = "kpi-label";
      label.textContent = d.delivery_date === "2025-04-28" ? `${d.delivery_date} · BLACKOUT DAY` : d.delivery_date;
      const val = document.createElement("strong");
      val.className = "kpi-value";
      val.textContent = `${fmtNum((d.peninsular_demand_mwh || 0) / 1000, 1)} GWh`;
      const sub = document.createElement("span");
      sub.className = "kpi-sub";
      sub.textContent =
        `ES ${fmtNum(d.price_es_mean_eur_mwh, 2)} € · PT ${fmtNum(d.price_pt_mean_eur_mwh, 2)} € · ` +
        `${d.decoupled_hours} decoupled h`;
      card.append(label, val, sub);
      kpiBox.appendChild(card);
    });

    upsertChart("chart-blackout", {
      type: "line",
      data: {
        labels: hourly.map((r) => `${r.delivery_date.slice(5)} ${String(r.madrid_hour).padStart(2, "0")}h`),
        datasets: [
          {
            label: "Peninsular Demand (MW)",
            data: hourly.map((r) => r.peninsular_demand_mw),
            borderColor: "#f43f5e",
            backgroundColor: "rgba(244, 63, 94, 0.14)",
            fill: true,
            yAxisID: "y",
            tension: 0.2,
            pointRadius: 1.5,
          },
          {
            label: "ES Price (€/MWh)",
            data: hourly.map((r) => r.price_es_eur_mwh),
            borderColor: "#38bdf8",
            yAxisID: "y1",
            tension: 0.2,
            pointRadius: 1.5,
          },
          {
            label: "PT Price (€/MWh)",
            data: hourly.map((r) => r.price_pt_eur_mwh),
            borderColor: "#f59e0b",
            borderDash: [4, 4],
            yAxisID: "y1",
            tension: 0.2,
            pointRadius: 1.5,
          },
        ],
      },
      options: {
        ...chartBaseOptions("Demand (MW)"),
        scales: {
          ...chartBaseOptions("Demand (MW)").scales,
          y1: {
            position: "right",
            ticks: { color: "#38bdf8" },
            grid: { drawOnChartArea: false },
            title: { display: true, text: "Price (€/MWh)", color: "#38bdf8" },
          },
        },
      },
    });

    const allDaily = state.daily?.items || [];
    upsertChart("chart-spread", {
      type: "bar",
      data: {
        labels: allDaily.map((r) => r.delivery_date),
        datasets: [
          {
            label: "Daily Mean |ES − PT| Spread (€/MWh)",
            data: allDaily.map((r) => r.spread_abs_mean_eur_mwh),
            backgroundColor: "rgba(167, 139, 250, 0.65)",
          },
        ],
      },
      options: chartBaseOptions("|ES − PT| €/MWh"),
    });
  }

  function renderRealtimeChart(rt) {
    if (!rt) return;
    const meta = document.getElementById("realtime-meta");
    meta.textContent =
      `Trailing ${rt.hours}h up to ${rt.updated_at_utc} — ` +
      `batch points: ${fmtNum(rt.batch_points)}, stream points: ${fmtNum(rt.stream_points)}`;
    const items = rt.items || [];
    upsertChart("chart-realtime", {
      type: "line",
      data: {
        labels: items.map((r) => String(r.ts_utc).slice(11, 16)),
        datasets: [
          {
            label: "Real Demand (MW)",
            data: items.map((r) => r.demand_real_mw),
            borderColor: "#38bdf8",
            pointRadius: 1.5,
          },
          {
            label: "Forecast Demand (MW)",
            data: items.map((r) => r.demand_forecast_mw),
            borderColor: "#f59e0b",
            borderDash: [5, 5],
            pointRadius: 0,
          },
        ],
      },
      options: chartBaseOptions("National Demand (MW)"),
    });
  }

  function renderSystems() {
    const overview = state.systems?.overview || [];
    const monthly = state.systems?.monthly || [];
    const grid = document.getElementById("systems-cards");
    grid.replaceChildren();

    overview.forEach((sys) => {
      const card = document.createElement("article");
      const cls = sys.electric_system === "peninsular"
        ? "system-card peninsular"
        : !sys.ts_utc_verified
          ? "system-card unverified"
          : "system-card";
      card.className = cls;

      const head = document.createElement("div");
      head.className = "system-title";
      const h3 = document.createElement("h3");
      h3.textContent = sys.electric_system;
      const badge = document.createElement("span");
      badge.className = sys.ts_utc_verified ? "badge badge-ok" : "badge badge-warn";
      badge.textContent = sys.ts_utc_verified ? sys.system_scope : "⚠ Unverified UTC clock";
      head.append(h3, badge);
      card.appendChild(head);

      const rows = [
        ["Scope", sys.system_scope],
        ["Avg Daily Demand", `${fmtNum(sys.avg_daily_demand_mwh / 1000, 2)} GWh`],
        ["Peak Demand", `${fmtNum(sys.peak_demand_mw, 0)} MW`],
        ["Avg Renewable", sys.avg_renewable_share_pct !== null ? `${fmtNum(sys.avg_renewable_share_pct, 1)}%` : "—"],
        ["Coverage", `${fmtNum(sys.days)} days`],
      ];
      rows.forEach(([k, v]) => {
        const div = document.createElement("div");
        div.className = "system-stat";
        const lk = document.createElement("span");
        lk.textContent = k;
        const lv = document.createElement("strong");
        lv.textContent = v;
        div.append(lk, lv);
        card.appendChild(div);
      });
      grid.appendChild(card);
    });

    const months = [...new Set(monthly.map((r) => r.month))].sort();
    const palette = {
      peninsular: "#38bdf8",
      canarias: "#f59e0b",
      baleares: "#10b981",
      melilla: "#a78bfa",
      ceuta: "#f43f5e",
    };
    const datasets = overview.map((sys) => {
      const byM = new Map(
        monthly.filter((r) => r.electric_system === sys.electric_system).map((r) => [r.month, r.avg_renewable_share_pct])
      );
      return {
        label: sys.ts_utc_verified ? sys.electric_system : `${sys.electric_system} (unverified clock)`,
        data: months.map((m) => (byM.has(m) ? byM.get(m) : null)),
        borderColor: palette[sys.electric_system] || "#cbd5e1",
        tension: 0.25,
        pointRadius: 1,
      };
    });

    upsertChart("chart-systems", {
      type: "line",
      data: { labels: months, datasets },
      options: chartBaseOptions("Renewable Share (%)"),
    });

    renderRealtimeChart(state.summary?.realtime_demand);
  }

  function renderBenchmarksAndChaos() {
    const b = state.benchmarks;
    const invItems = state.summary?.metadata_inventory?.items || [];

    const invBody = document.querySelector("#table-inventory tbody");
    invBody.replaceChildren();
    invItems.forEach((item) => {
      const tr = document.createElement("tr");
      [item.table, fmtNum(item.total_records), fmtNum(item.data_files), fmtBytes(item.total_data_bytes)].forEach((v) => {
        const td = document.createElement("td");
        td.textContent = String(v);
        tr.appendChild(td);
      });
      invBody.appendChild(tr);
    });

    const q = state.summary?.quarantine_summary || { total_rejected_records: 0, by_rule: [] };
    document.getElementById("quarantine-note").textContent =
      `Live quarantine.rejected_records rows: ${fmtNum(q.total_rejected_records)} (clean backfilled baseline; chaos proofs run in isolated throwaway compose stacks).`;

    if (!b) return;

    const qRows = b.query_benchmarks || [];
    const compacted = qRows.filter((r) => r.layout === "compacted_iceberg");
    const qIds = [...new Set(compacted.map((r) => r.query_id))];
    const duckLat = qIds.map((id) => compacted.find((r) => r.query_id === id && r.engine === "duckdb")?.timing.p50_ms || 0);
    const polLat = qIds.map((id) => compacted.find((r) => r.query_id === id && r.engine === "polars")?.timing.p50_ms || 0);
    const duckRss = qIds.map((id) => compacted.find((r) => r.query_id === id && r.engine === "duckdb")?.timing.peak_rss_mib || 0);
    const polRss = qIds.map((id) => compacted.find((r) => r.query_id === id && r.engine === "polars")?.timing.peak_rss_mib || 0);

    upsertChart("chart-bench-latency", {
      type: "bar",
      data: {
        labels: qIds,
        datasets: [
          { label: "DuckDB 2.0 (compacted Iceberg)", data: duckLat, backgroundColor: "rgba(56, 189, 248, 0.78)" },
          { label: "Polars 2.1 (compacted Iceberg)", data: polLat, backgroundColor: "rgba(167, 139, 250, 0.78)" },
        ],
      },
      options: chartBaseOptions("p50 Latency (ms)"),
    });

    upsertChart("chart-bench-rss", {
      type: "bar",
      data: {
        labels: qIds,
        datasets: [
          { label: "DuckDB 2.0 Peak RSS (MiB)", data: duckRss, backgroundColor: "rgba(16, 185, 129, 0.78)" },
          { label: "Polars 2.1 Peak RSS (MiB)", data: polRss, backgroundColor: "rgba(245, 158, 11, 0.78)" },
        ],
      },
      options: chartBaseOptions("Isolated VmHWM (MiB)"),
    });

    const chaosBody = document.querySelector("#table-chaos tbody");
    chaosBody.replaceChildren();
    (b.chaos_proofs || []).forEach((c) => {
      const tr = document.createElement("tr");
      [c.suite, c.scenario, c.target, c.blocked_or_recovered].forEach((v) => {
        const td = document.createElement("td");
        td.textContent = String(v);
        tr.appendChild(td);
      });
      chaosBody.appendChild(tr);
    });

    const costBody = document.querySelector("#table-cost tbody");
    costBody.replaceChildren();
    (b.infra_scenarios || []).forEach((sc) => {
      const tr = document.createElement("tr");
      [
        sc.name,
        `€${sc.compute_eur_month.toFixed(2)}`,
        `€${sc.storage_eur_month.toFixed(2)}`,
        `€${sc.egress_eur_month.toFixed(2)}`,
        `€${sc.total_eur_month.toFixed(2)}`,
      ].forEach((v) => {
        const td = document.createElement("td");
        td.textContent = String(v);
        tr.appendChild(td);
      });
      costBody.appendChild(tr);
    });
  }

  async function probeLiveApi() {
    try {
      const h = await fetchJson("/health");
      if (h && h.status === "ok") {
        state.apiLive = true;
        const badge = document.getElementById("api-status-badge");
        badge.className = "badge badge-ok";
        badge.textContent = "Live FastAPI + DuckDB";
        const docsLink = document.getElementById("api-docs-link");
        if (docsLink) docsLink.hidden = false;
        const refreshBtn = document.getElementById("refresh-realtime-btn");
        if (refreshBtn) {
          refreshBtn.hidden = false;
          refreshBtn.addEventListener("click", async () => {
            refreshBtn.disabled = true;
            try {
              const liveRt = await fetchJson("/v1/demand/realtime?hours=24");
              renderRealtimeChart(liveRt);
            } finally {
              refreshBtn.disabled = false;
            }
          });
        }
      }
    } catch (_err) {
      // Static-only hosting (Cloudflare R2 / Pages) without API backend: remain silently static.
    }
  }

  async function init() {
    setupTabs();
    const results = await Promise.allSettled([
      fetchJson("./data/summary.json"),
      fetchJson("./data/optimal_windows.json"),
      fetchJson("./data/daily_market.json"),
      fetchJson("./data/blackout_2025_04_28.json"),
      fetchJson("./data/systems_daily.json"),
      fetchJson("./data/benchmarks.json"),
    ]);
    state.summary = results[0].status === "fulfilled" ? results[0].value : null;
    state.windows = results[1].status === "fulfilled" ? results[1].value : null;
    state.daily = results[2].status === "fulfilled" ? results[2].value : null;
    state.blackout = results[3].status === "fulfilled" ? results[3].value : null;
    state.systems = results[4].status === "fulfilled" ? results[4].value : null;
    state.benchmarks = results[5].status === "fulfilled" ? results[5].value : null;

    renderHeaderAndKpis();
    renderDuckCurve();
    renderAnnualAndDailyMibel();
    renderBlackoutAndSpread();
    renderSystems();
    renderBenchmarksAndChaos();
    await probeLiveApi();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
