// Dashboard: loads the JSON exported by `python -m nyc311 build-site` and
// renders Vega-Lite charts. Colors are role tokens resolved per color scheme.

const DARK = window.matchMedia("(prefers-color-scheme: dark)").matches;

const THEME = DARK
  ? {
      text: "#c3c2b7", muted: "#96958c", grid: "#33332f", surface: "#1a1a19",
      series: ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181"],
      // Ordinal blue ramp for age buckets (young -> old), dark steps.
      ramp: ["#184f95", "#256abf", "#3987e5", "#6da7ec", "#9ec5f4", "#cde2fb"],
    }
  : {
      text: "#52514e", muted: "#77766f", grid: "#e2e1dc", surface: "#fcfcfb",
      series: ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"],
      ramp: ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281", "#0d366b"],
    };

const BOROUGHS = ["BROOKLYN", "QUEENS", "MANHATTAN", "BRONX", "STATEN ISLAND"];
const AGE_BUCKETS = ["<1d", "1-3d", "3-7d", "7-14d", "14-30d", "30d+"];

const vlConfig = {
  background: null,
  font: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
  axis: {
    labelColor: THEME.text, titleColor: THEME.text, gridColor: THEME.grid,
    domainColor: THEME.grid, tickColor: THEME.grid, labelFontSize: 12, titleFontSize: 12,
    titleFontWeight: "normal",
  },
  legend: { labelColor: THEME.text, titleColor: THEME.text, orient: "top", labelFontSize: 12 },
  view: { stroke: null },
  bar: { cornerRadiusEnd: 4 },
  line: { strokeWidth: 2 },
};

const fmt = new Intl.NumberFormat("en-US");

async function load(name) {
  const res = await fetch(`data/${name}.json`);
  if (!res.ok) throw new Error(`${name}: HTTP ${res.status}`);
  return res.json();
}

function embed(id, spec) {
  return vegaEmbed(`#${id}`, { $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    config: vlConfig, width: "container", ...spec }, { actions: false });
}

function renderTiles(meta, resolution) {
  const totalClosed = resolution.reduce((s, r) => s + r.n_closed, 0);
  const tiles = [
    ["Requests in window", fmt.format(meta.n_requests)],
    ["Open now", fmt.format(meta.n_open)],
    ["Closed (agencies shown)", fmt.format(totalClosed)],
    ["Window start", meta.window_start],
  ];
  document.getElementById("tiles").innerHTML = tiles
    .map(([label, value]) =>
      `<div class="tile"><div class="label">${label}</div><div class="value">${value}</div></div>`)
    .join("");
}

function renderDaily(rows) {
  // The latest created day is partial (the source lags by about a day), so it
  // is left off the chart instead of drawing a misleading drop to zero.
  const lastDay = rows.reduce((m, r) => (r.day > m ? r.day : m), "");
  return embed("chart-daily", {
    height: 260,
    data: { values: rows.filter((r) => BOROUGHS.includes(r.borough) && r.day !== lastDay) },
    mark: { type: "line", point: false },
    encoding: {
      x: { field: "day", type: "temporal", title: null, axis: { format: "%b %d", grid: false } },
      y: { field: "n", type: "quantitative", title: "requests" },
      color: {
        field: "borough", type: "nominal", title: null,
        scale: { domain: BOROUGHS, range: THEME.series },
      },
      tooltip: [
        { field: "day", type: "temporal", format: "%a %b %d" },
        { field: "borough" },
        { field: "n", title: "requests", format: "," },
      ],
    },
  });
}

function renderTypes(rows) {
  return embed("chart-types", {
    height: { step: 24 },
    data: { values: rows },
    mark: { type: "bar", color: THEME.series[0] },
    encoding: {
      y: { field: "complaint_type", type: "nominal", sort: "-x", title: null },
      x: { field: "n", type: "quantitative", title: "requests" },
      tooltip: [{ field: "complaint_type", title: "type" }, { field: "n", format: "," }],
    },
  });
}

function renderResolution(rows) {
  // One row per agency with two measures -> long format for a dot plot.
  const long = rows.flatMap((r) => [
    { agency: r.agency, stat: "median", hours: r.median_hours, n: r.n_closed },
    { agency: r.agency, stat: "p90", hours: r.p90_hours, n: r.n_closed },
  ]);
  const order = rows.map((r) => r.agency);
  return embed("chart-resolution", {
    height: { step: 24 },
    data: { values: long },
    layer: [
      {
        mark: { type: "rule", color: THEME.grid, strokeWidth: 2 },
        encoding: {
          y: { field: "agency", type: "nominal", sort: order, title: null },
          x: { aggregate: "min", field: "hours", type: "quantitative" },
          x2: { aggregate: "max", field: "hours" },
        },
      },
      {
        mark: { type: "point", filled: true, size: 90, opacity: 1, stroke: THEME.surface, strokeWidth: 2 },
        encoding: {
          y: { field: "agency", type: "nominal", sort: order, title: null },
          x: { field: "hours", type: "quantitative", title: "hours from created to closed (log scale)",
               scale: { type: "log" } },
          color: { field: "stat", type: "nominal", title: null,
                   scale: { domain: ["median", "p90"], range: THEME.series.slice(0, 2) } },
          tooltip: [
            { field: "agency" }, { field: "stat" },
            { field: "hours", format: ",.1f" }, { field: "n", title: "closed requests", format: "," },
          ],
        },
      },
    ],
  });
}

function renderBacklog(rows) {
  const totals = {};
  rows.forEach((r) => { totals[r.agency] = (totals[r.agency] || 0) + r.n; });
  const agencies = Object.keys(totals).sort((a, b) => totals[b] - totals[a]).slice(0, 12);
  return embed("chart-backlog", {
    height: { step: 24 },
    data: { values: rows.filter((r) => agencies.includes(r.agency)) },
    mark: { type: "bar", stroke: THEME.surface, strokeWidth: 2, cornerRadiusEnd: 0 },
    encoding: {
      y: { field: "agency", type: "nominal", sort: agencies, title: null },
      x: { field: "n", type: "quantitative", title: "open requests", stack: "zero" },
      color: { field: "age_bucket", type: "ordinal", title: "age",
               scale: { domain: AGE_BUCKETS, range: THEME.ramp } },
      order: { field: "bucket_order", type: "quantitative" },
      tooltip: [{ field: "agency" }, { field: "age_bucket", title: "age" },
                { field: "n", title: "open", format: "," }],
    },
  });
}

function renderQuality(report) {
  const sub = document.getElementById("quality-sub");
  const body = document.querySelector("#quality-table tbody");
  if (!report) {
    sub.textContent = "No quality report was produced for this build.";
    return;
  }
  sub.textContent = `Overall: ${report.overall}. Run ${report.run_id}.`;
  body.innerHTML = "";
  for (const c of report.checks) {
    const tr = document.createElement("tr");
    const cells = [c.name, c.status, c.detail];
    cells.forEach((text, i) => {
      const td = document.createElement("td");
      td.textContent = text;
      if (i === 1) td.className = `status status-${text}`;
      tr.appendChild(td);
    });
    body.appendChild(tr);
  }
}

async function main() {
  const [meta, daily, types, resolution, backlog] = await Promise.all(
    ["meta", "daily_by_borough", "top_complaint_types", "resolution_by_agency", "backlog_by_agency"]
      .map(load),
  );
  const quality = await load("quality").catch(() => null);
  document.getElementById("asof").textContent =
    `Data as of ${meta.data_as_of} (latest request created, NYC time); ` +
    `source last updated ${meta.source_updated_at_utc} UTC; built ${meta.generated_at_utc}.`;
  renderTiles(meta, resolution);
  renderQuality(quality);
  await Promise.all([renderDaily(daily), renderTypes(types), renderResolution(resolution),
    renderBacklog(backlog)]);
}

main().catch((err) => {
  document.getElementById("asof").textContent = `Failed to load data: ${err.message}`;
});
