/* The page's charts and figures, drawn from results.json.

   Nothing is computed here that a report did not already compute: this file reads the one
   data file `verdict site-export` writes from the committed reports, formats it, and draws
   it as SVG. No library, no off-origin request, nothing inline, so the page runs under the
   content security policy in staticwebapp.config.json. */

"use strict";

const SVG = "http://www.w3.org/2000/svg";

/* ---------------------------------------------------------------- small helpers */

function $(id) {
  return document.getElementById(id);
}

function el(tag, attrs, text) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) node.setAttribute(key, value);
  if (text !== undefined) node.textContent = text;
  return node;
}

function sv(tag, attrs, text) {
  const node = document.createElementNS(SVG, tag);
  for (const [key, value] of Object.entries(attrs || {})) node.setAttribute(key, value);
  if (text !== undefined) node.textContent = text;
  return node;
}

function svgRoot(width, height, label) {
  return sv("svg", {
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    "aria-label": label,
    preserveAspectRatio: "xMidYMid meet",
  });
}

const number = new Intl.NumberFormat("en-CA");

function fmt(value, digits) {
  return value.toLocaleString("en-CA", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function ms(value) {
  if (value < 0.1) return `${fmt(value, 3)} ms`;
  if (value < 1) return `${fmt(value, 2)} ms`;
  return `${fmt(value, 1)} ms`;
}

function range(interval, digits, unit) {
  const u = unit || "";
  return `${fmt(interval.low, digits)} to ${fmt(interval.high, digits)}${u}`;
}

function dollars(value) {
  return `$${fmt(value, 2)}`;
}

function millions(value) {
  return `$${fmt(value / 1e6, 1)} million`;
}

function wholeDollars(value) {
  return `$${number.format(Math.round(value))}`;
}

function strong(text) {
  return el("strong", {}, text);
}

/* A paragraph assembled from strings and elements, so that bold runs are elements and no
   data ever reaches innerHTML. */
function fill(node, parts) {
  node.textContent = "";
  for (const part of parts) node.append(typeof part === "string" ? document.createTextNode(part) : part);
}

function legend(node, items) {
  node.textContent = "";
  for (const [swatchClass, text, shape] of items) {
    const item = el("span", { class: "legend-item" });
    item.append(el("span", { class: `swatch ${shape || "solid"} ${swatchClass}` }), document.createTextNode(text));
    node.append(item);
  }
}

function niceMax(value) {
  const steps = [1, 2, 2.5, 5, 10];
  const power = Math.pow(10, Math.floor(Math.log10(value)));
  for (const step of steps) if (step * power >= value) return step * power;
  return 10 * power;
}

/* Short names for the drift readout, where a feature's full description is too long. */
const WORDS = {
  score: "the model's own fraud score",
  card_txn_count_1h: "payments on a card in an hour",
  card_txn_count_24h: "payments on a card in a day",
  card_amount_sum_1h: "money spent on a card in an hour",
  card_amount_mean_24h: "a card's average payment over a day",
  card_amount_max_24h: "a card's largest payment in a day",
  card_seconds_since_last: "time since a card's last payment",
  card_distinct_merchants_24h: "merchants a card used in a day",
  device_txn_count_1h: "payments from a device in an hour",
  device_distinct_cards_1h: "cards used on a device in an hour",
  device_distinct_cards_24h: "cards used on a device in a day",
  device_amount_mean_1h: "a device's average payment in an hour",
  merchant_txn_count_1h: "payments at a merchant in an hour",
  merchant_distinct_cards_1h: "cards used at a merchant in an hour",
  merchant_amount_mean_1h: "a merchant's average payment in an hour",
  session_txn_count: "payments in a session",
  session_amount_sum: "money spent in a session",
};

function humanFeature(name, features) {
  if (WORDS[name]) return WORDS[name];
  const found = features.find((feature) => feature.name === name);
  return found ? found.description.replace(/\.$/, "").toLowerCase() : name.replace(/_/g, " ");
}

/* ---------------------------------------------------------------- the live strip */

function liveStrip(data) {
  $("dashboard-link").href = data.dashboard;
  if (!data.live.start) {
    $("live-strip").classList.add("ended");
    $("live-text").textContent =
      "Restarting: the first live window failed on its second day, and a thirty-day window on a larger, on-demand machine starts shortly. Why is in the repository (ADR 31).";
    document.querySelector(".cta-note").textContent =
      "The dashboard is back when the new window starts. Everything below was measured before it.";
    return;
  }
  const start = new Date(data.live.start);
  const days = data.live.days;
  const end = new Date(start.getTime() + days * 86400000);
  const runsUntil = new Date(data.live.runs_until || end);
  const reveal = new Date(Math.max(end, runsUntil) + 86400000);
  const now = new Date();
  const dateFormat = { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" };
  const endText = end.toLocaleDateString("en-GB", dateFormat);
  const revealText = reveal.toLocaleDateString("en-GB", dateFormat);
  const text = $("live-text");
  if (now < start) {
    text.textContent = `The live window opens on ${start.toLocaleDateString("en-GB", dateFormat)}`;
  } else if (now < end) {
    const day = Math.floor((now - start) / 86400000) + 1;
    text.textContent = `Live now: day ${day} of ${days}, until ${endText}. The stack runs on until ${runsUntil.toLocaleDateString("en-GB", dateFormat)} so the last week's labels arrive, and the fraud schedule is revealed on ${revealText}.`;
  } else if (now < runsUntil) {
    text.textContent = `The ${days}-day live window ended on ${endText}. The stack runs on until ${runsUntil.toLocaleDateString("en-GB", dateFormat)} so the last week's labels arrive, and the fraud schedule is revealed on ${revealText}.`;
  } else {
    $("live-strip").classList.add("ended");
    text.textContent = `The ${days}-day live window ran from ${start.toLocaleDateString("en-GB", dateFormat)} to ${endText}. The stack has been shut down, and every day of it is drawn below.`;
    // The dashboard went with the stack: the first button becomes the code.
    $("dashboard-link").hidden = true;
    document.querySelector(".cta-note").textContent = "The live dashboard went down with the stack on 7 November.";
    return;
  }
  $("dashboard-link").href = data.dashboard;
}

/* ---------------------------------------------------------------- hero */

function hero(data) {
  const base = data.load_tests.find((test) => test.rate === 1000);
  $("hero-p99").textContent = ms(base.p99_ms.value);
  fill($("hero-p99-note"), [
    `95% CI ${range(base.p99_ms, 1, " ms")}, at 1,000 a second on the live machine, against a ${data.budget_ms} ms budget. Synthetic track.`,
  ]);
  const kept = data.load_tests.filter((test) => test.kept_up === test.runs);
  const top = kept[kept.length - 1];
  $("hero-rate").textContent = `${fmt((top.rate * 3600) / 1e6, 1)} million`;
  $("hero-rate-unit").textContent = `transactions an hour, ${number.format(top.rate)} a second, and it kept up`;
  $("hero-rate-note").textContent = `four times the live rate, one scorer, every one of ${top.runs} runs. Synthetic track, on the live machine.`;
  const q = data.queue.difference_dollars;
  const year = data.queue.team_a_year_dollars;
  $("hero-queue").textContent = `+${millions(year.value)}`;
  $("hero-queue-note").textContent =
    `95% CI ${millions(year.low)} to ${millions(year.high)}: ${dollars(q.value)} more per analyst-hour, by ranking the review queue on expected loss. Synthetic, stated prices.`;
  $("hero-drift-note").textContent =
    "three shifts over fifty days of stream, and nothing flagged in the clean week before the first. Synthetic.";
}

/* ---------------------------------------------------------------- the pipeline */

const STAGES = [
  {
    hop: "ingest",
    title: "It arrives",
    body: "From the payment being sent to the scorer reading it off the stream: Redpanda, a Kafka-compatible log that keeps every transaction until it is decided.",
    stream: true,
  },
  {
    hop: "features",
    title: "What it knows",
    body: "Sixteen facts about the recent past of this card, its device, the merchant and the session, from state held in memory. None of them can include the transaction itself, or anything after it.",
  },
  {
    hop: "model",
    title: "The score",
    body: "Gradient-boosted trees (XGBoost, served as ONNX) give a probability of fraud. The challenger scores the same features beside it and decides nothing.",
  },
  {
    hop: "decision",
    title: "The decision",
    body: "",
  },
  {
    hop: "persist",
    title: "It is written",
    body: "The decision goes back on the stream, with the features it was made on, so every decision can be replayed and every training row is exactly what was served.",
  },
];

function pipeline(data) {
  const select = $("pipeline-rate");
  for (const test of data.load_tests) {
    select.append(el("option", { value: String(test.rate) }, `${number.format(test.rate)} transactions a second`));
  }
  const rules = data.rules;
  STAGES[3].body = `Decline at a score of ${fmt(rules.decline_at, 2)} or more; send to an analyst at ${fmt(rules.review_at, 2)} or more, or for any amount of ${dollars(rules.review_amount_dollars).replace(".00", "")} or more; otherwise approve. The rules are data, so a rollback can switch them.`;
  const list = $("pipeline");

  function draw() {
    const test = data.load_tests.find((t) => String(t.rate) === select.value);
    const hops = test.hops_p99_ms;
    const largest = Math.max(...STAGES.map((stage) => hops[stage.hop].value));
    list.textContent = "";
    for (const stage of STAGES) {
      const hop = hops[stage.hop];
      const item = el("li", { class: `stage${stage.stream ? " stream" : ""}` });
      const time = el("div", { class: "stage-time" }, ms(hop.value));
      time.append(el("small", {}, `p99, 95% CI ${range(hop, hop.value < 1 ? 3 : 1)}`));
      const bar = el("div", { class: "stage-bar", "aria-hidden": "true" });
      const fillBar = el("span");
      fillBar.style.width = `${Math.max(1, (100 * hop.value) / largest)}%`;
      bar.append(fillBar);
      item.append(el("h3", {}, stage.title), el("p", {}, stage.body), time, bar);
      list.append(item);
    }
    const scoring = ["features", "model", "decision", "persist"].reduce((sum, hop) => sum + hops[hop].value, 0);
    fill($("pipeline-verdict"), [
      strong(`The model is ${ms(hops.model.value)} of it.`),
      ` At ${number.format(test.rate)} a second the scorer's own four steps take ${ms(scoring)} between them at the 99th percentile. Most of the ${ms(test.p99_ms.value)} end to end is the transaction's trip through the stream. That trip is what lets a transaction survive the machine being taken away, never be decided twice, and be replayed later.`,
    ]);
  }

  select.addEventListener("change", draw);
  select.value = "1000";
  draw();

  const groups = { card: "The card", device: "The device", merchant: "The merchant", session: "The session" };
  const holder = $("features");
  for (const [entity, title] of Object.entries(groups)) {
    const group = el("div", { class: "feature-group" });
    group.append(el("h4", {}, title));
    const ul = el("ul");
    for (const feature of data.features.filter((f) => f.entity === entity)) {
      ul.append(el("li", { title: feature.name }, feature.description));
    }
    group.append(ul);
    holder.append(group);
  }
}

/* ---------------------------------------------------------------- load */

function loadChart(data) {
  const W = 760, H = 330, L = 56, R = 18, T = 18, B = 42;
  const tests = data.load_tests;
  const top = niceMax(Math.max(data.budget_ms * 1.2, ...tests.map((t) => t.p99_ms.high)));
  const x = (i) => L + ((W - L - R) * (i + 0.5)) / tests.length;
  const y = (v) => T + (H - T - B) * (1 - v / top);
  const svg = svgRoot(W, H, "Decision latency against load, with the 50 ms budget");

  for (let v = 0; v <= top; v += top / 5) {
    svg.append(sv("line", { class: "gridline", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svg.append(sv("text", { class: "tick", x: L - 8, y: y(v) + 4, "text-anchor": "end" }, fmt(v, 0)));
  }
  svg.append(sv("text", { class: "axis-title", x: 14, y: T + (H - T - B) / 2, transform: `rotate(-90 14 ${T + (H - T - B) / 2})`, "text-anchor": "middle" }, "milliseconds"));
  svg.append(sv("line", { class: "axis-line", x1: L, x2: W - R, y1: H - B, y2: H - B }));
  tests.forEach((t, i) => {
    svg.append(sv("text", { class: "tick", x: x(i), y: H - B + 18, "text-anchor": "middle" }, `${number.format(t.rate)} a second`));
  });
  svg.append(sv("line", { class: "budget", x1: L, x2: W - R, y1: y(data.budget_ms), y2: y(data.budget_ms) }));
  svg.append(sv("text", { class: "budget-label", x: W - R - 4, y: y(data.budget_ms) - 6, "text-anchor": "end" }, `THE BUDGET, ${data.budget_ms} MS`));

  const hits = [];
  tests.forEach((t, i) => {
    const hit = sv("rect", { class: "column-hit", x: x(i) - (W - L - R) / tests.length / 2, y: T, width: (W - L - R) / tests.length, height: H - T - B });
    svg.append(hit);
    hits.push(hit);
  });

  for (const key of ["p50", "p95", "p99"]) {
    const points = tests.map((t, i) => `${x(i)},${y(t[`${key}_ms`].value)}`).join(" ");
    svg.append(sv("polyline", { class: `series ${key}`, points }));
    tests.forEach((t, i) => {
      const v = t[`${key}_ms`];
      const offset = key === "p50" ? -7 : key === "p95" ? 0 : 7;
      svg.append(sv("line", { class: `whisker ${key}`, x1: x(i) + offset, x2: x(i) + offset, y1: y(v.low), y2: y(v.high) }));
      svg.append(sv("circle", { class: `dot ${key}`, cx: x(i), cy: y(v.value), r: 4 }));
    });
  }
  $("load-chart").append(svg);

  const readout = $("load-readout");
  function show(i) {
    hits.forEach((h, j) => h.classList.toggle("on", i === j));
    const t = tests[i];
    readout.textContent = "";
    readout.append(el("span", { class: "when" }, `${number.format(t.rate)} a second`));
    for (const key of ["p50", "p95", "p99"]) {
      const v = t[`${key}_ms`];
      const pair = el("span", { class: "pair" });
      pair.append(el("span", { class: "key" }, `${key} `), document.createTextNode(`${ms(v.value)} (${range(v, 1)})`));
      readout.append(pair);
    }
    readout.append(el("span", { class: "pair" }, `kept up in ${t.kept_up} of ${t.runs} runs`));
  }
  hits.forEach((hit, i) => {
    hit.addEventListener("mouseenter", () => show(i));
    hit.addEventListener("click", () => show(i));
  });
  show(0);

  legend($("load-legend"), [
    ["sw-p50", "p50, the typical decision"],
    ["sw-p95", "p95, one in twenty"],
    ["sw-p99", "p99, one in a hundred"],
    ["sw-warn", "the 50 ms budget", "dashed"],
  ]);
  const when = new Date(tests[0].measured_at).toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
  $("load-caption").textContent = `Synthetic track, on the live instance, ${when}. Five runs of 20 seconds at each load, the first 2,000 decisions of each left out. Whiskers are 95 percent intervals across runs. Source: docs/loadtest-live-*.json.`;

  const four = tests[tests.length - 1];
  const worst = Math.max(...four.per_run.map((run) => run.p99_ms));
  const rest = four.per_run.map((run) => run.p99_ms).filter((v) => v !== worst);
  fill($("load-verdict"), [
    strong("Up to three times the live rate, every percentile is well inside the budget."),
    ` At ${number.format(four.rate)} a second the consumer still kept up in every run, but the p99 starts to show the ceiling: four runs came in between ${fmt(Math.min(...rest), 0)} and ${fmt(Math.max(...rest), 0)} ms and one at ${fmt(worst, 0)}, which is why its interval crosses the line. The live stream's busiest minutes reached about 1,450 a second by the end of the window, so the scorer has nearly three times that in hand. This is one scorer on one partition; more of them would divide the load.`,
  ]);
}

/* ---------------------------------------------------------------- the window, day by day */

function windowChart(data) {
  const daily = data.daily;
  if (!daily || !daily.days.length) {
    $("window-panel").hidden = true;
    return;
  }
  const rows = daily.days;
  const W = 760, H = 300, L = 56, R = 18, T = 18, B = 62;
  const pct = (row) => 100 * row.over_budget_share;
  const top = niceMax(Math.max(1.25, ...rows.map(pct)));
  const slot = (W - L - R) / rows.length;
  const x = (i) => L + slot * i;
  const y = (v) => T + (H - T - B) * (1 - v / top);
  const svg = svgRoot(W, H, "The share of each day's decisions over the 50 ms budget, through the live window");
  const day = { day: "numeric", month: "short", timeZone: "UTC" };
  const label = (row) => new Date(`${row.day}T00:00:00Z`).toLocaleDateString("en-GB", day);
  const p99 = (row) => (row.p99_ms >= 1000 ? `${fmt(row.p99_ms / 1000, 1)} s` : ms(row.p99_ms));

  for (let v = 0; v <= top + 1e-9; v += top / 5) {
    svg.append(sv("line", { class: "gridline", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svg.append(sv("text", { class: "tick", x: L - 8, y: y(v) + 4, "text-anchor": "end" }, `${fmt(v, 1)}%`));
  }
  svg.append(sv("text", { class: "axis-title", x: 14, y: T + (H - T - B) / 2, transform: `rotate(-90 14 ${T + (H - T - B) / 2})`, "text-anchor": "middle" }, "over 50 ms"));

  const hits = [];
  rows.forEach((row, i) => {
    const hit = sv("rect", { class: "column-hit", x: x(i), y: T, width: slot, height: H - T - B });
    svg.append(hit);
    hits.push(hit);
    const h = Math.max(1, (H - T - B) * (pct(row) / top));
    svg.append(sv("rect", { class: "bar day-bar", x: x(i) + 1, y: H - B - h, width: Math.max(1, slot - 2), height: h, rx: Math.min(2, slot / 4) }));
    if (i === 0 || new Date(`${row.day}T00:00:00Z`).getUTCDay() === 1) {
      svg.append(sv("text", { class: "tick", x: x(i) + slot / 2, y: H - B + 16, "text-anchor": "middle" }, label(row)));
    }
    const cx = x(i) + slot / 2;
    if (row.minutes_without_decisions > 0) {
      svg.append(sv("circle", { class: "day-mark restart", cx, cy: H - B + 30, r: 4 }));
    }
    if (Object.keys(row.models).length > 1) {
      svg.append(sv("rect", { class: "day-mark model", x: cx - 3.5, y: H - B + 40, width: 7, height: 7, transform: `rotate(45 ${cx} ${H - B + 43.5})` }));
    }
  });
  svg.append(sv("line", { class: "axis-line", x1: L, x2: W - R, y1: H - B, y2: H - B }));
  svg.append(sv("line", { class: "budget", x1: L, x2: W - R, y1: y(1), y2: y(1) }));
  svg.append(sv("text", { class: "budget-label", x: L + 6, y: y(1) - 6, "text-anchor": "start" }, "ONE IN A HUNDRED: THE P99 AT 50 MS"));
  // The days after the window's thirty are the run-on, while the last labels arrive.
  const windowEnd = new Date(new Date(data.live.start).getTime() + data.live.days * 86400000).toISOString().slice(0, 10);
  const runOn = rows.findIndex((row) => row.day > windowEnd);
  if (runOn > 0) {
    svg.append(sv("line", { class: "request-mark", x1: x(runOn), x2: x(runOn), y1: T, y2: H - B }));
    svg.append(sv("text", { class: "request-label", x: x(runOn) + 5, y: T + 10 }, "run-on"));
  }
  $("window-chart").append(svg);

  const readout = $("window-readout");
  function pair(key, text) {
    const node = el("span", { class: "pair" });
    node.append(el("span", { class: "key" }, `${key} `), document.createTextNode(text));
    readout.append(node);
  }
  function show(i) {
    hits.forEach((h, j) => h.classList.toggle("on", i === j));
    const row = rows[i];
    readout.textContent = "";
    readout.append(el("span", { class: "when" }, new Date(`${row.day}T00:00:00Z`).toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "long", timeZone: "UTC" })));
    pair("decisions", number.format(row.decisions));
    pair("over 50 ms", `${fmt(pct(row), 2)}%`);
    pair("p50", ms(row.p50_ms));
    pair("p99", p99(row));
    pair("busiest", `${number.format(row.peak_per_second)} a second`);
    if (row.minutes_without_decisions > 0) pair("paused", `${row.minutes_without_decisions} min, a scorer restart`);
    const models = Object.keys(row.models);
    pair(models.length > 1 ? "models" : "model", models.join(", then "));
  }
  hits.forEach((hit, i) => {
    hit.addEventListener("mouseenter", () => show(i));
    hit.addEventListener("click", () => show(i));
  });
  show(rows.length - 1);

  legend($("window-legend"), [
    ["sw-accent", "share of the day's decisions over 50 ms"],
    ["sw-warn", "one in a hundred, where the p99 meets the budget", "dashed"],
    ["sw-ink", "a scorer restart that day", "dot"],
    ["sw-faint", "the champion changed that day", "diamond"],
  ]);

  const total = rows.reduce((sum, row) => sum + row.decisions, 0);
  const inside = rows.filter((row) => row.over_budget_share <= 0.01).length;
  const restarts = rows.filter((row) => row.minutes_without_decisions > 0);
  const quiet = rows.filter((row) => row.minutes_without_decisions === 0);
  const worstQuiet = quiet.reduce((a, b) => (b.over_budget_share > a.over_budget_share ? b : a), quiet[0]);
  $("window-caption").textContent = `Synthetic live track, ${label(rows[0])} to ${label(rows[rows.length - 1])}, ${number.format(total)} decisions. Every decision is counted, not sampled, and percentiles are read from the scorer's latency buckets. The first day starts when the window opened${runOn > 0 ? `, and the days after ${new Date(`${windowEnd}T00:00:00Z`).toLocaleDateString("en-GB", day)} are the run-on` : ""}. Source: docs/live/daily.json.`;
  const pauseRange = (days) => {
    const minutes = days.map((row) => row.minutes_without_decisions);
    const low = Math.min(...minutes), high = Math.max(...minutes);
    return low === high ? `${low}` : `${low} to ${high}`;
  };
  // The share over budget across a run of days, each decision counted once.
  const share = (days) => (100 * days.reduce((sum, row) => sum + row.over_budget_share * row.decisions, 0)) / days.reduce((sum, row) => sum + row.decisions, 0);
  const parts = [strong(`The day's p99 was inside the budget on ${inside} of ${rows.length} days.`)];
  if (rows.length >= 15) {
    parts.push(` Over the first seven full days ${fmt(share(rows.slice(1, 8)), 2)}% of decisions went over 50 ms, and over the last seven ${fmt(share(rows.slice(-7)), 2)}%. The fraud labels feed started on 6 October, the traffic grew, and the history compactor shares the same four virtual CPUs as the scorer.`);
  }
  if (restarts.length) {
    parts.push(` The scorer was restarted on ${restarts.length === 1 ? "one day" : `${restarts.length} days`} (${restarts.map(label).join(", ")}). A restart stops it for ${pauseRange(restarts)} minutes while it restores its state, and everything that arrives meanwhile is decided late and counted as late.`);
  }
  parts.push(` Without a restart the worst day was ${label(worstQuiet)}, with ${fmt(100 * worstQuiet.over_budget_share, 2)}% over.`);
  fill($("window-verdict"), parts);

  const table = $("window-table");
  const thead = el("thead");
  const head = el("tr");
  for (const name of ["Day", "Decisions", "Busiest a second", "p50", "p99", "Over 50 ms", "Paused", "Model"]) head.append(el("th", { scope: "col" }, name));
  thead.append(head);
  const body = el("tbody");
  for (const row of rows) {
    const tr = el("tr");
    const cells = [
      row.day,
      number.format(row.decisions),
      number.format(row.peak_per_second),
      ms(row.p50_ms),
      p99(row),
      `${fmt(pct(row), 2)}%`,
      row.minutes_without_decisions ? `${row.minutes_without_decisions} min` : "",
      Object.keys(row.models).join(", then "),
    ];
    for (const text of cells) tr.append(el("td", {}, text));
    body.append(tr);
  }
  table.append(thead, body);
}

/* ---------------------------------------------------------------- spot reclaims */

function spotChart(data) {
  const run = data.dry_run;
  const W = 760, H = 150, L = 16, R = 16, T = 26, B = 34;
  const start = new Date(run.first_minute).getTime();
  const end = new Date(run.last_minute).getTime() + 60000;
  const x = (t) => L + ((W - L - R) * (t - start)) / (end - start);
  const svg = svgRoot(W, H, "Seventy-two hours with every spot reclaim marked");
  const trackY = T + 20, trackH = 36;
  svg.append(sv("rect", { class: "track", x: L, y: trackY, width: W - L - R, height: trackH, rx: 6 }));
  for (let h = 0; h <= run.hours; h += 12) {
    const tx = x(start + h * 3600000);
    svg.append(sv("line", { class: "day-tick", x1: tx, x2: tx, y1: trackY + trackH, y2: trackY + trackH + 6 }));
    svg.append(sv("text", { class: "tick", x: tx, y: trackY + trackH + 20, "text-anchor": h === 0 ? "start" : h === run.hours ? "end" : "middle" }, `${h} h`));
  }
  const day = { day: "numeric", timeZone: "UTC" };
  const month = { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" };
  svg.append(sv("text", { class: "axis-title", x: L, y: T + 6 }, `${run.hours} hours at the live rate, ${new Date(start).toLocaleDateString("en-GB", day)} to ${new Date(end).toLocaleDateString("en-GB", month)}`));
  const marks = [];
  for (const reclaim of run.reclaims) {
    const a = new Date(reclaim.noticed_at).getTime();
    const b = new Date(reclaim.recovered_at).getTime();
    const mark = sv("rect", { class: "reclaim", x: x(a), y: trackY + 4, width: Math.max(3, x(b) - x(a)), height: trackH - 8, rx: 2 });
    svg.append(mark);
    const hit = sv("rect", { class: "reclaim-hit", x: x(a) - 6, y: trackY, width: Math.max(3, x(b) - x(a)) + 12, height: trackH });
    svg.append(hit);
    marks.push([mark, hit, reclaim]);
  }
  $("spot-chart").append(svg);
  $("spot-count").textContent = String(run.spot_reclaims);

  const readout = $("spot-readout");
  function show(index) {
    const [, , reclaim] = marks[index];
    marks.forEach(([mark], j) => mark.classList.toggle("on", j === index));
    const at = new Date(reclaim.noticed_at);
    const when = at.toLocaleString("en-GB", { weekday: "short", hour: "2-digit", minute: "2-digit", timeZone: "UTC" });
    readout.textContent = "";
    readout.append(
      el("span", { class: "when" }, `${when} UTC`),
      el("span", { class: "pair" }, `AWS gave notice, and ${reclaim.minutes} minutes later the platform was current again`),
      el("span", { class: "pair" }, `${reclaim.minutes_without_decisions} of those minutes with no decision at all`),
    );
  }
  marks.forEach(([, hit], i) => {
    hit.addEventListener("mouseenter", () => show(i));
    hit.addEventListener("click", () => show(i));
  });
  readout.textContent = "Point at a mark to read it.";
  $("spot-caption").textContent = `Synthetic track, the 72-hour dry run before go-live, development schedule at 1,000 a second. A mark runs from AWS's two-minute notice to the minute the platform was deciding on time again. Source: docs/dry-run-report.json.`;

  const stats = [
    [`${fmt(run.uptime_percent, 2)}%`, "of minutes with decisions", `${run.minutes_without_decisions} minutes without any, in ${number.format(run.hours * 60)}`, "up"],
    [`${run.median_recovery_minutes} min`, "median from notice to current again", `range ${Math.min(...run.reclaims.map((r) => r.minutes))} to ${Math.max(...run.reclaims.map((r) => r.minutes))} minutes, over ${run.spot_reclaims} reclaims`, ""],
    [`${fmt(run.p99_ms.value, 1)} ms`, "p99 while serving, over three days", `95% CI ${range(run.p99_ms, 1, " ms")}; p50 ${fmt(run.p50_ms.value, 1)} ms. Estimated from the histogram, reclaim catch-ups left out`, ""],
    [`${number.format(Math.round(run.decisions / 1e6))} million`, "decisions in the 72 hours", `${run.other_stops} stops other than AWS's own`, ""],
  ];
  const holder = $("spot-headline");
  for (const [figure, caption, rangeText, mood] of stats) {
    const card = el("div", { class: `stat ${mood}` });
    card.append(el("span", { class: "figure" }, figure), el("span", { class: "caption" }, caption), el("span", { class: "range" }, rangeText));
    holder.append(card);
  }
}

/* ---------------------------------------------------------------- drift */

/* Each regime's name on the chart, its name in the legend, and what it does. The design of
   the regimes is public (verdict/events/generator/regimes.py); the live window's timing of
   them is what is sealed. */
const REGIMES = {
  baseline: ["Baseline", "The baseline", "the traffic the model was trained on"],
  "card-testing-wave": ["Card testing", "A card-testing wave", "fraud 2.1 times as common, 5 points more of it online"],
  "amount-drift-no-fraud-change": ["Amounts drift", "Amounts drift, fraud does not", "amounts about 35 percent higher, the same share of fraud"],
  "takeover-season": ["Takeovers", "Account takeovers", "fraud 1.4 times as common, 15 points more of it online"],
};

function driftChart(data) {
  const drift = data.drift;
  const days = drift.days;
  const first = days[0].day, last = days[days.length - 1].day;
  const top = drift.quantities;
  const W = 760, L = 220, R = 14, T = 30;
  const bw = (W - L - R) / (last + 1);
  const x = (d) => L + d * bw;
  const drawn = number.format(Math.round(days.reduce((s, d) => s + d.values, 0) / days.length));

  // What the fraudsters changed: three of the schedule's settings, drawn as steps.
  const levers = [
    ["How common fraud is", (r) => r.fraud_rate_multiplier, (v) => `${fmt(v, 1)}x`, 1],
    ["Typical payment amount", (r) => r.typical_amount_multiplier, (v) => `+${fmt((v - 1) * 100, 0)}%`, 1],
    ["Share paid online", (r) => r.online_share_points, (v) => `+${fmt(v, 0)} pts`, 0],
  ];
  const laneH = 40, laneGap = 8;
  const leverTop = T + 22;
  const gridTop = leverTop + levers.length * (laneH + laneGap) + 30;
  const rowH = 13;
  const names = drift.names;
  const H = gridTop + names.length * rowH + 30;
  const svg = svgRoot(W, H, "What the fraud did each day, and which of the 17 monitored signals shifted");
  const regimeIndex = Object.fromEntries(drift.regimes.map((r, i) => [r.name, i]));
  const end = (i) => (i + 1 < drift.regimes.length ? drift.regimes[i + 1].starts_day : last + 1);

  drift.regimes.forEach((regime, i) => {
    if (i % 2 === 1) svg.append(sv("rect", { class: "regime-span", x: x(regime.starts_day), y: T - 22, width: x(end(i)) - x(regime.starts_day), height: H - 30 - T + 22 }));
    const lastOne = i === drift.regimes.length - 1;
    svg.append(sv("text", { class: "regime-label", x: lastOne ? x(end(i)) - 2 : x(regime.starts_day) + 4, y: T - 8, "text-anchor": lastOne ? "end" : "start" }, REGIMES[regime.name] ? REGIMES[regime.name][0] : regime.name));
  });

  svg.append(sv("text", { class: "group-label", x: 0, y: leverTop - 8 }, "What the fraud did"));
  levers.forEach(([title, get, label, base], k) => {
    const y0 = leverTop + k * (laneH + laneGap);
    const values = drift.regimes.map(get);
    const hi = Math.max(...values), lo = Math.min(base, ...values);
    const y = (v) => y0 + laneH - 4 - ((laneH - 16) * (v - lo)) / (hi - lo || 1);
    svg.append(sv("text", { class: "row-label", x: L - 10, y: y0 + laneH / 2 + 4, "text-anchor": "end" }, title));
    svg.append(sv("line", { class: "gridline", x1: L, x2: W - R, y1: y(base), y2: y(base) }));
    let points = "";
    drift.regimes.forEach((regime, i) => {
      const v = get(regime);
      points += `${x(regime.starts_day)},${y(v)} ${x(end(i))},${y(v)} `;
      if (v !== base) svg.append(sv("text", { class: "bar-value", x: x(end(i)) - 4, y: y(v) - 4, "text-anchor": "end" }, label(v)));
    });
    svg.append(sv("polyline", { class: "series lever", points: points.trim() }));
  });

  svg.append(sv("text", { class: "group-label", x: 0, y: gridTop - 10 }, "What the monitors saw"));
  svg.append(sv("text", { class: "group-note", x: L + 4, y: gridTop + 4 * rowH }, "the training"));
  svg.append(sv("text", { class: "group-note", x: L + 4, y: gridTop + 5 * rowH + 2 }, "week, the"));
  svg.append(sv("text", { class: "group-note", x: L + 4, y: gridTop + 6 * rowH + 4 }, "yardstick"));
  names.forEach((name, j) => {
    svg.append(sv("text", { class: "tick", x: L - 10, y: gridTop + j * rowH + rowH - 3, "text-anchor": "end" }, humanFeature(name, data.features)));
  });
  const cols = [];
  for (const day of days) {
    const cls = `regime-${regimeIndex[day.regime]}`;
    names.forEach((name, j) => {
      const state = day.drifted.includes(name) ? `shifted ${cls}` : day.moderate.includes(name) ? `moved ${cls}` : "steady";
      svg.append(sv("rect", { class: `cell ${state}`, x: x(day.day) + 1, y: gridTop + j * rowH + 1, width: Math.max(1, bw - 2), height: rowH - 2, rx: 1.5 }));
    });
    const hit = sv("rect", { class: "column-hit", x: x(day.day), y: T - 22, width: bw, height: H - 30 - T + 22 });
    svg.append(hit);
    cols.push([hit, day]);
  }
  const rx = x(drift.first_request_day) + bw / 2;
  svg.append(sv("line", { class: "request-mark", x1: rx, x2: rx, y1: leverTop - 4, y2: H - 30 }));
  svg.append(sv("text", { class: "request-label", x: rx + 5, y: gridTop - 10 }, "retraining requested"));
  for (let d = 0; d <= last; d += 7) {
    svg.append(sv("text", { class: "tick", x: x(d) + bw / 2, y: H - 10, "text-anchor": "middle" }, `day ${d}`));
  }
  $("drift-chart").append(svg);

  const readout = $("drift-readout");
  function show(index) {
    const [, day] = cols[index];
    cols.forEach(([hit], j) => hit.classList.toggle("on", j === index));
    readout.textContent = "";
    readout.append(el("span", { class: "when" }, `Day ${day.day}`));
    readout.append(el("span", { class: "pair" }, `fraud running: ${REGIMES[day.regime] ? REGIMES[day.regime][1].toLowerCase() : day.regime}`));
    const shifted = day.drifted.map((name) => humanFeature(name, data.features));
    readout.append(el("span", { class: "pair" }, shifted.length ? `${shifted.length} of ${top} signals shifted: ${shifted.join("; ")}` : `none of the ${top} signals shifted`));
  }
  cols.forEach(([hit], i) => {
    hit.addEventListener("mouseenter", () => show(i));
    hit.addEventListener("click", () => show(i));
  });
  readout.textContent = "Point at a day to see which signals shifted.";

  legend($("drift-legend"), [
    ...drift.regimes.map((r, i) => [`sw-regime-${i}`, REGIMES[r.name] ? `${REGIMES[r.name][1]}: ${REGIMES[r.name][2]}` : r.name]),
  ]);
  $("drift-caption").textContent = `Synthetic track, offline replay of the public development schedule. The three lines are the schedule's settings. In the grid a solid square is a signal past the line that day, a pale one a signal that moved but stayed under it, measured on about ${drawn} transactions drawn each day. Source: docs/drift-report.json and verdict/events/generator/regimes.py.`;
  fill($("drift-method"), [
    `Each day, about ${drawn} transactions are drawn by a hash of their identifier (the same ones whoever reruns it), and for each of the ${top} signals (the model's inputs and its own score) the day's distribution is compared with the training week's. A signal counts as shifted when its population stability index passes ${fmt(drift.psi_threshold, 2)} or its Kolmogorov-Smirnov statistic passes ${fmt(drift.ks_threshold, 2)}, the conventions credit scoring has used for decades. `,
    strong("A retraining request needs the same signal shifted on two days running,"),
    " because a single odd day is often noise. That puts the earliest request a day after a change begins, which is where this one opened. In the live window the reference is the live traffic itself: the window's first two full days, and after each promotion the two full days after it.",
  ]);
}

/* ---------------------------------------------------------------- retraining */

function retrainChart(data) {
  const groups = [
    ["Built when the alarm fired", "trained up to the day before the drift", data.retrain.at_the_alarm],
    ["Built once the drifted days' labels arrived", "about nine days after the drift began", data.retrain.after_labels],
  ];
  const own = data.models.synthetic.champion;
  const W = 760, H = 250, L = 250, R = 70, T = 30, B = 34;
  const x = (v) => L + (W - L - R) * v;
  const svg = svgRoot(W, H, "Champion and candidate PR-AUC, before and after the drifted labels arrived");
  for (let v = 0; v <= 1.0001; v += 0.2) {
    svg.append(sv("line", { class: "gridline", x1: x(v), x2: x(v), y1: T - 8, y2: H - B }));
    svg.append(sv("text", { class: "tick", x: x(v), y: H - B + 16, "text-anchor": "middle" }, fmt(v, 1)));
  }
  svg.append(sv("text", { class: "axis-title", x: x(0.5), y: H - 4, "text-anchor": "middle" }, "PR-AUC on rows neither model saw"));
  svg.append(sv("line", { class: "divider", x1: x(own.value), x2: x(own.value), y1: T - 16, y2: H - B }));
  svg.append(sv("text", { class: "divider-label", x: x(own.value), y: T - 20, "text-anchor": "middle" }, `champion on its own stream, ${fmt(own.value, 2)}`));
  const barH = 22, gap = 6, groupH = barH * 2 + gap + 30;
  groups.forEach(([title, note, comparison], i) => {
    const gy = T + 8 + i * groupH;
    svg.append(sv("text", { class: "group-label", x: L - 12, y: gy + barH - 4, "text-anchor": "end" }, title));
    svg.append(sv("text", { class: "group-note", x: L - 12, y: gy + barH + 12, "text-anchor": "end" }, note));
    [["champion", comparison.champion], ["candidate", comparison.candidate]].forEach(([kind, v], j) => {
      const by = gy + j * (barH + gap);
      svg.append(sv("rect", { class: `bar ${kind}`, x: x(0), y: by, width: x(v.value) - x(0), height: barH, rx: 3 }));
      svg.append(sv("line", { class: "whisker", x1: x(v.low), x2: x(v.high), y1: by + barH / 2, y2: by + barH / 2 }));
      svg.append(sv("text", { class: "bar-value", x: x(v.value) + 6, y: by + barH / 2 + 4 }, fmt(v.value, 3)));
    });
  });
  $("retrain-chart").append(svg);
  legend($("retrain-legend"), [
    ["sw-champion", "the champion, the model in production"],
    ["sw-candidate", "the retrained candidate"],
  ]);
  $("retrain-caption").textContent = "Synthetic track, offline replay of the first regime change. Intervals are 95 percent bootstrap intervals, drawn as hairlines; at this size they are narrower than the bars' ends. Source: docs/retrain.json and docs/retrain-later.json.";
  const early = data.retrain.at_the_alarm;
  const later = data.retrain.after_labels;
  fill($("retrain-verdict"), [
    strong(`The drift did real damage: the champion falls from ${fmt(own.value, 2)} to ${fmt(early.champion.value, 2)} when the card-testing wave arrives.`),
    ` A candidate built the moment the alarm fires cannot help, because a fraud label takes about a week to arrive and nothing it could learn from has seen the change: it scores ${fmt(early.difference.value, 4)} against the champion (${range(early.difference, 4)}), and the gate refuses it. Once the drifted days' labels are in, the candidate recovers to ${fmt(later.candidate.value, 2)}, ${fmt(later.difference.value, 2)} better (${range(later.difference, 2)}). In the week between, the rules and the review queue have to do the work the model cannot, which is why the platform has both.`,
  ]);
}

/* ---------------------------------------------------------------- the queue */

function queueChart(data) {
  const q = data.queue;
  fill($("queue-lede"), [
    `Only so many transactions can be checked by a person. With ${q.analysts} analysts at ${q.reviews_per_analyst_hour} reviews an hour, a team checks ${number.format(q.reviews_per_day)} a day, and ${fmt((1 - q.reviews_per_day / (q.queued / q.days)) * 100, 0)} percent of what the rules send to review is never opened. So the order matters. The usual order is by the model's score. This platform orders by `,
    strong("expected loss"),
    ": the chance it is fraud, times the money at stake, less what a chargeback would recover anyway, less the cost of looking. A likely twelve-dollar fraud is not worth an analyst's time; a less likely forty-thousand-dollar one is.",
  ]);
  const W = 760, H = 150, L = 200, R = 90, T = 12, B = 30;
  const top = niceMax(q.by_expected_loss_dollars * 1.1);
  const x = (v) => L + (W - L - R) * (v / top);
  const svg = svgRoot(W, H, "Money caught per analyst-hour, by score and by expected loss");
  for (let v = 0; v <= top; v += top / 5) {
    svg.append(sv("line", { class: "gridline", x1: x(v), x2: x(v), y1: T, y2: H - B }));
    svg.append(sv("text", { class: "tick", x: x(v), y: H - B + 16, "text-anchor": "middle" }, `$${fmt(v, 0)}`));
  }
  const rows = [
    ["Ranked by score", q.by_score_dollars, "by-score"],
    ["Ranked by expected loss", q.by_expected_loss_dollars, "by-loss"],
  ];
  rows.forEach(([label, value, cls], i) => {
    const by = T + 10 + i * 46;
    svg.append(sv("text", { class: "row-label", x: L - 12, y: by + 19, "text-anchor": "end" }, label));
    svg.append(sv("rect", { class: `bar ${cls}`, x: x(0), y: by, width: x(value) - x(0), height: 28, rx: 3 }));
    svg.append(sv("text", { class: "bar-value", x: x(value) + 8, y: by + 19 }, `${dollars(value)} an hour`));
  });
  $("queue-chart").append(svg);
  $("queue-caption").textContent = `Synthetic track, ${q.days} days the champion never saw, ${number.format(q.transactions)} transactions scored and ${number.format(q.queued)} sent to review, replayed through the scorer's own engine and rules. Source: docs/queue-eval.json.`;
  const d = q.difference_dollars;
  const day = q.team_a_day_dollars, month = q.team_a_month_dollars, year = q.team_a_year_dollars;
  fill($("queue-verdict"), [
    strong(`${dollars(d.value)} more caught for every analyst-hour, with a 95 percent interval of ${dollars(d.low)} to ${dollars(d.high)}.`),
    ` For the team of ${q.analysts}, reviewing around the clock, that is ${wholeDollars(day.value)} a day, and ${millions(year.value)} a year at the same rate. The analysts, transactions and hours are the same in both; only the order differs. The interval is a bootstrap over days, and the comparison is paired, with each day ranked both ways.`,
  ]);
  const cards = [
    [dollars(d.value), "more per analyst-hour", `95% CI ${dollars(d.low)} to ${dollars(d.high)}`],
    [wholeDollars(day.value), `more a day, for ${q.analysts} analysts`, `${wholeDollars(day.low)} to ${wholeDollars(day.high)}; ${fmt(q.analyst_hours_a_day, 0)} analyst-hours`],
    [`$${fmt(month.value / 1e3, 0)} thousand`, "more a month", `$${fmt(month.low / 1e3, 0)} to $${fmt(month.high / 1e3, 0)} thousand`],
    [millions(year.value), "more a year", `${millions(year.low)} to ${millions(year.high)}, at the rate of the ${q.days} days measured`],
  ];
  const holder = $("queue-headline");
  for (const [figure, caption, rangeText] of cards) {
    const card = el("div", { class: "stat up" });
    card.append(el("span", { class: "figure" }, figure), el("span", { class: "caption" }, caption), el("span", { class: "range" }, rangeText));
    holder.append(card);
  }
  fill($("queue-assumption"), [
    strong("The prices are assumptions."),
    ` A review is priced at ${dollars(q.review_cost_dollars)}, and ${fmt(q.recovery_rate * 100, 0)} percent of a fraud is assumed recovered by chargeback. Changing either changes the dollars reported but not the order of the queue, and a test checks that. The queue here is ${fmt(q.queue_fraud_share * 100, 0)} percent fraud, richer than a real team's, because it was measured offline on the scaled-down synthetic stream, where the model separates fraud far better than on the full live stream or on real data.`,
  ]);
}

/* ---------------------------------------------------------------- the rest */

function rollback(data) {
  const r = data.rollback_ms;
  $("rollback-ms").textContent = `${ms(r.value)} (95% CI ${range(r, 1, " ms")}, five drills at 1,000 a second)`;
}

function leak(data) {
  const v = data.leak.same_model_inflation;
  fill($("leak-inflation"), [
    `Retrained on the leaky features, the same model changes its prediction on ${data.leak.rows_changed} of ${number.format(data.leak.test_rows)} test rows, and its PR-AUC moves by ${fmt(v.value, 5)} (95% CI ${range(v, 6)}). `,
    strong("A difference that small never shows in a score, so the test checks the features themselves."),
    " Real-data track, offline.",
  ]);
}

function sealed(data) {
  const table = $("sealed-table");
  const rows = [
    ["Sealed at", new Date(data.live.sealed_at).toUTCString().replace("GMT", "UTC")],
    ["The secret's fingerprint", data.live.secret_sha256],
    ["The schedule's fingerprint", data.live.schedule_sha256],
    ["The generator code's fingerprint", data.live.source_sha256],
  ];
  const body = el("tbody");
  for (const [label, value] of rows) {
    const tr = el("tr");
    tr.append(el("th", { scope: "row", class: "wrap" }, label), el("td", { class: label === "Sealed at" ? "" : "hash" }, value));
    body.append(tr);
  }
  table.append(body);
  table.append(el("caption", {}, "SHA-256 hashes, committed as docs/sealed-schedule.json before the window opened. After the reveal, `verdict schedule verify` with the published secret checks all three."));
}

function live(data) {
  const p = data.promotions[0];
  if (!p) {
    $("live-panel").hidden = true;
    return;
  }
  const d = p.difference;
  const signed = (v) => `${v >= 0 ? "+" : ""}${fmt(v, 3)}`;
  const cards = [
    [fmt(p.champion_pr_auc, 3), "PR-AUC of the model that opened the window", "on the live stream, where guessing scores about 0.03"],
    [fmt(p.challenger_pr_auc, 3), "PR-AUC of the retrained model", "on the same transactions"],
    [signed(d.value), "the difference", `95% CI ${signed(d.low)} to ${signed(d.high)}`],
  ];
  const holder = $("live-headline");
  for (const [figure, caption, rangeText] of cards) {
    const card = el("div", { class: "stat up" });
    card.append(el("span", { class: "figure" }, figure), el("span", { class: "caption" }, caption), el("span", { class: "range" }, rangeText));
    holder.append(card);
  }
  const first = p.days[0], last = p.days[p.days.length - 1];
  fill($("live-verdict"), [
    strong(`PR-AUC rose from ${fmt(p.champion_pr_auc, 3)} to ${fmt(p.challenger_pr_auc, 3)}, a difference of ${signed(d.value)} with a 95 percent interval of ${signed(d.low)} to ${signed(d.high)}.`),
    ` Synthetic live track, ${first} to ${last}, days the new model was not trained on: ${number.format(p.rows)} labelled transactions, ${number.format(p.frauds)} of them fraud, each weighted to stand for the whole stream. ${p.replayed ? "The new model scored the features the scorer had served on those days, beside the scores the old one gave live. " : ""}The promotion gate found it eligible on ${p.judged_on}. A person merged the pull request carrying that evidence, and only then did the flag move. Source: ${p.source}.`,
  ]);

  const later = data.promotions.slice(1);
  if (!later.length) {
    for (const id of ["live-next-head", "live-next-lede", "live-next"]) $(id).hidden = true;
    return;
  }
  const parts = [];
  later.forEach((q, i) => {
    const lead = `PR-AUC rose from ${fmt(q.champion_pr_auc, 3)} to ${fmt(q.challenger_pr_auc, 3)} on ${q.days.join(" and ")}, a difference of ${signed(q.difference.value)} (95% CI ${signed(q.difference.low)} to ${signed(q.difference.high)}).`;
    parts.push(i === 0 ? strong(lead) : ` ${lead}`);
    parts.push(` The gate found it eligible on ${q.judged_on}, and a person moved the flag that day.`);
  });
  const now = later[later.length - 1];
  parts.push(` The gain is smaller, on harder traffic: the first retrained model, which scored ${fmt(p.challenger_pr_auc, 2)} before the shift, scored ${fmt(later[0].champion_pr_auc, 2)} after it. ${now.challenger} has decided every transaction since. Synthetic live track, replayed, weighted. Source: ${later.map((q) => q.source).join(", ")}.`);
  fill($("live-next"), parts);
}

function honesty(data) {
  const s = data.models.synthetic.champion, r = data.models.real.champion;
  $("honesty-synth").textContent = `${fmt(s.value, 2)} (${range(s, 3)})`;
  $("honesty-real").textContent = `${fmt(r.value, 3)} (${range(r, 3)}), against a base rate of 0.035`;
  const p = data.promotions[0];
  const now = data.promotions[data.promotions.length - 1];
  $("honesty-live").textContent = !p
    ? "-"
    : now === p
      ? `the retrained model scores ${fmt(p.challenger_pr_auc, 2)}, against the first champion's ${fmt(p.champion_pr_auc, 2)} on the same days (the difference's 95% CI is +${fmt(p.difference.low, 3)} to +${fmt(p.difference.high, 3)})`
      : `the first retrained model scored ${fmt(p.challenger_pr_auc, 2)} against the opening champion's ${fmt(p.champion_pr_auc, 2)}, fell to ${fmt(now.champion_pr_auc, 2)} after the fraud shifted, and the model retrained on the shift scores ${fmt(now.challenger_pr_auc, 2)}`;
  $("adr-count").textContent = number.format(data.adr_count);
}

async function main() {
  try {
    const response = await fetch("results.json", { cache: "no-cache" });
    if (!response.ok) throw new Error(`results.json: ${response.status}`);
    const data = await response.json();
    liveStrip(data);
    hero(data);
    pipeline(data);
    loadChart(data);
    spotChart(data);
    windowChart(data);
    driftChart(data);
    retrainChart(data);
    live(data);
    queueChart(data);
    rollback(data);
    leak(data);
    sealed(data);
    honesty(data);
  } catch (error) {
    const failure = $("failure");
    failure.hidden = false;
    failure.textContent = `The page could not draw its figures (${error.message}). The same numbers are in the repository's README.`;
  }
}

main();
