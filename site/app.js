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

const WORDS = { score: "the model's own score" };

function humanFeature(name, features) {
  if (WORDS[name]) return WORDS[name];
  const found = features.find((feature) => feature.name === name);
  return found ? found.description.replace(/\.$/, "").toLowerCase() : name.replace(/_/g, " ");
}

/* ---------------------------------------------------------------- the live strip */

function liveStrip(data) {
  const start = new Date(data.live.start);
  const days = data.live.days;
  const end = new Date(start.getTime() + days * 86400000);
  const reveal = new Date(end.getTime() + 86400000);
  const now = new Date();
  const dateFormat = { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" };
  const endText = end.toLocaleDateString("en-GB", dateFormat);
  const revealText = reveal.toLocaleDateString("en-GB", dateFormat);
  const text = $("live-text");
  if (now < start) {
    text.textContent = `The live window opens on ${start.toLocaleDateString("en-GB", dateFormat)}`;
  } else if (now < end) {
    const day = Math.floor((now - start) / 86400000) + 1;
    text.textContent = `Live now: day ${day} of ${days}, until ${endText}. The fraud schedule is revealed on ${revealText}.`;
  } else {
    $("live-strip").classList.add("ended");
    text.textContent = `The ${days}-day live window ran from ${start.toLocaleDateString("en-GB", dateFormat)} to ${endText}.`;
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
      ` At ${number.format(test.rate)} a second the scorer's own four steps take ${ms(scoring)} between them at the 99th percentile. Most of the ${ms(test.p99_ms.value)} end to end is the transaction's trip through the stream, and that is the part that buys something: a transaction on the stream survives the machine being taken away, is never decided twice, and can be replayed. The time is spent on not losing anything, not on arithmetic.`,
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
    ` At ${number.format(four.rate)} a second the consumer still kept up in every run, but the p99 is where the ceiling starts to show: four runs came in between ${fmt(Math.min(...rest), 0)} and ${fmt(Math.max(...rest), 0)} ms and one at ${fmt(worst, 0)}, which is why its interval crosses the line. The live stream peaks at 1,250 a second, so it runs with better than three times its busiest hour in hand. One scorer on one partition; more would divide the load.`,
  ]);
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
  const W = 760, H = 320, L = 44, R = 12, T = 34, B = 40;
  const first = days[0].day, last = days[days.length - 1].day;
  const span = last - first + 1;
  const bw = (W - L - R) / span;
  const x = (d) => L + (d - first) * bw;
  const top = drift.quantities;
  const y = (v) => T + (H - T - B) * (1 - v / top);
  const svg = svgRoot(W, H, "Drifted quantities per day, with the regime changes shaded");

  drift.regimes.forEach((regime, i) => {
    const from = Math.max(regime.starts_day, first);
    const to = i + 1 < drift.regimes.length ? drift.regimes[i + 1].starts_day : last + 1;
    if (i % 2 === 1) svg.append(sv("rect", { class: "regime-span", x: x(from), y: T - 22, width: x(to) - x(from), height: H - B - T + 22 }));
    svg.append(sv("text", { class: "regime-label", x: x(from) + 4, y: T - 8 }, REGIMES[regime.name] ? REGIMES[regime.name][0] : regime.name));
  });

  for (let v = 0; v <= top; v += 5) {
    svg.append(sv("line", { class: "gridline", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svg.append(sv("text", { class: "tick", x: L - 8, y: y(v) + 4, "text-anchor": "end" }, String(v)));
  }
  svg.append(sv("text", { class: "axis-title", x: 12, y: T + (H - T - B) / 2, transform: `rotate(-90 12 ${T + (H - T - B) / 2})`, "text-anchor": "middle" }, "quantities drifted"));
  svg.append(sv("line", { class: "axis-line", x1: L, x2: W - R, y1: H - B, y2: H - B }));
  for (let d = Math.ceil(first / 7) * 7; d <= last; d += 7) {
    svg.append(sv("text", { class: "tick", x: x(d) + bw / 2, y: H - B + 18, "text-anchor": "middle" }, `day ${d}`));
  }

  const regimeIndex = Object.fromEntries(drift.regimes.map((r, i) => [r.name, i]));
  const bars = [];
  for (const day of days) {
    const count = day.drifted.length;
    const cls = `bar regime-${regimeIndex[day.regime]}`;
    let bar;
    if (count === 0) {
      bar = sv("circle", { class: "quiet", cx: x(day.day) + bw / 2, cy: y(0) - 4, r: 2.6 });
    } else {
      bar = sv("rect", { class: cls, x: x(day.day) + 1.5, y: y(count), width: Math.max(1, bw - 3), height: y(0) - y(count), rx: 1.5 });
    }
    svg.append(bar);
    const hit = sv("rect", { class: "column-hit", x: x(day.day), y: T - 22, width: bw, height: H - B - T + 22 });
    svg.append(hit);
    bars.push([hit, bar, day]);
  }
  const rx = x(drift.first_request_day) + bw / 2;
  svg.append(sv("line", { class: "request-mark", x1: rx, x2: rx, y1: y(top) + 6, y2: y(0) }));
  svg.append(sv("text", { class: "request-label", x: rx + 5, y: y(top) + 16 }, "retraining requested"));
  $("drift-chart").append(svg);

  const readout = $("drift-readout");
  function show(index) {
    const [, , day] = bars[index];
    bars.forEach(([hit], j) => hit.classList.toggle("on", j === index));
    readout.textContent = "";
    readout.append(el("span", { class: "when" }, `Day ${day.day}`));
    readout.append(el("span", { class: "pair" }, REGIMES[day.regime] ? REGIMES[day.regime][1].toLowerCase() : day.regime));
    if (day.drifted.length === 0) {
      readout.append(el("span", { class: "pair" }, "nothing drifted"));
    } else {
      const names = day.drifted.map((name) => humanFeature(name, data.features));
      readout.append(el("span", { class: "pair" }, `${day.drifted.length} drifted: ${names.slice(0, 4).join("; ")}${names.length > 4 ? `; and ${names.length - 4} more` : ""}`));
    }
  }
  bars.forEach(([hit], i) => {
    hit.addEventListener("mouseenter", () => show(i));
    hit.addEventListener("click", () => show(i));
  });
  readout.textContent = "Point at a day to see what moved.";

  legend(
    $("drift-legend"),
    drift.regimes.map((r, i) => [`sw-regime-${i}`, REGIMES[r.name] ? `${REGIMES[r.name][1]}: ${REGIMES[r.name][2]}` : r.name]),
  );
  $("drift-caption").textContent = `Synthetic track, offline replay of the development schedule, days ${first} to ${last} judged (the first week is the reference). A green dot is a day with nothing drifted. Source: docs/drift-report.json.`;
  fill($("drift-method"), [
    `Each day, about ${number.format(Math.round(days.reduce((s, d) => s + d.values, 0) / days.length))} transactions are drawn by a hash of their identifier (the same ones whoever reruns it), and for each of the ${top} quantities (the model's inputs and its own score) the day's distribution is compared with the reference week's. A quantity has drifted when its population stability index passes ${fmt(drift.psi_threshold, 2)} or its Kolmogorov-Smirnov statistic passes ${fmt(drift.ks_threshold, 2)}, the conventions credit scoring has used for decades. `,
    strong("A request needs the same quantity drifted on two days running,"),
    " because one odd day is weather and two is climate. That puts the earliest request a day after a change begins, which is where this one opened. In the live window the reference is the window's own first two full days, so the monitors judge the live traffic against itself.",
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
    ` A candidate built the moment the alarm fires cannot help, because a fraud label takes about a week to arrive and nothing it could learn from has seen the change: it scores ${fmt(early.difference.value, 4)} against the champion (${range(early.difference, 4)}), and the gate refuses it. Once the drifted days' labels are in, the candidate recovers to ${fmt(later.candidate.value, 2)}, ${fmt(later.difference.value, 2)} better (${range(later.difference, 2)}). The week in between is carried by the rules and the review queue, not the model, which is why the platform has both.`,
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
    ` For the team of ${q.analysts}, reviewing around the clock, that is ${wholeDollars(day.value)} a day, and ${millions(year.value)} a year at the same rate. Same analysts, same transactions, same hours: only the order changes. The interval is a bootstrap over days, and the comparison is paired, each day ranked both ways.`,
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
    strong("The prices are stated, not discovered."),
    ` A review is priced at ${dollars(q.review_cost_dollars)} and ${fmt(q.recovery_rate * 100, 0)} percent of a fraud is assumed recovered by chargeback. Neither changes the order of the queue, only the dollars reported; a test holds the ranking to that. The queue here is ${fmt(q.queue_fraud_share * 100, 0)} percent fraud, richer than a real team's, because the synthetic stream is easier than real fraud.`,
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
    `Retrained on the leaky features, the same model changes its prediction on ${data.leak.rows_changed} of ${number.format(data.leak.test_rows)} test rows, and its PR-AUC moves by ${fmt(v.value, 5)} (95% CI ${range(v, 6)}): nothing a scoreboard would ever show. `,
    strong("That is the point of testing the features rather than the score."),
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

function honesty(data) {
  const s = data.models.synthetic.champion, r = data.models.real.champion;
  $("honesty-synth").textContent = `${fmt(s.value, 2)} (${range(s, 3)})`;
  $("honesty-real").textContent = `${fmt(r.value, 3)} (${range(r, 3)}), against a base rate of 0.035`;
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
    driftChart(data);
    retrainChart(data);
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
