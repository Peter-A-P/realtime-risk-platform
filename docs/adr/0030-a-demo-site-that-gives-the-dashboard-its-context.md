# 30. A demo site that gives the dashboard its context

- Status: accepted, 2026-09-28
- Date: 2026-09-28
- Deciders: Peter Parker (that the project needs a designed page to the
  standard of the portfolio's other demos, with the live dashboard linked at
  its very top, because the dashboard on its own tells a visitor very little;
  2026-09-28); the build session (the page's content, its data, and where it
  is served)

## Context

The live dashboard at `risk.peterparker.ca` (ADR 14) is the platform's own
monitoring: Grafana panels of decisions a second, latency percentiles, hops
and lags, built for the person running the system. A visitor who is not that
person sees lines with no baseline, spikes with no explanation (a spot
reclaim and its catch-up look like an outage), and none of the measured
results the repository is built to produce. The portfolio's other projects
each have a demo page (`capacity`, `targeting`, `adaptive`, `finishline`)
that explains its result to a reader with no technical background and still
gives a technical reader the numbers, intervals and sources. This project had
only the dashboard.

## Decision

**A static page, `site/`, served at `fraud.peterparker.ca`**, in the design the
other demo pages share, whose first element is a link to the live dashboard
and a line saying which day of the live window it is. Below it the page
explains, with charts:

- one transaction's five steps, timed per hop at each measured load;
- latency against load, with the budget drawn on;
- the 72-hour dry run's spot reclaims and recoveries;
- the drift monitors over the replayed regime schedule;
- what retraining could and could not recover, and the approval path;
- the review queue's two rankings;
- the leak the point-in-time test caught;
- a panel-by-panel guide to reading the dashboard, including why its p99
  reads higher than the load test's (it is estimated from histogram buckets);
- the sealed schedule's fingerprints;
- what the page does not show.

**Every figure comes from a committed report.** `verdict site-export` reads
the reports in `docs/` (the same ones the README cites) and writes
`site/results.json`; `app.js` only formats and draws it. A test holds the
committed file to a fresh export, so the page cannot fall behind the reports,
and every interval in it to containing its estimate.

**The same host and policy as the other demos**: Azure Static Web Apps on the
free tier, deployed from this machine with the SWA CLI, so nothing is
committed that could publish to it; the content security policy allows
nothing off-origin and no inline script or style, and `verdict site-serve`
sends the same headers locally so a page the policy would break is seen
before it is published. `docs/site.md` is the runbook.

**Its own hostname, not the dashboard's.** Moving Grafana off
`risk.peterparker.ca` would mean changing the tunnel's public hostname,
Grafana's root URL and the link in every alert email, on the live stack,
during the live window, for a name. The page takes a new name and the
dashboard keeps its own.

## Consequences

- The live numbers stay on the dashboard, and the page says so. When the live
  window ends, its report fills the README's live table, and the export can
  carry the same numbers to the page.
- The page is one more thing to redeploy when a report it reads changes; the
  export test fails in CI until the data file is regenerated, which is the
  reminder.
- Nothing about the platform changes: no port, no route, no credential on the
  live stack.

## Sources

- Azure Static Web Apps, free plan and custom domains:
  https://learn.microsoft.com/en-us/azure/static-web-apps/plans and
  https://learn.microsoft.com/en-us/azure/static-web-apps/custom-domain-external
- Deploying with the SWA CLI:
  https://learn.microsoft.com/en-us/azure/static-web-apps/static-web-apps-cli-deploy
- Content Security Policy: https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP
- ADR 9 (the latency budget), ADR 14 (the live stack and its dashboard),
  ADR 25 (latency while serving and availability).
