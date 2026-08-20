# ADR 0012: Keep dashboard source in main and history data separate

- Status: accepted
- Date: 2026-08-20

## Context

The benchmark dashboard is a small static website. Generating its HTML and JavaScript from Python made the page difficult to edit and review, while a separate generated `benchmark-pages` branch duplicated the deployment source. Benchmark history still benefits from a generated aggregate data file because the browser should not load and combine every historical run independently.

## Decision

Keep the dashboard as hand-authored `index.html`, `app.js`, and `style.css` files in `docs/benchmarks/` on `main`. Configure GitHub Pages to serve `/docs` from `main`. Keep benchmark history data in the generated `benchmark-history` branch and have the dashboard fetch its `site/data.json` at runtime.

Use a small data-only history generator to aggregate normalized run records. It must not generate or contain the dashboard's HTML, CSS, or JavaScript. The benchmark workflow owns history-data updates but does not need Pages deployment permissions or a separate dashboard-publishing workflow.

## Consequences

- Dashboard code is easy to edit, review, test, and preview as ordinary static assets.
- There is one source of truth for page code and no generated dashboard branch to synchronize.
- Benchmark data remains out of `main` and can grow independently from application source.
- GitHub Pages updates when dashboard code changes on `main`; history updates remain independent data-branch commits.
- The page depends on public raw-content access to the history branch and must tolerate unavailable or malformed data.
