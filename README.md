# Project 411 — AI v0 MVS

Minimum viable system to produce **one** AI briefing day end-to-end and render it in a minimal local dashboard UI.

## What this includes
- **Pipeline** (v0): generates `runs/ai/YYYY-MM-DD/run.json` (currently `--sample` mode).
- **Minimal UI**: loads the most recent `run.json` and renders:
  - AI topic list (up to 5 rows): title, intel line, reason label, confidence, freshness
  - Click-to-open drill-down card with sections in spec order

Constraints satisfied:
- No auth, no accounts
- No database required (filesystem JSON)
- No chat (v0)

## Requirements
- Node.js 18+ (works with Node 22)

## Install
```bash
cd project-411-ai-mvs
npm install
```

## Run the pipeline (sample)
```bash
npm run run:ai:sample
```

## Run the pipeline (YouTube ingestion)
This ingests the YouTube channels listed in `inputs/youtube_channels.json` and attempts to cluster videos into topics.

```bash
node scripts/run_ai_briefing.js --youtube
```

Notes:
- YouTube sources are mapped to the spec’s `sources[].type` enum as **Analyst** ("Influencer" isn’t an allowed value in v0).
- If clustering can’t find enough related videos to form 3-source topics, you may get fewer than 5 topics.

This writes:
- `runs/ai/YYYY-MM-DD/run.json`
- `runs/ai/YYYY-MM-DD/sources.json`

## Start the UI server
```bash
PORT=4110 npm start
```

Open:
- http://localhost:4110

Health check:
- http://localhost:4110/healthz

Latest run JSON:
- http://localhost:4110/api/run/latest

## Notes / next step
The current pipeline produces a **spec-shaped sample** run. To wire real ingestion:
- implement Stage 1 ingestion (RSS/API fetch)
- implement clustering
- add an LLM step to draft title/intel/context/timeline/entities/etc and scoring

