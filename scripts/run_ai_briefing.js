#!/usr/bin/env node
import fs from 'fs/promises';
import path from 'path';
import { XMLParser } from 'fast-xml-parser';

const argv = new Set(process.argv.slice(2));
const isSample = argv.has('--sample');
const useYouTube = argv.has('--youtube'); // legacy
const useRegistry = argv.has('--registry') || (!isSample && !useYouTube);

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const RUNS_DIR = path.join(ROOT, 'runs', 'ai');
const INPUTS_DIR = path.join(ROOT, 'inputs');

const SUBDOMAINS = [
  'model_releases',
  'ai_regulation',
  'ai_security',
  'ai_infra',
  'ai_apps_tools',
  'ai_business_market'
];

function isoNow() {
  return new Date().toISOString();
}

function lenOrZero(x) {
  return Array.isArray(x) ? x.length : 0;
}

async function safeWriteJson(filePath, obj) {
  const tmp = filePath + '.tmp';
  await fs.writeFile(tmp, JSON.stringify(obj, null, 2) + '\n', 'utf8');
  await fs.rename(tmp, filePath);
}

function ymd(date = new Date()) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function clampInt(n, min, max) {
  const x = Number(n);
  if (!Number.isFinite(x)) return min;
  return Math.max(min, Math.min(max, Math.trunc(x)));
}

function freshnessHours(firstCredibleAt) {
  const t = Date.parse(firstCredibleAt);
  if (!Number.isFinite(t)) return null;
  const hrs = (Date.now() - t) / 36e5;
  return Math.max(0, Math.round(hrs));
}

function validateTopic(topic) {
  // Minimal structural validation for v0 spec output shape.
  const err = (m) => {
    const e = new Error(`Topic validation failed (${topic?.topic_id ?? 'unknown'}): ${m}`);
    e.topic = topic;
    throw e;
  };

  if (!topic || typeof topic !== 'object') err('not an object');
  if (topic.domain !== 'AI') err('domain must be AI');
  if (typeof topic.title !== 'string' || topic.title.length > 60) err('title missing/too long');
  if (typeof topic.intel_line !== 'string' || topic.intel_line.length > 140) err('intel_line missing/too long');

  if (topic.included_by_source_override !== undefined && typeof topic.included_by_source_override !== 'boolean') err('included_by_source_override must be boolean');
  if (topic.briefing_reason !== undefined && typeof topic.briefing_reason !== 'string') err('briefing_reason must be string');
  if (topic.confidence_rationale !== undefined) {
    if (!Array.isArray(topic.confidence_rationale)) err('confidence_rationale must be array');
    if (topic.confidence_rationale.length < 1 || topic.confidence_rationale.length > 3) err('confidence_rationale must be 1–3');
  }
  if (topic.keywords !== undefined) {
    if (!Array.isArray(topic.keywords)) err('keywords must be array');
    if (topic.keywords.length > 8) err('keywords max 8');
  }

  const ctx = topic.context;
  if (!ctx) err('context missing');
  if (typeof ctx.what_changed !== 'string' || ctx.what_changed.length > 80) err('context.what_changed missing/too long');
  if (typeof ctx.whos_impacted !== 'string' || ctx.whos_impacted.length > 80) err('context.whos_impacted missing/too long');
  if (typeof ctx.what_to_watch_next !== 'string' || ctx.what_to_watch_next.length > 80) err('context.what_to_watch_next missing/too long');

  const timeline = topic.timeline;
  if (!Array.isArray(timeline) || timeline.length < 3 || timeline.length > 6) err('timeline must be 3–6 items');
  for (const it of timeline) {
    if (!it || typeof it !== 'object') err('timeline item not object');
    if (typeof it.date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(it.date)) err('timeline.date invalid');
    if (typeof it.event !== 'string' || it.event.length > 90) err('timeline.event missing/too long');
    if (!Array.isArray(it.source_ids) || it.source_ids.length < 1) err('timeline.source_ids missing');
  }

  const entities = topic.entities;
  if (!Array.isArray(entities) || entities.length < 1 || entities.length > 8) err('entities must be 1–8');
  for (const e of entities) {
    if (typeof e !== 'string' || e.length < 1 || e.length > 40) err('entity invalid/too long');
  }

  const soe = topic.second_order_effects;
  if (!Array.isArray(soe) || soe.length !== 2) err('second_order_effects must be exactly 2');
  for (const s of soe) {
    if (typeof s !== 'string' || s.length < 1 || s.length > 110) err('second_order_effect invalid/too long');
  }

  const contr = topic.contradictions;
  if (!Array.isArray(contr) || contr.length > 3) err('contradictions must be 0–3');
  for (const c of contr) {
    if (typeof c !== 'string' || c.length < 1 || c.length > 120) err('contradiction invalid/too long');
  }

  const sources = topic.sources;
  if (!Array.isArray(sources) || sources.length < 1 || sources.length > 5) err('sources must be 1–5');
  for (const s of sources) {
    if (!s || typeof s !== 'object') err('source not object');
    if (typeof s.source_id !== 'string') err('source_id missing');
    if (typeof s.publisher !== 'string') err('source.publisher missing');
    if (typeof s.title !== 'string' || s.title.length > 90) err('source.title missing/too long');
    if (typeof s.url !== 'string') err('source.url missing');
    if (!['Primary','Trade','Mainstream','Analyst','Research','Influencer','Social'].includes(s.type)) err('source.type invalid');
    if (typeof s.published_at !== 'string') err('source.published_at missing');
    if (typeof s.retrieved_at !== 'string') err('source.retrieved_at missing');
  }

  const ts = topic.timestamps;
  if (!ts || typeof ts !== 'object') err('timestamps missing');
  for (const k of ['first_seen_at','last_updated_at','first_credible_at']) {
    if (typeof ts[k] !== 'string') err(`timestamps.${k} missing`);
  }

  const tags = topic.tags;
  if (!tags || typeof tags !== 'object') err('tags missing');
  if (typeof tags.subdomain !== 'string' || !tags.subdomain) err('tags.subdomain missing');

  const sc = topic.score;
  if (!sc || typeof sc !== 'object') err('score missing');
  sc.relevance = clampInt(sc.relevance, 0, 5);
  sc.impact = clampInt(sc.impact, 0, 5);
  sc.novelty = clampInt(sc.novelty, 0, 3);
  sc.credibility = clampInt(sc.credibility, 0, 5);
  sc.time_sensitivity = clampInt(sc.time_sensitivity, 0, 4);
  sc.total = clampInt(sc.total, 0, 22);

  return topic;
}

function pickSubdomainFromText(text) {
  const t = String(text || '').toLowerCase();
  if (/(sec|vuln|exploit|breach|jailbreak|prompt injection|injection)/.test(t)) return 'ai_security';
  if (/(regulat|policy|\blaw\b|\bact\b|compliance|agency|\beu\b|\bftc\b|\bdoj\b|white house|executive order)/.test(t)) return 'ai_regulation';
  if (/(gpu|nvidia|amd|accelerator|cuda|inference|training|cluster|datacenter|h100|b200|chip)/.test(t)) return 'ai_infra';
  if (/(copilot|workspace|m365|office|slack|zoom|notion|productivity|admin|audit|governance)/.test(t)) return 'ai_apps_tools';
  if (/(pricing|price|tier|billing|cost|enterprise|contract|acquis|ipo|funding|revenue)/.test(t)) return 'ai_business_market';
  if (/(model|llm|gpt|claude|gemini|openai|anthropic|meta|llama|mistral|release|preview|benchmark)/.test(t)) return 'model_releases';
  return 'ai_business_market';
}

function reasonLabelFromSubdomain(subdomain, text) {
  const t = String(text || '').toLowerCase();
  if (subdomain === 'ai_regulation') return 'Regulatory';
  if (subdomain === 'ai_security') return 'Risk';
  if (/(outage|incident|downtime)/.test(t)) return 'Operational';
  if (subdomain === 'ai_apps_tools') return 'Operational';
  if (subdomain === 'ai_infra' || subdomain === 'model_releases' || subdomain === 'ai_business_market') return 'Strategic';
  return 'Impact';
}

function computeCredibilityC(clusterSources) {
  // v0 deterministic credibility score 0–5 (type-based, explainable)
  const types = new Set(clusterSources.map((s) => s.type));
  const pubs = new Set(clusterSources.map((s) => s.publisher));
  let C = 0;
  if (types.has('Primary')) C += 3;
  else {
    const nonSocial = clusterSources.filter((s) => s.type !== 'Social');
    if (types.has('Trade') && types.has('Mainstream')) C += 2;
    else if (nonSocial.length >= 2) C += 2;
  }
  if (pubs.size >= 2) C += 1;

  // Influencer sources are not authoritative alone
  if (types.size === 1 && types.has('Influencer')) {
    C = Math.min(C, 2);
  }

  return clampInt(C, 0, 5);
}

function computeSubscores(subdomain, combinedText, sources) {
  // Keep simple deterministic heuristics; tune later without changing rubric ranges.
  const t = String(combinedText || '').toLowerCase();

  const R = (() => {
    if (subdomain === 'ai_security') return 5;
    if (subdomain === 'ai_regulation') return 4;
    if (subdomain === 'ai_apps_tools') return 4;
    if (subdomain === 'ai_infra') return 3;
    if (subdomain === 'model_releases') return 3;
    return 3;
  })();

  const I = (() => {
    if (/pricing|cost|tier|billing/.test(t)) return 4;
    if (/exploit|breach|vuln/.test(t)) return 4;
    if (/flagship|frontier|state of the art|sota/.test(t)) return 4;
    if (subdomain === 'ai_regulation') return 3;
    return 2;
  })();

  const N = 2; // v0 default; true novelty requires deeper logic

  const T = (() => {
    if (subdomain === 'ai_security') return 4;
    if (subdomain === 'ai_regulation') return 3;
    if (/outage|incident|downtime/.test(t)) return 4;
    return 2;
  })();

  return {
    relevance: clampInt(R, 0, 5),
    impact: clampInt(I, 0, 5),
    novelty: clampInt(N, 0, 3),
    time_sensitivity: clampInt(T, 0, 4)
  };
}

function confidenceFrom(C, clusterSources, contradictions) {
  const pubs = new Set(clusterSources.map((s) => s.publisher));
  const types = new Set(clusterSources.map((s) => s.type));
  const hasPrimary = types.has('Primary');
  const independent = pubs.size >= 2;
  const hasContradictions = Array.isArray(contradictions) && contradictions.length > 0;

  // Trust discipline:
  // - Never High unless Primary OR 2 independent sources, and no contradictions.
  // - Single-source topics are Low unless Primary.
  let conf = 'Low';

  const singleSource = clusterSources.length === 1;
  if (singleSource) {
    conf = hasPrimary ? 'Med' : 'Low';
  } else {
    if ((hasPrimary || independent) && !hasContradictions && C >= 4) conf = 'High';
    else if (C >= 3) conf = 'Med';
    else conf = 'Low';
  }

  // Influencer-only topics cannot exceed Medium confidence
  if (types.size === 1 && types.has('Influencer')) {
    if (conf === 'High') conf = 'Med';
  }

  return conf;
}

function passesEligibilityGates(C, R, clusterSources) {
  const types = new Set(clusterSources.map((s) => s.type));
  const nonSocial = clusterSources.filter((s) => s.type !== 'Social');
  const sourceCount = clusterSources.length;

  // Hard excludes
  if (C <= 1) return false;
  if (R <= 1) return false;
  if (types.size === 1 && types.has('Social')) return false;

  // Source count gate: >=2 except 1 allowed only if Primary
  if (sourceCount < 2) {
    return types.has('Primary');
  }

  // Credibility gate: C>=3, or urgent exception (not implemented in v0 heuristics)
  if (C < 3) return false;

  // Relevance gate
  if (R < 2) return false;

  // Require at least 2 non-social sources unless Primary
  if (!types.has('Primary') && nonSocial.length < 2) return false;

  return true;
}

function short(s, max) {
  const str = String(s || '').trim();
  if (str.length <= max) return str;
  return str.slice(0, max - 1).trimEnd() + '…';
}

const STOPWORDS = new Set([
  'the','and','for','with','from','this','that','your','into','over','about','new','today','you','are','was','were','will','its','it','how','why','what',
  'a','an','to','of','in','on','at','by','as','or','not','be','can','may','via','vs','is','we','our','their','they','i','my',
  'chapter','part','episode','live','stream','watch','review',
  // extra junk suppression
  'which','built','class','using','used','make','makes','making','like','than','then','into','here','there','also','about','best','good'
]);

function extractKeywords(text) {
  const toks = normalizeText(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .split(/\s+/)
    .map((x) => x.trim())
    .filter((x) => x && x.length >= 4 && !STOPWORDS.has(x));

  const out = [];
  const seen = new Set();
  for (const t of toks) {
    if (seen.has(t)) continue;
    seen.add(t);
    out.push(t);
    if (out.length >= 8) break;
  }
  return out;
}

function tokenizeTitle(s) {
  // Used for clustering. Keep as lowercase tokens without min-length=4 restriction.
  return normalizeText(s || '')
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .split(/\s+/)
    .filter((x) => x && x.length > 2 && !STOPWORDS.has(x));
}

function jaccard(a, b) {
  const A = new Set(a);
  const B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const union = new Set([...A, ...B]).size;
  return union ? inter / union : 0;
}

async function fetchText(url, { timeoutMs = 15000 } = {}) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ac.signal,
      headers: {
        // Some feeds (notably OpenAI) block generic bot UAs; use a browser-like UA.
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) Project411AI/0.0.2',
        'accept': 'application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7'
      }
    });
    if (!res.ok) throw new Error(`http_${res.status}`);
    return await res.text();
  } finally {
    clearTimeout(t);
  }
}

async function resolveChannelId(channelUrl) {
  const html = await fetchText(channelUrl);
  const m1 = html.match(/"channelId"\s*:\s*"(UC[a-zA-Z0-9_-]{20,})"/);
  if (m1) return m1[1];
  const m2 = html.match(/\/channel\/(UC[a-zA-Z0-9_-]{20,})/);
  if (m2) return m2[1];
  throw new Error(`could not resolve channelId for ${channelUrl}`);
}

async function ingestYouTubeSources() {
  // legacy ingestion; registry ingestion is preferred.
  const inPath = path.join(INPUTS_DIR, 'youtube_channels.json');
  const raw = await fs.readFile(inPath, 'utf8');
  const channels = JSON.parse(raw);
  const parser = new XMLParser({ ignoreAttributes: false });

  const outSources = [];
  let updated = false;

  for (const ch of channels) {
    if (!ch.channelId) {
      ch.channelId = await resolveChannelId(ch.url);
      updated = true;
    }
    const feedUrl = `https://www.youtube.com/feeds/videos.xml?channel_id=${encodeURIComponent(ch.channelId)}`;
    const xml = await fetchText(feedUrl);
    const parsed = parser.parse(xml);
    const entries = parsed?.feed?.entry;
    const arr = Array.isArray(entries) ? entries : entries ? [entries] : [];

    for (const e of arr) {
      const videoId = e?.['yt:videoId'] ?? e?.videoId;
      const title = e?.title;
      const pub = e?.published;
      const link = e?.link?.['@_href'] || (Array.isArray(e?.link) ? e.link[0]?.['@_href'] : null);
      const summary = e?.['media:group']?.['media:description'] ?? '';
      if (!videoId || !title || !pub) continue;

      outSources.push({
        source_id: `yt:${ch.channelId}:${videoId}`,
        publisher: ch.name || 'YouTube',
        title: String(title),
        url: link || `https://www.youtube.com/watch?v=${videoId}`,
        summary: String(summary || ''),
        type: 'Influencer',
        published_at: new Date(pub).toISOString(),
        retrieved_at: isoNow(),
        is_primary: false,
        _channelId: ch.channelId
      });
    }
  }

  if (updated) {
    await fs.writeFile(inPath, JSON.stringify(channels, null, 2) + '\n', 'utf8');
  }

  const seen = new Set();
  const deduped = [];
  for (const s of outSources.sort((a, b) => Date.parse(b.published_at) - Date.parse(a.published_at))) {
    if (seen.has(s.url)) continue;
    seen.add(s.url);
    deduped.push(s);
  }

  return deduped;
}

function toArray(x) {
  if (!x) return [];
  return Array.isArray(x) ? x : [x];
}

function stripHtml(s) {
  return String(s || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function textVal(x) {
  if (x === null || x === undefined) return null;
  if (typeof x === 'string' || typeof x === 'number') return String(x);
  if (typeof x === 'object') {
    if (typeof x['#text'] === 'string' || typeof x['#text'] === 'number') return String(x['#text']);
    if (typeof x.text === 'string' || typeof x.text === 'number') return String(x.text);
  }
  return String(x);
}

function parseRssOrAtom(xmlText) {
  const parser = new XMLParser({ ignoreAttributes: false });
  const parsed = parser.parse(xmlText);

  // RSS 2.0
  const rssItems = parsed?.rss?.channel?.item;
  if (rssItems) {
    return toArray(rssItems).map((it) => ({
      title: textVal(it.title),
      url: textVal(it.link),
      published: textVal(it.pubDate || it.published || it.date || it['dc:date']),
      summary: textVal(it.description || it['content:encoded'] || '')
    }));
  }

  // Atom
  const entries = parsed?.feed?.entry;
  if (entries) {
    return toArray(entries).map((e) => ({
      title: textVal(e.title),
      url:
        e.link?.['@_href'] ||
        (Array.isArray(e.link) ? e.link.find((l) => l['@_rel'] === 'alternate')?.['@_href'] : null) ||
        (Array.isArray(e.link) ? e.link[0]?.['@_href'] : null),
      published: textVal(e.published || e.updated),
      summary: textVal(e.summary) || ''
    }));
  }

  return [];
}

async function ingestFromRegistry(windowStartMs, windowEndMs) {
  const regPath = path.join(INPUTS_DIR, 'source_registry.json');
  const raw = await fs.readFile(regPath, 'utf8');
  const registry = JSON.parse(raw);

  const enabled = registry.filter((s) => s.enabled && s.rss_url);

  const sources = [];
  const nowIso = isoNow();

  // Track fetch stats and write back deterministically (no LLM)
  const statsByName = new Map();

  // Log HTML/parse issues (not shown in UI)
  const errLogPath = path.join(ROOT, 'runs', 'ai', 'rss-errors.log');

  for (const src of enabled) {
    let status = 'ok';
    let last_error = '';
    let usedCount = 0;

    try {
      const body = await fetchText(src.rss_url, { timeoutMs: 15000 });
      const head = String(body || '').trimStart().slice(0, 120);

      // Detect HTML masquerading as RSS
      if (/^<!doctype/i.test(head) || /^<html/i.test(head) || head.includes('<html')) {
        status = 'parse_error';
        last_error = 'non_xml_response';
        await fs.mkdir(path.dirname(errLogPath), { recursive: true });
        await fs.appendFile(errLogPath, `${nowIso}\t${src.name}\t${src.rss_url}\t${head.replace(/\s+/g, ' ')}\n`, 'utf8');
      } else {
        const items = parseRssOrAtom(body);
        // throttle influencers aggressively
        const maxItems = src.source_type === 'Influencer' ? 6 : 40;
        const used = items.slice(0, maxItems);
        usedCount = lenOrZero(used);

        if (usedCount === 0) {
          status = 'empty';
        }

        for (const it of used) {
          if (!it.title || !it.url || !it.published) continue;
          const pubMs = Date.parse(it.published);
          if (!Number.isFinite(pubMs)) continue;
          if (typeof windowStartMs === 'number' && Number.isFinite(windowStartMs) && pubMs < windowStartMs) continue;
          if (typeof windowEndMs === 'number' && Number.isFinite(windowEndMs) && pubMs > windowEndMs) continue;
          const published_at = new Date(pubMs).toISOString();
          sources.push({
            source_id: `${src.name}:${Buffer.from(String(it.url)).toString('base64url').slice(0, 24)}`,
            publisher: src.name,
            title: String(it.title),
            url: String(it.url),
            summary: stripHtml(it.summary || ''),
            type: src.source_type,
            published_at,
            retrieved_at: nowIso,
            is_primary: src.source_type === 'Primary'
          });
        }
      }
    } catch (e) {
      const msg = String(e?.message || e);
      last_error = msg.slice(0, 120);
      if (msg.includes('http_403') || msg.includes('http_401')) status = 'blocked';
      else if (msg.includes('http_')) status = 'blocked';
      else if (msg.includes('AbortError')) status = 'timeout';
      else status = 'parse_error';
    }

    statsByName.set(src.name, {
      last_fetched_at: nowIso,
      items_fetched_last_run: usedCount,
      status,
      last_error
    });
  }

  // Update registry fetch metadata (safe write)
  for (const s of registry) {
    const st = statsByName.get(s.name);
    if (st) {
      s.last_fetched_at = st.last_fetched_at;
      s.items_fetched_last_run = st.items_fetched_last_run;
      s.status = st.status;
      s.last_error = st.last_error;
    }
    if (typeof s.always_show !== 'boolean') s.always_show = false;
    if (typeof s.status !== 'string') s.status = 'empty';
    if (typeof s.last_error !== 'string') s.last_error = '';
  }
  await safeWriteJson(regPath, registry);

  function normalizeUrl(u) {
    try {
      const url = new URL(u);
      url.hash = '';
      // drop common tracking params
      for (const k of Array.from(url.searchParams.keys())) {
        if (k.startsWith('utm_') || ['fbclid','gclid','mc_cid','mc_eid'].includes(k)) url.searchParams.delete(k);
      }
      url.search = url.searchParams.toString() ? `?${url.searchParams.toString()}` : '';
      url.hostname = url.hostname.replace(/^www\./, '');
      return url.toString();
    } catch {
      return String(u || '');
    }
  }

  // dedupe by normalized URL
  const seen = new Set();
  const deduped = [];
  for (const s of sources.sort((a, b) => Date.parse(b.published_at) - Date.parse(a.published_at))) {
    const nu = normalizeUrl(s.url);
    s.url = nu;
    if (seen.has(nu)) continue;
    seen.add(nu);
    deduped.push(s);
  }

  return { sources: deduped, registry };
}

const ALIAS_MAP = [
  { re: /clawd\s*bot|clawdbot|clawed\s*bot|moltbot/i, canon: 'OpenClaw' },
  { re: /openclaw/i, canon: 'OpenClaw' },
  { re: /claude\s*code/i, canon: 'Claude Code' },
  { re: /copilot/i, canon: 'Copilot' }
];

const ENTITY_DICT = [
  { re: /\bopenai\b/i, name: 'OpenAI' },
  { re: /\bdeepmind\b/i, name: 'DeepMind' },
  { re: /\bgoogle\b/i, name: 'Google' },
  { re: /\banthropic\b/i, name: 'Anthropic' },
  { re: /\bmicrosoft\b/i, name: 'Microsoft' },
  { re: /\bnvidia\b/i, name: 'NVIDIA' },
  { re: /\bhugging\s*face\b/i, name: 'Hugging Face' },
  { re: /\barxiv\b/i, name: 'arXiv' },
  { re: /\beu\b|\beuropean\s+union\b/i, name: 'EU' },
  { re: /\bftc\b/i, name: 'FTC' },
  { re: /\bdoj\b/i, name: 'DOJ' }
];

function normalizeText(s) {
  let x = String(s || '').trim();
  for (const a of ALIAS_MAP) x = x.replace(a.re, a.canon);
  return x;
}

function extractPrimaryEntity(text) {
  const t = normalizeText(text || '');
  for (const a of ALIAS_MAP) {
    if (a.re.test(t)) return a.canon;
  }
  for (const e of ENTITY_DICT) {
    if (e.re.test(t)) return e.name;
  }
  return 'Other';
}

function extractEntities(text) {
  const raw = String(text || '');
  const out = [];
  const seen = new Set();

  const BAD_ENTITY_RE = /\b(Which|Read|Article|Enables|Built|Unveils)\b/;

  // Add canonical entities from our dictionaries first
  for (const a of ALIAS_MAP) {
    if (a.re.test(raw) && !seen.has(a.canon)) { seen.add(a.canon); out.push(a.canon); }
  }
  for (const e of ENTITY_DICT) {
    if (e.re.test(raw) && !seen.has(e.name)) { seen.add(e.name); out.push(e.name); }
  }

  // Simple proper-noun phrase capture (rule-based NER-ish)
  const candidates = raw.match(/\b[A-Z][a-zA-Z0-9&-]+(?:\s+[A-Z][a-zA-Z0-9&-]+){0,3}\b/g) || [];
  for (const c of candidates) {
    const clean = c.trim();
    if (clean.length < 3 || clean.length > 40) continue;
    if (BAD_ENTITY_RE.test(clean)) continue;
    const low = clean.toLowerCase();
    if (STOPWORDS.has(low)) continue;
    if (['The','A','An','In','On','At','For','With','From','By','And','Or','To','Of'].includes(clean)) continue;
    if (!seen.has(clean)) {
      seen.add(clean);
      out.push(clean);
      if (out.length >= 8) break;
    }
  }

  return out.slice(0, 8);
}

function storyFingerprint(src) {
  const combined = `${src.title || ''} ${src.summary || ''}`;
  const norm = normalizeText(combined);
  const subdomain = SUBDOMAINS.includes(pickSubdomainFromText(norm)) ? pickSubdomainFromText(norm) : 'ai_business_market';
  const entity = extractPrimaryEntity(norm);
  const kws = tokenizeTitle(norm).slice(0, 8);
  return { entity, subdomain, kws };
}

function clusterSourcesByTitle(sources) {
  // Deterministic cross-source clustering:
  // 1) Bucket by (primary_entity + subdomain)
  // 2) Within bucket, group by keyword overlap (permissive when entity != Other)

  const buckets = new Map();
  for (const src of sources) {
    const fp = storyFingerprint(src);
    src._fp = fp;
    const bucketKey = `${fp.entity}|${fp.subdomain}`;
    if (!buckets.has(bucketKey)) buckets.set(bucketKey, []);
    buckets.get(bucketKey).push(src);
  }

  const clusters = [];

  for (const [bucketKey, items] of buckets.entries()) {
    const [entity, subdomain] = bucketKey.split('|');
    const threshold = entity !== 'Other' ? 0.10 : 0.25;

    // greedy within-bucket clustering
    for (const src of items) {
      const kws = src._fp.kws;
      let best = null;
      let bestScore = 0;
      for (const c of clusters) {
        if (c.entity !== entity || c.subdomain !== subdomain) continue;
        const overlap = jaccard(kws, c.kws);
        if (overlap > bestScore) {
          bestScore = overlap;
          best = c;
        }
      }

      if (best && bestScore >= threshold) {
        best.sources.push(src);
        best.kws = Array.from(new Set([...best.kws, ...kws])).slice(0, 12);
      } else {
        clusters.push({ entity, subdomain, kws: kws.slice(0, 8), sources: [src] });
      }
    }
  }

  // sort sources inside clusters by published_at desc for consistent timelines
  for (const c of clusters) {
    c.sources.sort((a, b) => Date.parse(b.published_at) - Date.parse(a.published_at));
  }

  // sort clusters by recency
  clusters.sort((a, b) => Date.parse(b.sources[0]?.published_at || 0) - Date.parse(a.sources[0]?.published_at || 0));

  return clusters;
}

function inferPhenomenonTitle(clusterSources) {
  // Deterministic "editorial" title generator: remove creator framing and use phenomenon nouns.
  const allText = normalizeText(
    clusterSources.map((s) => `${s.title} ${s.summary || ''}`).join(' | ')
  ).toLowerCase();

  if (/openclaw|clawdbot/.test(allText)) return 'OpenClaw-style always-on agent workflows trend';
  if (/prompt injection|injection/.test(allText)) return 'Prompt-injection risks resurface for tool-using AI agents';
  if (/open\s*source/.test(allText) && /model/.test(allText)) return 'Open-source model momentum shifts competitive baseline';
  if (/deepfake/.test(allText)) return 'Real-time deepfake tooling spreads via open-source releases';
  if (/gpu|accelerator|h100|b200|nvidia/.test(allText)) return 'AI accelerator roadmap updates reshape capacity planning';
  if (/pricing|tier|billing|cost/.test(allText)) return 'AI vendor pricing changes force renewed cost planning';
  if (/regulat|policy|\blaw\b|\bact\b|compliance|agency/.test(allText)) return 'Policy guidance tightens around enterprise AI use';

  // fallback: compact keyword-based title
  const toks = tokenizeTitle(allText).slice(0, 6).map((t) => t[0].toUpperCase() + t.slice(1));
  const base = toks.length ? toks.join(' ') : 'AI briefing updates';
  return short(base, 60);
}

function inferIntelLine(clusterSources, phenomenonTitle) {
  const types = new Set(clusterSources.map((s) => s.type));
  const hasPrimary = types.has('Primary');
  const hasInfluencer = types.has('Influencer');
  const srcCount = clusterSources.length;

  // No hype; no "X says".
  if (srcCount === 1) {
    if (hasPrimary) {
      return short(`${phenomenonTitle}. Primary source posted an update; assess enterprise impact on cost, risk, or adoption.`, 140);
    }
    const kind = hasInfluencer ? 'creator source' : 'single source';
    return short(`${phenomenonTitle}. One ${kind} surfaced this; treat as preliminary and watch for confirmation.`, 140);
  }

  const mix = hasPrimary ? 'primary sources and coverage' : hasInfluencer ? 'creator coverage' : 'coverage';
  return short(`${phenomenonTitle}. Multiple ${mix} surfaced it; assess enterprise impact on cost, risk, or adoption.`, 140);
}

function buildTopicsFromClusters(dateStr, clusters, registry, windowStartMs) {
  const alwaysShowByPublisher = new Map((registry || []).map((s) => [s.name, !!s.always_show]));
  const allSources = clusters.flatMap((c) => c.sources);

  const excluded_reasons = {
    credibility: 0,
    relevance: 0,
    source_count: 0,
    outside_window: 0,
    diversity: 0,
    social_only: 0
  };

  const normClusters = clusters.map((c) => ({
    sources: c.sources
      .sort((a, b) => Date.parse(b.published_at) - Date.parse(a.published_at))
      .slice(0, 5)
  }));

  // For v0 policy update: do NOT force-fill to 3 sources; allow 1–5 sources per topic.
  const completed = normClusters.map((c) => ({ sources: [...c.sources].slice(0, 5) }));

  // clusters that can't produce a 3-source topic (spec requirement) count as source_count exclusions
  // For v0 policy update: allow 1–5 sources per topic candidate.
  const eligible = completed.filter((c) => {
    if (c.sources.length >= 1) return true;
    excluded_reasons.source_count += 1;
    return false;
  });

  const candidates = [];
  for (let i = 0; i < eligible.length; i++) {
    const c = eligible[i];
    const combinedText = c.sources.map((s) => `${s.title} ${s.summary || ''}`).join(' | ');
    const subdomain = SUBDOMAINS.includes(pickSubdomainFromText(combinedText))
      ? pickSubdomainFromText(combinedText)
      : 'ai_business_market';

    const title = short(inferPhenomenonTitle(c.sources), 60);
    const intel = inferIntelLine(c.sources, title);

    const publishedTimes = c.sources.map((s) => Date.parse(s.published_at)).filter(Number.isFinite);
    const firstCredMs = Math.min(...publishedTimes);
    const lastMs = Math.max(...publishedTimes);

    if (typeof windowStartMs === 'number' && Number.isFinite(windowStartMs) && firstCredMs < windowStartMs) {
      excluded_reasons.outside_window += 1;
      continue;
    }

    const firstCred = new Date(firstCredMs).toISOString();
    const last = new Date(lastMs).toISOString();

    let entities = extractEntities(c.sources.map((s) => `${s.title} ${s.summary || ''}`).join(' | '));
    const keywords = extractKeywords(combinedText);
    if (entities.length === 0) entities = [extractPrimaryEntity(combinedText)];

    const C = computeCredibilityC(c.sources);
    const subs = computeSubscores(subdomain, combinedText, c.sources);
    const score = {
      relevance: subs.relevance,
      impact: subs.impact,
      novelty: subs.novelty,
      credibility: C,
      time_sensitivity: subs.time_sensitivity,
      total: clampInt(subs.relevance + subs.impact + subs.novelty + C + subs.time_sensitivity, 0, 22)
    };

    const contradictions = [];

    const conf = confidenceFrom(C, c.sources, contradictions);
    const included_by_source_override = c.sources.some((s) => alwaysShowByPublisher.get(s.publisher));

    const pubs = Array.from(new Set(c.sources.map((s) => s.publisher)));
    const types = new Set(c.sources.map((s) => s.type));
    const hasPrimary = types.has('Primary');

    const briefing_reason = (() => {
      if (included_by_source_override) return 'Included by source override';
      if (c.sources.length === 1 && hasPrimary) return 'Primary in window';
      if (pubs.length >= 2) return '2 sources agree';
      if (c.sources.length === 1) return 'Single source';
      return 'In queue';
    })();

    const confidence_rationale = (() => {
      const out = [];
      if (c.sources.length === 1) out.push('Single source: confidence is Low unless Primary');
      if (hasPrimary) out.push('Primary source present: confidence capped at Medium unless corroborated');
      if (pubs.length >= 2) out.push('At least 2 independent publishers in cluster');
      if (!contradictions.length) out.push('No contradictions detected in sources');
      return out.slice(0, 3);
    })();

    const topic = {
      topic_id: `ai-${dateStr}-${String(i + 1).padStart(2, '0')}`,
      domain: 'AI',
      included_by_source_override,
      briefing_reason,
      confidence_rationale,
      title,
      intel_line: intel,
      context: {
        what_changed: short('New reporting surfaced this development and its implications.', 80),
        whos_impacted: short('Enterprise teams planning AI rollout, cost, governance, or security.', 80),
        what_to_watch_next: short('Primary docs, vendor statements, and measurable follow-up signals.', 80)
      },
      reason_label: reasonLabelFromSubdomain(subdomain, combinedText),
      confidence: conf,
      freshness_hours: freshnessHours(firstCred) ?? 0,
      timestamps: {
        first_seen_at: firstCred,
        last_updated_at: last,
        first_credible_at: firstCred
      },
      tags: { subdomain },
      score,
      timeline: (() => {
        const s0 = c.sources[0];
        const s1 = c.sources[1];
        const s2 = c.sources[2];
        const out = [];
        if (s0) out.push({ date: dateStr, event: short(`First surfaced via: ${s0.publisher}`, 90), source_ids: [s0.source_id] });
        if (s1) out.push({ date: dateStr, event: short(`Additional coverage: ${s1.publisher}`, 90), source_ids: [s1.source_id] });
        if (s2) out.push({ date: dateStr, event: short(`Additional coverage: ${s2.publisher}`, 90), source_ids: [s2.source_id] });
        while (out.length < 3) out.push({ date: dateStr, event: short('Additional coverage: (single source)', 90), source_ids: [s0?.source_id || 'unknown'] });
        return out.slice(0, 3);
      })(),
      entities: entities.map((e) => short(e, 40)).slice(0, 8),
      keywords,
      second_order_effects: [
        short('If this continues, expect accelerated enterprise evaluation and toolchain changes.', 110),
        short('If this continues, expect tighter governance and clearer ROI requirements.', 110)
      ],
      contradictions,
      sources: c.sources.map((s) => ({
        source_id: s.source_id,
        publisher: s.publisher,
        title: short(s.title, 90),
        url: s.url,
        type: s.type,
        published_at: s.published_at,
        retrieved_at: s.retrieved_at,
        is_primary: s.type === 'Primary'
      }))
    };

    candidates.push(validateTopic(topic));
  }

  // Rank all candidates (deterministic)
  candidates.sort((a, b) => (b.score.total - a.score.total)
    || (b.score.time_sensitivity - a.score.time_sensitivity)
    || (Date.parse(b.timestamps.last_updated_at) - Date.parse(a.timestamps.last_updated_at)));

  // Apply section-specific rules
  // - Briefing: require >=2 sources OR 1 source if Primary. Influencer-only topics must not enter briefing.
  // - Queue: allow 1–2 source topics; still exclude social-only.

  const eligibleForBriefing = [];
  const allCandidatesForQueue = [];
  const overrideIneligible = [];

  for (const t of candidates) {
    const srcs = t.sources || [];
    const types = new Set(srcs.map((s) => s.type));
    const hasPrimary = types.has('Primary');
    const influencerOnly = (types.size === 1 && types.has('Influencer'));

    if (types.size === 1 && types.has('Social')) {
      excluded_reasons.social_only += 1;
      continue;
    }

    // Always allow into queue consideration (within window already enforced earlier)
    allCandidatesForQueue.push(t);

    // Briefing source-count gate
    if (!(srcs.length >= 2 || (srcs.length === 1 && hasPrimary))) {
      excluded_reasons.source_count += 1;
      continue;
    }

    // Briefing trust gates
    const ok = passesEligibilityGates(t.score.credibility, t.score.relevance, srcs);
    if (!ok) {
      if (t.score.credibility < 3) excluded_reasons.credibility += 1;
      if (t.score.relevance < 2) excluded_reasons.relevance += 1;
      if (t.included_by_source_override) overrideIneligible.push(t);
      continue;
    }

    // Influencer-only topics go to queue, not briefing
    if (influencerOnly) {
      continue;
    }

    eligibleForBriefing.push(t);
  }

  // Build top-5 briefing with diversity constraint (max 2 per subdomain)
  const briefing_topics = [];
  const perSub = new Map();
  const inBriefing = new Set();

  for (const t of eligibleForBriefing) {
    const sd = t.tags.subdomain;
    const count = perSub.get(sd) || 0;
    if (count >= 2) {
      excluded_reasons.diversity += 1;
      continue;
    }
    briefing_topics.push(t);
    inBriefing.add(t.topic_id);
    perSub.set(sd, count + 1);
    if (briefing_topics.length >= 5) break;
  }

  // Queue = ranked remainder up to 20 (excluding anything already in briefing)
  const queued_topics = [];
  for (const t of allCandidatesForQueue) {
    if (inBriefing.has(t.topic_id)) continue;
    if (queued_topics.length >= 20) break;
    queued_topics.push(t);
  }

  // Inject override topics (cap 5) if they were excluded by trust gates, without exceeding 20.
  const injected = [];
  for (const t of overrideIneligible) {
    if (injected.length >= 5) break;
    if (queued_topics.length >= 20) break;

    const dup = briefing_topics.some((x) => x.title === t.title) || queued_topics.some((x) => x.title === t.title);
    if (dup) continue;

    queued_topics.push(t);
    injected.push(t);
  }

  const out = {
    candidate_count: candidates.length,
    eligible_count: eligibleForBriefing.length,
    briefing_topics,
    queued_topics,
    briefing_count: briefing_topics.length,
    queue_count: queued_topics.length,
    excluded_reasons
  };

  return out;
}

function sampleRun(dateStr) {
  // Pure sample output that conforms to the v0 AI topic shape.
  // NOTE: URLs are placeholders; replace with real sources when wiring ingestion.
  const now = isoNow();
  const baseTs = new Date(Date.now() - 6 * 3600 * 1000).toISOString();

  const mkSource = (id, publisher, title, url, type, pubHoursAgo) => ({
    source_id: id,
    publisher,
    title,
    url,
    type,
    published_at: new Date(Date.now() - pubHoursAgo * 3600 * 1000).toISOString(),
    retrieved_at: now,
    is_primary: type === 'Primary'
  });

  const topics = [
    {
      topic_id: `ai-${dateStr}-01`,
      domain: 'AI',
      title: 'Major model update shifts enterprise pricing',
      intel_line: 'A leading AI vendor changed pricing and limits, altering cost planning for large deployments.',
      context: {
        what_changed: 'Pricing tiers and rate limits changed for key model endpoints.',
        whos_impacted: 'Teams running production workloads with predictable monthly spend.',
        what_to_watch_next: 'Updated enterprise terms and any rollback within 72 hours.'
      },
      reason_label: 'Strategic',
      confidence: 'Med',
      freshness_hours: 6,
      timestamps: {
        first_seen_at: baseTs,
        last_updated_at: now,
        first_credible_at: baseTs
      },
      tags: { subdomain: 'ai_business_market' },
      score: { relevance: 5, impact: 4, novelty: 2, credibility: 3, time_sensitivity: 3, total: 17 },
      timeline: [
        { date: dateStr, event: 'Vendor posts updated pricing and usage limits for flagship models.', source_ids: ['s1'] },
        { date: dateStr, event: 'Trade press reports enterprise buyers re-check contract assumptions.', source_ids: ['s2'] },
        { date: dateStr, event: 'Analysts note potential margin/usage tradeoffs for heavy users.', source_ids: ['s3'] }
      ],
      entities: ['VendorCo', 'FlagshipModel', 'Enterprise API', 'Procurement'],
      second_order_effects: [
        'If sustained, expect renewed focus on cost controls and model routing strategies.',
        'If competitors respond, multi-vendor fallback may become default in RFPs.'
      ],
      contradictions: [
        'Primary post implies broad availability; trade coverage suggests staged rollout.'
      ],
      sources: [
        mkSource('s1', 'VendorCo Blog', 'Pricing update: new tiers and limits', 'https://example.com/primary-1', 'Primary', 6),
        mkSource('s2', 'AI Trade Daily', 'Enterprises reassess budgets after model pricing changes', 'https://example.com/trade-1', 'Trade', 5),
        mkSource('s3', 'Analyst Note', 'Pricing shift may alter workload placement decisions', 'https://example.com/analyst-1', 'Analyst', 4)
      ]
    },
    {
      topic_id: `ai-${dateStr}-02`,
      domain: 'AI',
      title: 'New AI security finding targets prompt-injection paths',
      intel_line: 'Researchers demonstrated prompt-injection techniques that can exfiltrate tool outputs in some setups.',
      context: {
        what_changed: 'A new writeup shows practical exploit chains in tool-using agents.',
        whos_impacted: 'Apps that let models browse docs, email, or internal tools.',
        what_to_watch_next: 'Vendor mitigations, patches, and reproducible PoCs shared publicly.'
      },
      reason_label: 'Risk',
      confidence: 'Med',
      freshness_hours: 8,
      timestamps: {
        first_seen_at: new Date(Date.now() - 8 * 3600 * 1000).toISOString(),
        last_updated_at: now,
        first_credible_at: new Date(Date.now() - 8 * 3600 * 1000).toISOString()
      },
      tags: { subdomain: 'ai_security' },
      score: { relevance: 5, impact: 4, novelty: 2, credibility: 3, time_sensitivity: 4, total: 18 },
      timeline: [
        { date: dateStr, event: 'Researchers publish prompt-injection exploit writeup and mitigations.', source_ids: ['s4'] },
        { date: dateStr, event: 'Security trade outlets summarize exposure for enterprise assistants.', source_ids: ['s5'] },
        { date: dateStr, event: 'Vendors acknowledge issue and recommend isolation/sandboxing.', source_ids: ['s6'] }
      ],
      entities: ['Research Team', 'Tool-using agents', 'RAG pipeline', 'Enterprise assistants'],
      second_order_effects: [
        'If exploited widely, expect stricter tool permissions and output filtering by default.',
        'If mitigations mature, agent deployments may shift toward safer execution sandboxes.'
      ],
      contradictions: [],
      sources: [
        mkSource('s4', 'Research Paper', 'Prompt injection in tool-using agents: attack paths and defenses', 'https://example.com/primary-2', 'Primary', 8),
        mkSource('s5', 'Security Trade', 'Why prompt injection remains the top risk for AI agents', 'https://example.com/trade-2', 'Trade', 7),
        mkSource('s6', 'Vendor Advisory', 'Guidance: reduce tool scope and sanitize model outputs', 'https://example.com/mainstream-1', 'Mainstream', 6)
      ]
    },
    {
      topic_id: `ai-${dateStr}-03`,
      domain: 'AI',
      title: 'Regulator issues draft AI guidance for workplace use',
      intel_line: 'A regulator released draft guidance on AI use at work, raising compliance expectations.',
      context: {
        what_changed: 'Draft guidance clarifies acceptable AI-assisted decisions and disclosures.',
        whos_impacted: 'HR, legal, and teams using AI for hiring or performance workflows.',
        what_to_watch_next: 'Comment deadlines and whether enforcement language hardens.'
      },
      reason_label: 'Regulatory',
      confidence: 'High',
      freshness_hours: 10,
      timestamps: {
        first_seen_at: new Date(Date.now() - 10 * 3600 * 1000).toISOString(),
        last_updated_at: now,
        first_credible_at: new Date(Date.now() - 10 * 3600 * 1000).toISOString()
      },
      tags: { subdomain: 'ai_regulation' },
      score: { relevance: 4, impact: 3, novelty: 2, credibility: 4, time_sensitivity: 3, total: 16 },
      timeline: [
        { date: dateStr, event: 'Agency publishes draft guidance covering AI in employment contexts.', source_ids: ['s7'] },
        { date: dateStr, event: 'Trade outlets highlight disclosure and audit expectations.', source_ids: ['s8'] },
        { date: dateStr, event: 'Stakeholders begin preparing public comments and internal reviews.', source_ids: ['s9'] }
      ],
      entities: ['US Agency', 'Employers', 'HR systems', 'Compliance'],
      second_order_effects: [
        'If adopted, expect broader internal policy and documentation requirements.',
        'If enforcement tightens, vendors may add audit logs and explainability features.'
      ],
      contradictions: [],
      sources: [
        mkSource('s7', 'US Agency', 'Draft guidance: AI systems in workplace decision-making', 'https://example.com/primary-3', 'Primary', 10),
        mkSource('s8', 'Workplace Tech Trade', 'What the draft AI guidance means for HR teams', 'https://example.com/trade-3', 'Trade', 9),
        mkSource('s9', 'Mainstream', 'Regulator seeks comments on AI rules for employment', 'https://example.com/mainstream-2', 'Mainstream', 9)
      ]
    },
    {
      topic_id: `ai-${dateStr}-04`,
      domain: 'AI',
      title: 'AI infra vendor announces new accelerator roadmap',
      intel_line: 'An AI infrastructure vendor outlined an accelerator roadmap that may shift capacity planning.',
      context: {
        what_changed: 'Roadmap details improved memory and interconnect for next-gen accelerators.',
        whos_impacted: 'Teams planning GPU clusters, model training, or inference scaling.',
        what_to_watch_next: 'GA timelines, pricing, and confirmed hyperscaler availability.'
      },
      reason_label: 'Strategic',
      confidence: 'Med',
      freshness_hours: 12,
      timestamps: {
        first_seen_at: new Date(Date.now() - 12 * 3600 * 1000).toISOString(),
        last_updated_at: now,
        first_credible_at: new Date(Date.now() - 12 * 3600 * 1000).toISOString()
      },
      tags: { subdomain: 'ai_infra' },
      score: { relevance: 3, impact: 3, novelty: 2, credibility: 3, time_sensitivity: 2, total: 13 },
      timeline: [
        { date: dateStr, event: 'Vendor keynote outlines accelerator roadmap and target workloads.', source_ids: ['s10'] },
        { date: dateStr, event: 'Trade coverage compares specs and expected availability windows.', source_ids: ['s11'] },
        { date: dateStr, event: 'Analysts note potential effects on inference cost curves.', source_ids: ['s12'] }
      ],
      entities: ['InfraVendor', 'NextGen Accelerator', 'Hyperscalers', 'GPU clusters'],
      second_order_effects: [
        'If timelines hold, expect procurement cycles to shift toward next-gen capacity.',
        'If supply stays tight, enterprises may diversify vendors or use more distillation.'
      ],
      contradictions: [],
      sources: [
        mkSource('s10', 'InfraVendor', 'Roadmap: next-gen accelerators for AI workloads', 'https://example.com/primary-4', 'Primary', 12),
        mkSource('s11', 'Chip Trade', 'What the new accelerator roadmap means for buyers', 'https://example.com/trade-4', 'Trade', 11),
        mkSource('s12', 'Analyst Note', 'Inference economics could shift with memory gains', 'https://example.com/analyst-2', 'Analyst', 10)
      ]
    },
    {
      topic_id: `ai-${dateStr}-05`,
      domain: 'AI',
      title: 'Productivity suite adds AI admin controls and audit logs',
      intel_line: 'A workplace suite added AI controls and auditing that improve governance for rollouts.',
      context: {
        what_changed: 'Admins gained policy toggles, logging, and scoped access for AI features.',
        whos_impacted: 'IT admins and security teams managing AI feature deployments.',
        what_to_watch_next: 'Default settings, data retention options, and GA dates by tenant.'
      },
      reason_label: 'Operational',
      confidence: 'Med',
      freshness_hours: 14,
      timestamps: {
        first_seen_at: new Date(Date.now() - 14 * 3600 * 1000).toISOString(),
        last_updated_at: now,
        first_credible_at: new Date(Date.now() - 14 * 3600 * 1000).toISOString()
      },
      tags: { subdomain: 'ai_apps_tools' },
      score: { relevance: 4, impact: 2, novelty: 2, credibility: 3, time_sensitivity: 2, total: 13 },
      timeline: [
        { date: dateStr, event: 'Vendor announces new AI admin policies and auditing features.', source_ids: ['s13'] },
        { date: dateStr, event: 'Trade outlets detail governance implications for enterprises.', source_ids: ['s14'] },
        { date: dateStr, event: 'Docs update shows logging fields and retention options.', source_ids: ['s15'] }
      ],
      entities: ['WorkSuiteCo', 'Admin console', 'Audit logs', 'IT governance'],
      second_order_effects: [
        'If controls are strong, adoption may accelerate with fewer policy exceptions.',
        'If defaults are permissive, expect a wave of rapid lock-down configurations.'
      ],
      contradictions: [],
      sources: [
        mkSource('s13', 'WorkSuiteCo', 'New AI admin controls and auditing for enterprise tenants', 'https://example.com/primary-5', 'Primary', 14),
        mkSource('s14', 'Workplace IT Trade', 'Governance gets easier with new AI admin controls', 'https://example.com/trade-5', 'Trade', 13),
        mkSource('s15', 'Docs', 'Audit log schema and retention options for AI features', 'https://example.com/primary-6', 'Primary', 13)
      ]
    }
  ].map(validateTopic);

  // compute freshness_hours from first_credible_at if missing
  for (const t of topics) {
    if (typeof t.freshness_hours !== 'number') {
      t.freshness_hours = freshnessHours(t.timestamps.first_credible_at) ?? 0;
    }
  }

  const briefing_topics = topics.slice(0, 5);
  const queued_topics = [];

  return {
    run_id: `ai-${dateStr}-morning`,
    domain: 'AI',
    briefing_date: dateStr,
    cadence: 'morning',
    topic_cap: 5,
    started_at: now,
    completed_at: now,
    window: {
      window_start: new Date(Date.now() - 36 * 3600 * 1000).toISOString(),
      window_end: now
    },
    candidate_count: topics.length,
    eligible_count: topics.length,
    briefing_count: briefing_topics.length,
    queue_count: queued_topics.length,
    excluded_reasons: { credibility: 0, relevance: 0, source_count: 0, outside_window: 0, diversity: 0, social_only: 0 },
    briefing_topics,
    queued_topics
  };
}

async function main() {
  const dateStr = ymd();
  await fs.mkdir(path.join(RUNS_DIR, dateStr), { recursive: true });

  let run;

  if (isSample) {
    run = sampleRun(dateStr);
  } else if (useRegistry) {
    const nowMs = Date.now();
    // Morning-only cadence: anchor the window end to today's 09:00 UTC.
    const d = new Date(nowMs);
    const windowEndMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 9, 0, 0);
    const effectiveEndMs = Math.min(nowMs, windowEndMs);
    const windowStartMs = effectiveEndMs - 72 * 3600 * 1000;

    const { sources, registry } = await ingestFromRegistry(windowStartMs, effectiveEndMs);
    // Store raw sources for audit/debug
    const sourcesPath = path.join(RUNS_DIR, dateStr, 'sources.json');
    await fs.writeFile(sourcesPath, JSON.stringify({ domain: 'AI', briefing_date: dateStr, sources }, null, 2) + '\n', 'utf8');

    const normalizedSources = sources.map((s) => ({ ...s, title: normalizeText(s.title), summary: normalizeText(s.summary || '') }));
    const clusters = clusterSourcesByTitle(normalizedSources);
    const built = buildTopicsFromClusters(dateStr, clusters, registry, windowStartMs);

    const now = isoNow();
    run = {
      run_id: `ai-${dateStr}-morning`,
      domain: 'AI',
      briefing_date: dateStr,
      cadence: 'morning',
      topic_cap: 5,
      started_at: now,
      completed_at: now,
      window: {
        window_start: new Date(windowStartMs).toISOString(),
        window_end: new Date(effectiveEndMs).toISOString()
      },
      candidate_count: built.candidate_count,
      eligible_count: built.eligible_count,
      briefing_count: built.briefing_count,
      queue_count: built.queue_count,
      excluded_reasons: built.excluded_reasons,
      briefing_topics: built.briefing_topics,
      queued_topics: built.queued_topics
    };
  } else if (useYouTube) {
    // legacy
    const sources = await ingestYouTubeSources();
    const sourcesPath = path.join(RUNS_DIR, dateStr, 'sources.json');
    await fs.writeFile(sourcesPath, JSON.stringify({ domain: 'AI', briefing_date: dateStr, sources }, null, 2) + '\n', 'utf8');

    const nowMs = Date.now();
    const windowStartMs = nowMs - 72 * 3600 * 1000;

    const normalizedSources = sources.map((s) => ({ ...s, title: normalizeText(s.title), summary: normalizeText(s.summary || '') }));
    const clusters = clusterSourcesByTitle(normalizedSources);
    const built = buildTopicsFromClusters(dateStr, clusters, [], windowStartMs);

    const now = isoNow();
    run = {
      run_id: `ai-${dateStr}-morning`,
      domain: 'AI',
      briefing_date: dateStr,
      cadence: 'morning',
      topic_cap: 5,
      started_at: now,
      completed_at: now,
      window: {
        window_start: new Date(windowStartMs).toISOString(),
        window_end: now
      },
      candidate_count: built.candidate_count,
      eligible_count: built.eligible_count,
      briefing_count: built.briefing_count,
      queue_count: built.queue_count,
      excluded_reasons: built.excluded_reasons,
      briefing_topics: built.briefing_topics,
      queued_topics: built.queued_topics
    };
  } else {
    run = sampleRun(dateStr);
  }

  // Validate output
  if (!Array.isArray(run.briefing_topics)) throw new Error('run.briefing_topics missing');
  if (!Array.isArray(run.queued_topics)) throw new Error('run.queued_topics missing');
  run.briefing_topics = run.briefing_topics.slice(0, 5).map(validateTopic);
  run.queued_topics = run.queued_topics.slice(0, 20).map(validateTopic);

  const outPath = path.join(RUNS_DIR, dateStr, 'run.json');
  await fs.writeFile(outPath, JSON.stringify(run, null, 2) + '\n', 'utf8');

  console.log(`Wrote ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
