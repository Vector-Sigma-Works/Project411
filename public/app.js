const $ = (sel) => document.querySelector(sel);

function fmtFreshness(hours) {
  if (typeof hours !== 'number' || !Number.isFinite(hours)) return '—';
  if (hours < 48) return `${hours}h`;
  const d = Math.round(hours / 24);
  return `${d}d`;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

let ACTIVE_KW = null;
let LAST_RUN = null;
let ACTIVE_PAGE = 'briefing';
let TOPIC_COUNT = 5;

function getDisplayTopics(run) {
  const briefing = Array.isArray(run?.briefing_topics) ? run.briefing_topics : [];
  const queued = Array.isArray(run?.queued_topics) ? run.queued_topics : [];

  const combined = briefing.concat(queued);
  const sliced = combined.slice(0, TOPIC_COUNT);
  const filtered = ACTIVE_KW ? sliced.filter((t) => (t.entities || []).includes(ACTIVE_KW)) : sliced;
  return { briefing, queued, display: filtered };
}

function renderList(run) {
  const list = $('#list');
  list.innerHTML = '';

  const { display: topics } = getDisplayTopics(run);
  if (!topics.length) {
    list.innerHTML = `<div class="row"><div class="title">No topics found.</div></div>`;
    return;
  }

  topics.forEach((t, idx) => {
    const row = document.createElement('div');
    row.className = 'row';
    row.dataset.topicId = t.topic_id;
    const overrideBadge = t.included_by_source_override ? `<span class="badge">Included by source override</span>` : '';
    const singleSourceBadge = (t.sources && t.sources.length === 1)
      ? `<span class="badge">Single source</span>`
      : '';

    row.innerHTML = `
      <div class="rowTop">
        <div class="title">${escapeHtml(t.title)}</div>
        <div class="badges">
          ${overrideBadge}
          ${singleSourceBadge}
          <span class="badge">${escapeHtml(t.reason_label)}</span>
          <span class="badge conf ${escapeHtml(t.confidence)}">${escapeHtml(t.confidence)}</span>
          <span class="badge">${escapeHtml(fmtFreshness(t.freshness_hours))}</span>
        </div>
      </div>
      <div class="intel">${escapeHtml(t.intel_line)}</div>
    `;

    row.addEventListener('click', () => {
      document.querySelectorAll('.row').forEach((x) => x.classList.remove('active'));
      row.classList.add('active');
      renderCard(t);
    });

    list.appendChild(row);

    if (idx === 0) {
      // auto-select first topic for speed
      row.classList.add('active');
      renderCard(t);
    }
  });
}

function renderKwState() {
  const el = $('#kwState');
  if (!el) return;
  if (!ACTIVE_KW) {
    el.textContent = '';
  } else {
    el.textContent = `Filter: ${ACTIVE_KW} (click again to clear)`;
  }
}

function wireKeywordClicks() {
  document.querySelectorAll('.chip').forEach((btn) => {
    btn.addEventListener('click', (ev) => {
      ev.preventDefault();
      const kw = btn.dataset.kw;
      if (!kw) return;
      ACTIVE_KW = (ACTIVE_KW === kw) ? null : kw;
      renderKwState();
      if (LAST_RUN) renderList(LAST_RUN);
    });
  });
}

function renderCard(t) {
  const card = $('#card');
  card.classList.remove('empty');

  const ctx = t.context || {};

  const contradictions = Array.isArray(t.contradictions) ? t.contradictions : [];
  const showContradictions = contradictions.length > 0;

  const rationale = Array.isArray(t.confidence_rationale) ? t.confidence_rationale : [];
  const showRationale = (typeof t.briefing_reason === 'string' && t.briefing_reason) || rationale.length;

  const firstSurf = t?.timestamps?.first_credible_at ? new Date(t.timestamps.first_credible_at).toISOString().slice(0, 10) : '—';
  const lastUpd = t?.timestamps?.last_updated_at ? new Date(t.timestamps.last_updated_at).toISOString().slice(0, 10) : '—';

  card.innerHTML = `
    <div class="cardHeader">
      <div>
        <div class="cardTitle">${escapeHtml(t.title)}</div>
        <div class="meta">${escapeHtml(t.domain)} • ${escapeHtml(t.reason_label)} • Confidence: ${escapeHtml(t.confidence)} • Freshness: ${escapeHtml(fmtFreshness(t.freshness_hours))} • First surfaced: ${escapeHtml(firstSurf)} • Last updated: ${escapeHtml(lastUpd)}</div>
      </div>
      <div class="badges">
        <span class="badge">${escapeHtml(t.reason_label)}</span>
        <span class="badge conf ${escapeHtml(t.confidence)}">${escapeHtml(t.confidence)}</span>
      </div>
    </div>

    <div class="section">
      <h3 class="sectionTitle">Context</h3>
      <div class="kv">
        <div class="k">What changed</div><div class="v">${escapeHtml(ctx.what_changed ?? '')}</div>
        <div class="k">Who’s impacted</div><div class="v">${escapeHtml(ctx.whos_impacted ?? '')}</div>
        <div class="k">What to watch next</div><div class="v">${escapeHtml(ctx.what_to_watch_next ?? '')}</div>
      </div>
    </div>

    <!-- Timeline hidden in v0 UI (data retained in JSON). -->

    <div class="section">
      <h3 class="sectionTitle">Entities</h3>
      <div class="chips">
        ${(t.entities || []).map((e) => `<button class="chip" data-kw="${escapeHtml(e)}">${escapeHtml(e)}</button>`).join('')}
      </div>
      <div class="hint">Click an entity to filter the list.</div>

      ${(t.keywords && t.keywords.length) ? `
        <details style="margin-top:10px">
          <summary class="small" style="cursor:pointer">Show keywords</summary>
          <div class="chips" style="margin-top:8px">
            ${t.keywords.map((k) => `<button class="chip" data-kw="${escapeHtml(k)}">${escapeHtml(k)}</button>`).join('')}
          </div>
        </details>
      ` : ''}
    </div>

    <div class="section">
      <h3 class="sectionTitle">If this continues…</h3>
      <ul class="ul">
        ${(t.second_order_effects || []).map((e) => `<li>${escapeHtml(e)}</li>`).join('')}
      </ul>
    </div>

    ${showRationale ? `
      <div class="section">
        <h3 class="sectionTitle">Rationale</h3>
        <div class="kv">
          <div class="k">Briefing reason</div><div class="v">${escapeHtml(t.briefing_reason || '—')}</div>
        </div>
        ${rationale.length ? `
          <ul class="ul" style="margin-top:8px">
            ${rationale.map((x) => `<li>${escapeHtml(x)}</li>`).join('')}
          </ul>
        ` : ''}
      </div>
    ` : ''}

    ${showContradictions ? `
      <div class="section">
        <h3 class="sectionTitle">Contradictions</h3>
        <ul class="ul">
          ${contradictions.map((c) => `<li>${escapeHtml(c)}</li>`).join('')}
        </ul>
      </div>
    ` : ''}

    <div class="section">
      <h3 class="sectionTitle">Sources</h3>
      <div class="sources">
        ${(t.sources || []).map((s) => `
          <div class="source">
            <div class="sourceTop">
              <div class="sourcePub">${escapeHtml(s.publisher)}</div>
              <div class="sourceType">${escapeHtml(s.type)}</div>
            </div>
            <div class="sourceTitle">${escapeHtml(s.title)}</div>
            <div class="sourceLink"><a href="${escapeHtml(s.url)}" target="_blank" rel="noreferrer">Open source</a></div>
          </div>
        `).join('')}
      </div>
    </div>
  `;

  wireKeywordClicks();
}

function setPage(page) {
  ACTIVE_PAGE = page;
  $('#pageBriefing').hidden = page !== 'briefing';
  $('#pageSources').hidden = page !== 'sources';
  $('#navBriefing').classList.toggle('active', page === 'briefing');
  $('#navSources').classList.toggle('active', page === 'sources');
}

function mkSwitch(on) {
  const sw = document.createElement('div');
  sw.className = 'switch' + (on ? ' on' : '');
  sw.innerHTML = `<div class="knob"></div>`;
  return sw;
}

const IS_GITHUB_PAGES = location.hostname.endsWith('github.io');

function apiPath(p) {
  // Use relative paths so GitHub Pages subpaths (/<repo>/) work.
  return String(p || '').replace(/^\//, '');
}

async function loadSources() {
  const box = $('#sources');
  box.innerHTML = 'Loading sources…';

  // On GitHub Pages we have static JSON only (no write-back toggles).
  const url = IS_GITHUB_PAGES ? apiPath('data/source_registry.json') : apiPath('api/sources');
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) {
    box.innerHTML = 'Failed to load registry.';
    return;
  }
  const data = await res.json();
  const sources = Array.isArray(data) ? data : (data.sources || []);

  const wrap = document.createElement('div');
  wrap.className = 'tableWrap';

  const table = document.createElement('table');
  table.className = 'table';
  table.innerHTML = `
    <thead>
      <tr>
        <th class="th">Name</th>
        <th class="th">Type</th>
        <th class="th">Enabled</th>
        <th class="th">Always Show</th>
        <th class="th">RSS URL</th>
        <th class="th">Last fetched</th>
        <th class="th">Items (last run)</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;

  const tbody = table.querySelector('tbody');

  for (const s of sources) {
    const tr = document.createElement('tr');

    const tdName = document.createElement('td');
    tdName.className = 'td';
    tdName.innerHTML = `<strong>${escapeHtml(s.name)}</strong><div class="small">${escapeHtml(s.homepage_url || '')}</div>`;

    const tdType = document.createElement('td');
    tdType.className = 'td';
    tdType.textContent = s.source_type;

    const tdEnabled = document.createElement('td');
    tdEnabled.className = 'td';
    const swEnabled = mkSwitch(!!s.enabled);
    tdEnabled.appendChild(swEnabled);

    const tdAlways = document.createElement('td');
    tdAlways.className = 'td';
    const swAlways = mkSwitch(!!s.always_show);
    tdAlways.appendChild(swAlways);

    const tdRss = document.createElement('td');
    tdRss.className = 'td';
    tdRss.innerHTML = s.rss_url ? `<a href="${escapeHtml(s.rss_url)}" target="_blank" rel="noreferrer" style="color:var(--accent);text-decoration:none">RSS</a>` : '<span class="small">(none)</span>';

    const tdLast = document.createElement('td');
    tdLast.className = 'td';
    tdLast.textContent = s.last_fetched_at ? String(s.last_fetched_at).slice(0, 19).replace('T', ' ') : '—';

    const tdCount = document.createElement('td');
    tdCount.className = 'td';
    tdCount.textContent = (s.items_fetched_last_run ?? '—');

    async function updateSource(patch) {
      if (IS_GITHUB_PAGES) {
        alert('GitHub Pages demo mode: source toggles are disabled (static JSON only).');
        return false;
      }
      const resp = await fetch(apiPath('api/sources/update'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: s.name, patch })
      });
      if (!resp.ok) {
        alert('Update failed');
        return false;
      }
      const out = await resp.json();
      return !!out.ok;
    }

    swEnabled.addEventListener('click', async () => {
      const next = !swEnabled.classList.contains('on');
      const ok = await updateSource({ enabled: next });
      if (ok) swEnabled.classList.toggle('on', next);
    });

    swAlways.addEventListener('click', async () => {
      const next = !swAlways.classList.contains('on');
      const ok = await updateSource({ always_show: next });
      if (ok) swAlways.classList.toggle('on', next);
    });

    tr.appendChild(tdName);
    tr.appendChild(tdType);
    tr.appendChild(tdEnabled);
    tr.appendChild(tdAlways);
    tr.appendChild(tdRss);
    tr.appendChild(tdLast);
    tr.appendChild(tdCount);

    tbody.appendChild(tr);
  }

  wrap.appendChild(table);
  box.innerHTML = '';
  box.appendChild(wrap);
}

async function loadLatest() {
  const meta = $('#meta');
  meta.textContent = 'Loading latest run…';

  const url = IS_GITHUB_PAGES ? apiPath('data/run.latest.json') : apiPath('api/run/latest');
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) {
    meta.textContent = IS_GITHUB_PAGES
      ? 'No demo run.json found.'
      : 'No run.json found. Run: npm run run:ai:sample';
    $('#list').innerHTML = '';
    $('#card').innerHTML = 'Select a topic to view the drill-down card.';
    $('#card').classList.add('empty');
    return;
  }
  const run = await res.json();
  LAST_RUN = run;

  const briefingCount = run.briefing_count ?? (run.briefing_topics || []).length;
  const queueCount = run.queue_count ?? (run.queued_topics || []).length;

  const refreshedAt = run.completed_at || run.started_at || null;
  let refreshedStr = '—';
  try {
    if (refreshedAt) {
      refreshedStr = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
      }).format(new Date(refreshedAt)) + ' ET';
    }
  } catch {}

  meta.textContent = `Date: ${run.briefing_date} • Refreshed: ${refreshedStr} • Briefing: ${briefingCount}/5 • Queue: ${queueCount}`;

  const note = $('#note');
  if (briefingCount < 5) {
    note.hidden = false;
    note.textContent = `Only ${briefingCount} topics met trust gates today.`;
  } else {
    note.hidden = true;
    note.textContent = '';
  }

  renderKwState();
  renderList(run);
}

$('#reload').addEventListener('click', () => {
  if (ACTIVE_PAGE === 'sources') loadSources();
  else loadLatest();
});

$('#navBriefing').addEventListener('click', () => {
  setPage('briefing');
});
$('#navSources').addEventListener('click', async () => {
  setPage('sources');
  await loadSources();
});

$('#topicCount').addEventListener('change', () => {
  TOPIC_COUNT = Number($('#topicCount').value) || 5;
  if (LAST_RUN) renderList(LAST_RUN);
});

setPage('briefing');
loadLatest().catch((e) => {
  console.error(e);
  $('#meta').textContent = 'Failed to load.';
});
