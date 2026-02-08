import express from 'express';
import fs from 'fs/promises';
import path from 'path';

const app = express();
const PORT = Number(process.env.PORT || 4110);

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname));
const RUNS_DIR = path.join(ROOT, 'runs', 'ai');
const REGISTRY_PATH = path.join(ROOT, 'inputs', 'source_registry.json');

app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(ROOT, 'public'), { etag: true, maxAge: '1h' }));

app.get('/healthz', (_req, res) => res.json({ ok: true }));

async function safeWriteJson(filePath, obj) {
  const tmp = filePath + '.tmp';
  await fs.writeFile(tmp, JSON.stringify(obj, null, 2) + '\n', 'utf8');
  await fs.rename(tmp, filePath);
}

async function findLatestRunPath() {
  let dates;
  try {
    dates = await fs.readdir(RUNS_DIR);
  } catch {
    return null;
  }
  const ymdDirs = dates.filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d)).sort();
  if (!ymdDirs.length) return null;
  const latestDate = ymdDirs[ymdDirs.length - 1];
  const p = path.join(RUNS_DIR, latestDate, 'run.json');
  return p;
}

app.get('/api/sources', async (_req, res) => {
  try {
    const raw = await fs.readFile(REGISTRY_PATH, 'utf8');
    const registry = JSON.parse(raw);
    res.json({ ok: true, sources: registry });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'registry_read_failed' });
  }
});

app.post('/api/sources/update', async (req, res) => {
  const name = String(req.body?.name || '').trim();
  const patch = req.body?.patch || {};
  const enabled = patch.enabled;
  const always_show = patch.always_show;

  if (!name) {
    res.status(400).json({ ok: false, error: 'missing_name' });
    return;
  }
  if (enabled !== undefined && typeof enabled !== 'boolean') {
    res.status(400).json({ ok: false, error: 'enabled_must_be_boolean' });
    return;
  }
  if (always_show !== undefined && typeof always_show !== 'boolean') {
    res.status(400).json({ ok: false, error: 'always_show_must_be_boolean' });
    return;
  }

  try {
    const raw = await fs.readFile(REGISTRY_PATH, 'utf8');
    const registry = JSON.parse(raw);
    const idx = registry.findIndex((s) => s && s.name === name);
    if (idx === -1) {
      res.status(404).json({ ok: false, error: 'source_not_found' });
      return;
    }

    const s = registry[idx];
    if (enabled !== undefined) s.enabled = enabled;
    if (always_show !== undefined) s.always_show = always_show;
    if (typeof s.always_show !== 'boolean') s.always_show = false;

    await safeWriteJson(REGISTRY_PATH, registry);
    res.json({ ok: true, source: s });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'registry_write_failed' });
  }
});

app.get('/api/run/latest', async (_req, res) => {
  const p = await findLatestRunPath();
  if (!p) {
    res.status(404).json({ ok: false, error: 'no_run_found' });
    return;
  }
  try {
    const raw = await fs.readFile(p, 'utf8');
    res.set('Content-Type', 'application/json; charset=utf-8');
    res.send(raw);
  } catch (e) {
    res.status(500).json({ ok: false, error: 'read_failed' });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Project 411 AI MVS UI: http://0.0.0.0:${PORT}`);
});
