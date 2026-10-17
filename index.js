const express = require('express');
const multer = require('multer');
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 1024 * 1024, files: 5 } });
const path = require('path');
const app = express();

const SECRET = process.env.INBOUND_SECRET;
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const TTL = 60 * 60; // 1 hour
const MAX_MSGS = 30;

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: false, limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Storage: Upstash Redis if configured, otherwise in-memory (resets on restart)
const mem = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of mem) if (v.exp < now) mem.delete(k);
}, 60000).unref();

async function redis(commands) {
  const r = await fetch(`${REDIS_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REDIS_TOKEN}` },
    body: JSON.stringify(commands),
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  return r.json();
}

async function getMsgs(addr) {
  if (REDIS_URL && REDIS_TOKEN) {
    const out = await redis([['LRANGE', `mb:${addr}`, 0, MAX_MSGS - 1]]);
    return (out[0].result || []).map((m) => JSON.parse(m));
  }
  const e = mem.get(addr);
  return e && e.exp > Date.now() ? e.msgs : [];
}

async function addMsg(addr, msg) {
  if (REDIS_URL && REDIS_TOKEN) {
    const key = `mb:${addr}`;
    await redis([['LPUSH', key, JSON.stringify(msg)], ['LTRIM', key, 0, MAX_MSGS - 1], ['EXPIRE', key, TTL]]);
    return;
  }
  const e = mem.get(addr) || { msgs: [] };
  e.msgs.unshift(msg);
  e.msgs = e.msgs.slice(0, MAX_MSGS);
  e.exp = Date.now() + TTL * 1000;
  mem.set(addr, e);
}

const validAddress = (a) =>
  typeof a === 'string' && /^[a-z0-9._-]{1,30}@svmail\.com$/.test(a.toLowerCase());

app.get('/api/messages', async (req, res) => {
  const address = String(req.query.address || '').toLowerCase();
  if (!validAddress(address)) return res.status(400).json({ error: 'Invalid address' });
  try {
    res.set('Cache-Control', 'no-store');
    res.json(await getMsgs(address));
  } catch (e) {
    res.status(500).json({ error: 'Inbox unavailable' });
  }
});

app.post('/api/inbound', async (req, res) => {
  if (!SECRET || req.get('x-inbound-secret') !== SECRET) return res.sendStatus(401);
  const { to, from, subject, text, date } = req.body || {};
  const address = String(to || '').toLowerCase();
  if (!validAddress(address)) return res.sendStatus(204);
  try {
    await addMsg(address, {
      from: String(from || '').slice(0, 200),
      subject: String(subject || '').slice(0, 300),
      text: String(text || '').slice(0, 5000),
      date: date || new Date().toISOString(),
    });
    res.sendStatus(200);
  } catch (e) {
    res.sendStatus(500);
  }
});


// Mailgun inbound route -> forward to /api/inbound/mailgun?key=INBOUND_SECRET
app.post('/api/inbound/mailgun', upload.any(), async (req, res) => {
  if (!SECRET || req.query.key !== SECRET) return res.sendStatus(401);
  const b = req.body || {};
  const address = String(b.recipient || '').toLowerCase();
  if (!validAddress(address)) return res.sendStatus(200);
  try {
    await addMsg(address, {
      from: String(b.from || b.sender || '').slice(0, 200),
      subject: String(b.subject || '').slice(0, 300),
      text: String(b['body-plain'] || '').slice(0, 5000),
      date: new Date().toISOString(),
    });
    res.sendStatus(200);
  } catch (e) {
    res.sendStatus(500);
  }
});

app.get('/healthz', (req, res) => res.send('ok'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Sinzu Mail on ${PORT}`));
module.exports = app;
