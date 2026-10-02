'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn } = require('child_process');

const PORT = process.env.PORT || 3000;

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const BOTS_DIR = path.join(DATA_DIR, 'bots');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
fs.mkdirSync(BOTS_DIR, { recursive: true });

// ให้บอท JS ที่อยู่ใน Volume หา discord.js ใน /app/node_modules เจอ
try {
  const link = path.join(BOTS_DIR, 'node_modules');
  try { fs.unlinkSync(link); } catch {}
  fs.symlinkSync(path.join(__dirname, 'node_modules'), link, 'dir');
} catch (e) {
  console.error('symlink node_modules ไม่สำเร็จ:', e.message);
}

// ---------- state ----------
let bots = {};
try { bots = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch {}
function save() {
  const tmp = STATE_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(bots, null, 1));
  fs.renameSync(tmp, STATE_FILE);
}

const procs = {};
const logs = {};
let shuttingDown = false;
const ANSI = /\x1b\[[0-9;]*[A-Za-z]/g;

function addLog(id, k, data) {
  const arr = logs[id] || (logs[id] = []);
  const lines = String(data).replace(ANSI, '').split(/\r?\n/);
  while (lines.length && lines[lines.length - 1] === '') lines.pop();
  for (const m of lines) arr.push({ t: Date.now(), k, m: m.slice(0, 2000) });
  if (arr.length > 400) arr.splice(0, arr.length - 400);
}

const codeFile = (b) => path.join(BOTS_DIR, b.id, b.lang === 'py' ? 'bot.py' : 'bot.js');
const readCode = (b) => { try { return fs.readFileSync(codeFile(b), 'utf8'); } catch { return ''; } };
const pub = (b, full) => Object.assign({
  id: b.id, name: b.name, lang: b.lang, status: b.status,
  startedAt: b.startedAt || 0, lastExit: b.lastExit || '', hasToken: !!b.token
}, full ? { code: readCode(b) } : {});

// ---------- start / stop ----------
function startBot(id) {
  const b = bots[id];
  if (!b || procs[id]) return;
  const file = codeFile(b);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  if (!fs.existsSync(file)) fs.writeFileSync(file, '');

  const env = Object.assign({}, process.env, { DISCORD_TOKEN: b.token || '', PYTHONUNBUFFERED: '1' });

  const py = b.lang === 'py';
  const child = spawn(py ? 'python3' : 'node', py ? ['-u', file] : [file], { cwd: path.dirname(file), env });
  procs[id] = child;
  b.status = 'running';
  b.desired = true;
  b.startedAt = Date.now();
  b.lastExit = '';
  save();
  addLog(id, 'sys', '▶ เริ่มรันบอท (' + (py ? 'Python' : 'JavaScript') + ')');

  child.stdout.on('data', (d) => addLog(id, 'out', d));
  child.stderr.on('data', (d) => addLog(id, 'err', d));

  const done = (status, msg) => {
    if (procs[id] !== child) return;
    delete procs[id];
    if (shuttingDown) return; // เซิร์ฟเวอร์ปิดเอง -> คงสถานะเดิมไว้ เพื่อเปิดใหม่อัตโนมัติ
    const cur = bots[id];
    if (!cur) return;
    cur.status = status;
    cur.desired = false;
    cur.lastExit = msg;
    save();
    addLog(id, status === 'error' ? 'err' : 'sys', msg);
  };
  child.on('error', (e) => done('error', '❌ สตาร์ทไม่ได้: ' + e.message));
  child.on('exit', (code, sig) => {
    if (child.stopRequested) return done('stopped', '⏹ หยุดบอทแล้ว');
    if (code === 0) return done('stopped', '✅ โค้ดทำงานจบเอง (exit 0)');
    done('error', '❌ บอทหยุดเพราะ error (' + (sig ? 'signal ' + sig : 'exit code ' + code) + ')');
  });
}

function stopBot(id) {
  return new Promise((resolve) => {
    const b = bots[id];
    const c = procs[id];
    if (!c) {
      if (b) { b.desired = false; if (b.status === 'running') b.status = 'stopped'; save(); }
      return resolve();
    }
    if (b) { b.desired = false; save(); }
    c.stopRequested = true;
    c.once('exit', () => resolve());
    c.kill('SIGTERM');
    setTimeout(() => { try { c.kill('SIGKILL'); } catch {} }, 4000);
  });
}

// ---------- http ----------
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let s = '', n = 0;
    req.on('data', (c) => {
      n += c.length;
      if (n > 1e6) { reject(new Error('ข้อมูลใหญ่เกินไป')); req.destroy(); return; }
      s += c;
    });
    req.on('end', () => { try { resolve(s ? JSON.parse(s) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const p = new URL(req.url, 'http://x').pathname;
    const M = req.method;

    if (M === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html')));
    }
    if (!p.startsWith('/api/')) return json(res, 404, { error: 'not found' });

    if (p === '/api/bots') {
      if (M === 'GET') {
        const list = Object.values(bots).sort((a, b) => (a.created || 0) - (b.created || 0));
        return json(res, 200, { bots: list.map((b) => pub(b)) });
      }
      if (M === 'POST') {
        const d = await readBody(req);
        if (d.lang !== 'py' && d.lang !== 'js') return json(res, 400, { error: 'lang ต้องเป็น py หรือ js' });
        const id = crypto.randomBytes(4).toString('hex');
        const b = {
          id, lang: d.lang, name: String(d.name || 'บอทใหม่').trim().slice(0, 40) || 'บอทใหม่',
          token: '', desired: false, status: 'stopped', startedAt: 0, lastExit: '', created: Date.now()
        };
        bots[id] = b;
        fs.mkdirSync(path.join(BOTS_DIR, id), { recursive: true });
        fs.writeFileSync(codeFile(b), typeof d.code === 'string' ? d.code : '');
        save();
        return json(res, 200, pub(b, true));
      }
    }

    const m = p.match(/^\/api\/bots\/([a-f0-9]{8})(?:\/(start|stop|logs))?$/);
    if (!m || !bots[m[1]]) return json(res, 404, { error: 'ไม่เจอบอทนี้' });
    const id = m[1], b = bots[id], sub = m[2];

    if (!sub && M === 'GET') return json(res, 200, pub(b, true));

    if (!sub && M === 'PUT') {
      const d = await readBody(req);
      if (typeof d.name === 'string' && d.name.trim()) b.name = d.name.trim().slice(0, 40);
      if (typeof d.code === 'string') fs.writeFileSync(codeFile(b), d.code);
      if (typeof d.token === 'string' && d.token.trim()) b.token = d.token.trim();
      save();
      return json(res, 200, pub(b, true));
    }

    if (!sub && M === 'DELETE') {
      await stopBot(id);
      fs.rmSync(path.join(BOTS_DIR, id), { recursive: true, force: true });
      delete bots[id];
      delete logs[id];
      save();
      return json(res, 200, { ok: true });
    }

    if (sub === 'start' && M === 'POST') {
      await stopBot(id);
      startBot(id);
      return json(res, 200, pub(b));
    }
    if (sub === 'stop' && M === 'POST') {
      await stopBot(id);
      if (b.status === 'error') { b.status = 'stopped'; save(); }
      return json(res, 200, pub(b));
    }
    if (sub === 'logs' && M === 'GET') return json(res, 200, { logs: logs[id] || [] });

    return json(res, 405, { error: 'method not allowed' });
  } catch (e) {
    console.error(e);
    try { json(res, 500, { error: e.message }); } catch {}
  }
});

// ---------- shutdown: ไม่แตะสถานะ เพื่อให้บูตใหม่แล้วเปิดบอทต่อ ----------
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const c of Object.values(procs)) { try { c.kill('SIGTERM'); } catch {} }
  setTimeout(() => process.exit(0), 1500);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('uncaughtException', (e) => console.error('uncaught:', e));
process.on('unhandledRejection', (e) => console.error('unhandled:', e));

// ---------- boot: เปิดบอทที่เคยรันอยู่กลับมาอัตโนมัติ ----------
let i = 0;
for (const b of Object.values(bots)) {
  if (b.desired) {
    setTimeout(() => {
      addLog(b.id, 'sys', '🔄 เซิร์ฟเวอร์รีสตาร์ท → เปิดบอทให้อัตโนมัติ');
      startBot(b.id);
    }, 800 + i++ * 1500);
  } else if (b.status === 'running') {
    b.status = 'stopped';
  }
}
save();

server.listen(PORT, '0.0.0.0', () => console.log('✅ Bot Host รันที่พอร์ต ' + PORT + ' | ข้อมูล: ' + DATA_DIR));
