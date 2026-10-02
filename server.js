'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const util = require('util');
const { spawn } = require('child_process');
const scrypt = util.promisify(crypto.scrypt);

const PORT = process.env.PORT || 3000;
const MAX_BOTS = parseInt(process.env.MAX_BOTS_PER_USER || '3', 10);
const SIGNUP_OPEN = process.env.SIGNUP !== 'off';

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const BOTS_DIR = path.join(DATA_DIR, 'bots');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
fs.mkdirSync(BOTS_DIR, { recursive: true });

// แยกผู้ใช้ด้วย uid ของระบบ (ทำได้เมื่อเซิร์ฟเวอร์รันเป็น root) ปิดได้ด้วย ISOLATION=off
const ISOLATE = typeof process.getuid === 'function' && process.getuid() === 0 && process.env.ISOLATION !== 'off';
if (ISOLATE) {
  try { fs.chmodSync(DATA_DIR, 0o711); fs.chmodSync(BOTS_DIR, 0o711); } catch (e) { console.error('chmod:', e.message); }
}

// ให้บอท JS ที่อยู่ใน Volume หา discord.js ใน /app/node_modules เจอ
try {
  const link = path.join(BOTS_DIR, 'node_modules');
  try { fs.unlinkSync(link); } catch {}
  fs.symlinkSync(path.join(__dirname, 'node_modules'), link, 'dir');
} catch (e) {
  console.error('symlink node_modules ไม่สำเร็จ:', e.message);
}

// ---------- state ----------
let state = { users: {}, sessions: {}, bots: {}, nextUid: 20000 };
try {
  const raw = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  if (raw && raw.users && raw.bots) state = Object.assign(state, raw);
  else if (raw && typeof raw === 'object') state.bots = raw; // ไฟล์รุ่นเก่า (ยังไม่มีผู้ใช้)
} catch {}
const bots = state.bots;

function save() {
  const tmp = STATE_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(state, null, 1), { mode: 0o600 });
  fs.renameSync(tmp, STATE_FILE);
  try { fs.chmodSync(STATE_FILE, 0o600); } catch {}
}

const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const getUser = (u) => (hasOwn(state.users, u) ? state.users[u] : null);

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

// ตั้งสิทธิ์โฟลเดอร์/ไฟล์ของบอท ให้เจ้าของเท่านั้นที่อ่านได้
function fixPerm(b) {
  if (!ISOLATE) return;
  const u = getUser(b.owner);
  if (!u) return;
  const dir = path.join(BOTS_DIR, b.id);
  try {
    fs.chownSync(dir, u.uid, u.uid);
    fs.chmodSync(dir, 0o700);
    const f = codeFile(b);
    if (fs.existsSync(f)) { fs.chownSync(f, u.uid, u.uid); fs.chmodSync(f, 0o600); }
  } catch (e) { console.error('fixPerm:', e.message); }
}

// ---------- start / stop ----------
function startBot(id) {
  const b = bots[id];
  if (!b || procs[id]) return;
  const owner = getUser(b.owner);
  if (!owner) {
    b.status = 'error'; b.desired = false; b.lastExit = 'บอทนี้ไม่มีเจ้าของ'; save();
    addLog(id, 'err', 'บอทนี้ไม่มีเจ้าของ จึงไม่รันให้');
    return;
  }
  const file = codeFile(b);
  const dir = path.dirname(file);
  fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(file)) fs.writeFileSync(file, '');
  fixPerm(b);

  // ส่ง env เฉพาะที่จำเป็น ไม่ส่งตัวแปรลับของเซิร์ฟเวอร์ให้โค้ดผู้ใช้
  const env = {
    PATH: process.env.PATH || '/usr/local/bin:/usr/bin:/bin',
    HOME: dir, TMPDIR: dir, LANG: 'C.UTF-8',
    DISCORD_TOKEN: b.token || '',
    PYTHONUNBUFFERED: '1', PYTHONDONTWRITEBYTECODE: '1'
  };
  const opts = { cwd: dir, env };
  if (ISOLATE) { opts.uid = owner.uid; opts.gid = owner.uid; }

  const py = b.lang === 'py';
  let child;
  try {
    child = spawn(py ? 'python3' : 'node', py ? ['-u', file] : [file], opts);
  } catch (e) {
    b.status = 'error'; b.desired = false; b.lastExit = 'สตาร์ทไม่ได้: ' + e.message; save();
    addLog(id, 'err', b.lastExit);
    return;
  }
  procs[id] = child;
  b.status = 'running';
  b.desired = true;
  b.startedAt = Date.now();
  b.lastExit = '';
  save();
  addLog(id, 'sys', 'เริ่มรันบอท (' + (py ? 'Python' : 'JavaScript') + ')');

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
  child.on('error', (e) => {
    let msg = 'สตาร์ทไม่ได้: ' + e.message;
    if (ISOLATE && (e.code === 'EPERM' || e.code === 'EACCES')) msg += ' (ระบบแยกผู้ใช้ใช้ไม่ได้บนโฮสต์นี้ ดูตัวแปร ISOLATION)';
    done('error', msg);
  });
  child.on('exit', (code, sig) => {
    if (child.stopRequested) return done('stopped', 'หยุดบอทแล้ว');
    if (code === 0) return done('stopped', 'โค้ดทำงานจบเอง (exit 0)');
    done('error', 'บอทหยุดเพราะ error (' + (sig ? 'signal ' + sig : 'exit code ' + code) + ')');
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

// ---------- session / auth ----------
const SESSION_MS = 30 * 24 * 3600 * 1000;
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');

function getCookie(req, name) {
  const c = req.headers.cookie || '';
  for (const part of c.split(';')) {
    const i = part.indexOf('=');
    if (i > 0 && part.slice(0, i).trim() === name) {
      try { return decodeURIComponent(part.slice(i + 1).trim()); } catch { return ''; }
    }
  }
  return '';
}
function sessCookie(req, tok, maxAge) {
  const secure = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() === 'https';
  return 'sid=' + tok + '; HttpOnly; SameSite=Lax; Path=/; Max-Age=' + maxAge + (secure ? '; Secure' : '');
}
function newSession(user) {
  const tok = crypto.randomBytes(32).toString('hex');
  state.sessions[sha(tok)] = { user, exp: Date.now() + SESSION_MS };
  save();
  return tok;
}
function userOf(req) {
  const tok = getCookie(req, 'sid');
  if (!tok) return null;
  const s = state.sessions[sha(tok)];
  if (!s || s.exp < Date.now()) return null;
  return getUser(s.user) ? s.user : null;
}

const ipOf = (req) => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const loginFails = new Map(), regCount = new Map();
function tooMany(map, ip, max, win) {
  const f = map.get(ip);
  return !!f && Date.now() - f.t < win && f.n >= max;
}
function bump(map, ip, win) {
  const f = map.get(ip);
  if (!f || Date.now() - f.t >= win) map.set(ip, { n: 1, t: Date.now() });
  else f.n++;
}

// ---------- http ----------
function json(res, code, obj, headers) {
  res.writeHead(code, Object.assign({ 'Content-Type': 'application/json; charset=utf-8' }, headers || {}));
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

    const ip = ipOf(req);

    // ----- สมัครสมาชิก (สมัครเสร็จเข้าสู่ระบบให้เลย) -----
    if (p === '/api/register' && M === 'POST') {
      if (!SIGNUP_OPEN) return json(res, 403, { error: 'ปิดรับสมัครอยู่' });
      if (tooMany(regCount, ip, 5, 3600000)) return json(res, 429, { error: 'สมัครบ่อยเกินไป ลองใหม่ภายหลัง' });
      const d = await readBody(req);
      const u = String(d.username || '').trim().toLowerCase();
      const pw = String(d.password || '');
      if (!/^[a-z0-9_]{3,20}$/.test(u) || u === '__proto__') return json(res, 400, { error: 'ชื่อผู้ใช้ต้องยาว 3-20 ตัว ใช้ a-z ตัวเลข และ _ เท่านั้น' });
      if (pw.length < 6 || pw.length > 100) return json(res, 400, { error: 'รหัสผ่านต้องยาว 6-100 ตัว' });
      if (getUser(u)) return json(res, 409, { error: 'ชื่อนี้มีคนใช้แล้ว' });
      const salt = crypto.randomBytes(16).toString('hex');
      const hash = (await scrypt(pw, salt, 64)).toString('hex');
      if (getUser(u)) return json(res, 409, { error: 'ชื่อนี้มีคนใช้แล้ว' });
      state.users[u] = { salt, hash, uid: state.nextUid++, created: Date.now() };
      // บอทรุ่นเก่าที่ยังไม่มีเจ้าของ ยกให้คนแรกที่สมัคร
      if (Object.keys(state.users).length === 1) {
        for (const b of Object.values(bots)) if (!b.owner) { b.owner = u; fixPerm(b); }
      }
      bump(regCount, ip, 3600000);
      const tok = newSession(u);
      return json(res, 200, { user: u }, { 'Set-Cookie': sessCookie(req, tok, SESSION_MS / 1000) });
    }

    // ----- เข้าสู่ระบบ -----
    if (p === '/api/login' && M === 'POST') {
      if (tooMany(loginFails, ip, 10, 600000)) return json(res, 429, { error: 'ลองผิดบ่อยเกินไป รอ 10 นาทีนะ' });
      const d = await readBody(req);
      const u = String(d.username || '').trim().toLowerCase();
      const pw = String(d.password || '');
      const user = getUser(u);
      const hash = await scrypt(pw, user ? user.salt : '0'.repeat(32), 64);
      const ok = user && crypto.timingSafeEqual(hash, Buffer.from(user.hash, 'hex'));
      if (!ok) { bump(loginFails, ip, 600000); return json(res, 401, { error: 'ชื่อหรือรหัสผ่านไม่ถูกต้อง' }); }
      loginFails.delete(ip);
      const tok = newSession(u);
      return json(res, 200, { user: u }, { 'Set-Cookie': sessCookie(req, tok, SESSION_MS / 1000) });
    }

    // ----- ออกจากระบบ -----
    if (p === '/api/logout' && M === 'POST') {
      const tok = getCookie(req, 'sid');
      if (tok) { delete state.sessions[sha(tok)]; save(); }
      return json(res, 200, { ok: true }, { 'Set-Cookie': sessCookie(req, '', 0) });
    }

    // ----- ต่อจากนี้ต้องล็อกอินแล้ว -----
    const me = userOf(req);
    if (!me) return json(res, 401, { error: 'unauthorized' });
    if (p === '/api/me') return json(res, 200, { user: me });

    if (p === '/api/bots') {
      if (M === 'GET') {
        const list = Object.values(bots).filter((b) => b.owner === me).sort((a, b) => (a.created || 0) - (b.created || 0));
        return json(res, 200, { bots: list.map((b) => pub(b)) });
      }
      if (M === 'POST') {
        const d = await readBody(req);
        if (d.lang !== 'py' && d.lang !== 'js') return json(res, 400, { error: 'lang ต้องเป็น py หรือ js' });
        const mine = Object.values(bots).filter((b) => b.owner === me).length;
        if (mine >= MAX_BOTS) return json(res, 400, { error: 'สร้างบอทได้สูงสุด ' + MAX_BOTS + ' ตัวต่อคน' });
        const id = crypto.randomBytes(4).toString('hex');
        const b = {
          id, owner: me, lang: d.lang, name: String(d.name || 'บอทใหม่').trim().slice(0, 40) || 'บอทใหม่',
          token: '', desired: false, status: 'stopped', startedAt: 0, lastExit: '', created: Date.now()
        };
        bots[id] = b;
        fs.mkdirSync(path.join(BOTS_DIR, id), { recursive: true });
        fs.writeFileSync(codeFile(b), typeof d.code === 'string' ? d.code : '');
        fixPerm(b);
        save();
        return json(res, 200, pub(b, true));
      }
    }

    const m = p.match(/^\/api\/bots\/([a-f0-9]{8})(?:\/(start|stop|logs))?$/);
    // บอทของคนอื่น = ตอบว่า "ไม่เจอ" เหมือนไม่มีอยู่จริง
    if (!m || !bots[m[1]] || bots[m[1]].owner !== me) return json(res, 404, { error: 'ไม่เจอบอทนี้' });
    const id = m[1], b = bots[id], sub = m[2];

    if (!sub && M === 'GET') return json(res, 200, pub(b, true));

    if (!sub && M === 'PUT') {
      const d = await readBody(req);
      if (typeof d.name === 'string' && d.name.trim()) b.name = d.name.trim().slice(0, 40);
      if (typeof d.code === 'string') { fs.writeFileSync(codeFile(b), d.code); fixPerm(b); }
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

// ---------- boot ----------
for (const [k, s] of Object.entries(state.sessions)) if (s.exp < Date.now()) delete state.sessions[k];
for (const b of Object.values(bots)) fixPerm(b);

let i = 0;
for (const b of Object.values(bots)) {
  if (b.desired) {
    setTimeout(() => {
      addLog(b.id, 'sys', 'เซิร์ฟเวอร์รีสตาร์ท - เปิดบอทให้อัตโนมัติ');
      startBot(b.id);
    }, 800 + i++ * 1500);
  } else if (b.status === 'running') {
    b.status = 'stopped';
  }
}
save();

server.listen(PORT, '0.0.0.0', () => {
  console.log('Bot Host รันที่พอร์ต ' + PORT + ' | ข้อมูล: ' + DATA_DIR + ' | แยกผู้ใช้: ' + (ISOLATE ? 'เปิด' : 'ปิด'));
});
