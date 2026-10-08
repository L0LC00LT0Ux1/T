'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const util = require('util');
const { spawn } = require('child_process');
const { Bucket } = require("@upstash/blob");

const scrypt = util.promisify(crypto.scrypt);

// ============ CONFIG ============
const PORT = process.env.PORT || 3000;
const ADMIN_USER = String(process.env.ADMIN_USER || 'toux1').toLowerCase();
const TRIAL_DAYS = parseInt(process.env.TRIAL_DAYS || '3', 10);
const TRIAL_MS = TRIAL_DAYS * 24 * 3600 * 1000;
const DEFAULT_MAX_BOTS = parseInt(process.env.DEFAULT_MAX_BOTS || '1', 10);
const DEFAULT_MAX_SITES = parseInt(process.env.DEFAULT_MAX_SITES || '1', 10);
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const EMAIL_FROM = process.env.EMAIL_FROM || 'noreply@example.com';
const ADMIN_UI_DEFAULT = process.env.ADMIN_UI !== 'off';
const MAX_POSTS = parseInt(process.env.MAX_POSTS_PER_USER || '20', 10);
const SIGNUP_OPEN = process.env.SIGNUP !== 'off';
const QUOTA = parseInt(process.env.QUOTA_MB || '300', 10) * 1024 * 1024;
const MAX_LIB = parseInt(process.env.MAX_LIB_MB || '1000', 10) * 1024 * 1024;
const MAX_INSTALLS = 2;
const SITE_FIELD_MAX = 200000;
const SITE_BANNER = process.env.SITE_BANNER === 'on';
const SITE_DB_BYTES = parseInt(process.env.SITE_DB_KB || '1024', 10) * 1024;
const SITE_VALUE_MAX = parseInt(process.env.SITE_VALUE_KB || '64', 10) * 1024;
const SITE_KEYS_MAX = 2000;
const ROOM_MAX = parseInt(process.env.ROOM_MAX_CLIENTS || '40', 10);
const SITE_ROOMS_MAX = 20;
const MAX_SSE = parseInt(process.env.MAX_SSE || '600', 10);

// ===== SITE TIER =====
const SITE_FILE_MAX = parseInt(process.env.SITE_FILE_MAX || '52428800', 10);
const SITE_QUOTA_DEFAULT_MB = parseInt(process.env.SITE_QUOTA_MB || '500', 10);
const SITE_CONSOLE_MAX = parseInt(process.env.SITE_CONSOLE_MAX || '500', 10);
const SITE_RUNTIME_PORT_MIN = parseInt(process.env.SITE_PORT_MIN || '4000', 10);
const SITE_RUNTIME_PORT_MAX = parseInt(process.env.SITE_PORT_MAX || '4999', 10);
const SITE_RUNTIME_AUTO_INSTALL = process.env.SITE_AUTO_INSTALL !== 'off';

const MSG_MAX = 500;
const CHAT_IMG_MAX = 5 * 1024 * 1024;
const CHAT_VID_MAX = 10 * 1024 * 1024;
const CHAT_FILE_MAX = 10 * 1024 * 1024;

const DISCORD_WEBHOOK = process.env.DISCORD_WEBHOOK
  || 'https://discord.com/api/webhooks/1541593046813118495/hCm3CkixqczVAeVpOu2X45EZ2wkGj84aO8XuEjoO9sAO8dqwRiwpAu_PeqFpmuLIjhqE';

const isAdmin = (u) => !!u && String(u).toLowerCase() === ADMIN_USER;
const TRIAL_MSG = 'สิทธิ์คุณหมดแล้ว ไปติดต่อ ซื้อสิทธ์ เพิ่มได้ที่ https://discord.gg/dTz2njT9fZ';
const BOT_EXPIRED_MSG = 'บอทนี้หมดเวลาแล้ว กรุณาต่อเวลาใหม่';

// ============ ID GEN ============
function genPublicId(existing) {
  for (let i = 0; i < 20; i++) {
    const id = crypto.randomBytes(4).toString('hex').toUpperCase();
    if (!existing || !existing.has(id)) return id;
  }
  return crypto.randomBytes(6).toString('hex').toUpperCase();
}
function allBotIds() { const s = new Set(); for (const b of Object.values(state.bots)) if (b.publicId) s.add(b.publicId); return s; }
function allSiteIds() { const s = new Set(); for (const x of Object.values(hub.sites)) if (x.publicId) s.add(x.publicId); return s; }
function genBotId() { return genPublicId(allBotIds()); }
function genSiteId() { return genPublicId(allSiteIds()); }
function chatId(a, b) {
  const [x, y] = [a, b].map(u => String(u).toLowerCase()).sort();
  return crypto.createHash('sha256').update(x + ':' + y).digest('hex').slice(0, 8);
}

// ============ REDIS ============
const REDIS_URL = String(process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const REDIS_TOKEN = String(process.env.UPSTASH_REDIS_REST_TOKEN || '');
const hasRedis = !!(REDIS_URL && REDIS_TOKEN);

async function redisGet(key) {
  if (!hasRedis) return null;
  try {
    const r = await fetch(REDIS_URL + '/get/' + encodeURIComponent(key), { headers: { Authorization: 'Bearer ' + REDIS_TOKEN } });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const j = await r.json();
    if (!j || j.result == null) return null;
    if (typeof j.result === 'string') { try { return JSON.parse(j.result); } catch { return j.result; } }
    return j.result;
  } catch (e) { console.error('redisGet[' + key + ']:', e.message); return null; }
}
async function redisSet(key, value) {
  if (!hasRedis) return false;
  try {
    const body = typeof value === 'string' ? value : JSON.stringify(value);
    const r = await fetch(REDIS_URL + '/set/' + encodeURIComponent(key), {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'text/plain' },
      body
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return true;
  } catch (e) { console.error('redisSet[' + key + ']:', e.message); return false; }
}
const redisTimers = {};
function scheduleRedisSave(key, getter, delay) {
  if (!hasRedis) return;
  if (redisTimers[key]) clearTimeout(redisTimers[key]);
  redisTimers[key] = setTimeout(async () => {
    delete redisTimers[key];
    try { await redisSet(key, JSON.stringify(getter())); } catch (e) { console.error('redis flush ' + key + ':', e.message); }
  }, delay || 300);
}
async function flushRedis() {
  for (const k of Object.keys(redisTimers)) { clearTimeout(redisTimers[k]); delete redisTimers[k]; }
  if (!hasRedis) return;
  const tasks = [];
  try { tasks.push(redisSet('alexa:state', JSON.stringify(state))); } catch {}
  try { tasks.push(redisSet('alexa:hub', JSON.stringify(hub))); } catch {}
  try { tasks.push(redisSet('alexa:sitedata', JSON.stringify(sdata))); } catch {}
  await Promise.all(tasks.map(p => p.catch(() => {})));
  console.log('✓ Flushed state to Redis');
}

// ============ BLOB ============
const BLOB_TOKENS = String(process.env.UPSTASH_BLOB_TOKENS || process.env.UPSTASH_BLOB_TOKEN || '')
  .split(',').map(s => s.trim()).filter(Boolean);
const blobBuckets = [];
for (let i = 0; i < BLOB_TOKENS.length; i++) {
  try {
    const old = process.env.UPSTASH_BLOB_TOKEN;
    process.env.UPSTASH_BLOB_TOKEN = BLOB_TOKENS[i];
    const bk = Bucket.fromEnv();
    if (old !== undefined) process.env.UPSTASH_BLOB_TOKEN = old; else delete process.env.UPSTASH_BLOB_TOKEN;
    blobBuckets.push({ bucket: bk, id: i, used: 0 });
  } catch (e) { console.error('❌ Blob init error #' + i + ':', e.message); }
}
if (blobBuckets.length) console.log('✅ Upstash Blob connected (' + blobBuckets.length + ' bucket' + (blobBuckets.length > 1 ? 's' : '') + ')');
else console.warn('⚠️ UPSTASH_BLOB_TOKENS not found. Media stored locally.');

function getBucketById(id) { return blobBuckets.find(b => b.id === id) || null; }

// ============ DIRS ============
const volPath = process.env.RAILWAY_VOLUME_MOUNT_PATH || '';
const DATA_DIR = process.env.DATA_DIR || volPath || path.join(__dirname, 'data');
const BOTS_DIR = path.join(DATA_DIR, 'bots');
const MEDIA_DIR = path.join(DATA_DIR, 'media');
const BACKUP_DIR = path.join(DATA_DIR, 'backups');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const HUB_FILE = path.join(DATA_DIR, 'hub.json');
const SITEDATA_FILE = path.join(DATA_DIR, 'sitedata.json');
const SITES_DIR = path.join(DATA_DIR, 'sites');
try { fs.mkdirSync(BOTS_DIR, { recursive: true }); } catch {}
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }); } catch {}
try { fs.mkdirSync(BACKUP_DIR, { recursive: true }); } catch {}
try { fs.mkdirSync(SITES_DIR, { recursive: true }); } catch {}

function detectPersistent() {
  if (process.env.ASSUME_PERSISTENT === '1') return true;
  try {
    if (volPath && path.resolve(DATA_DIR).startsWith(path.resolve(volPath))) return true;
    return fs.statSync(DATA_DIR).dev !== fs.statSync('/').dev;
  } catch { return false; }
}
const PERSISTENT = detectPersistent();
const ISOLATE = typeof process.getuid === 'function' && process.getuid() === 0 && process.env.ISOLATION !== 'off';
if (ISOLATE) {
  try {
    fs.chmodSync(DATA_DIR, 0o711); fs.chmodSync(BOTS_DIR, 0o711);
    fs.chmodSync(MEDIA_DIR, 0o700); fs.chmodSync(BACKUP_DIR, 0o700);
  } catch (e) { console.error('chmod:', e.message); }
}
try {
  const link = path.join(BOTS_DIR, 'node_modules');
  try { fs.unlinkSync(link); } catch {}
  fs.symlinkSync(path.join(__dirname, 'node_modules'), link, 'dir');
} catch (e) { console.error('symlink:', e.message); }

// ============ JSON IO ============
function loadJson(file) {
  for (const f of [file, file + '.bak']) {
    try { return { data: JSON.parse(fs.readFileSync(f, 'utf8')), from: f }; }
    catch (e) {
      if (e.code !== 'ENOENT') {
        console.error('read err:', f, e.message);
        try { fs.copyFileSync(f, f + '.corrupt-' + Date.now()); } catch {}
      }
    }
  }
  return null;
}
function backupOnce(file) {
  try {
    const b = file + '.bak';
    const t = fs.existsSync(b) ? fs.statSync(b).mtimeMs : 0;
    if (Date.now() - t > 300000 && fs.existsSync(file)) { fs.copyFileSync(file, b); fs.chmodSync(b, 0o600); }
  } catch {}
}
function snapshot() {
  const d = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  for (const [name, file] of [['state', STATE_FILE], ['hub', HUB_FILE], ['sitedata', SITEDATA_FILE]]) {
    const dest = path.join(BACKUP_DIR, name + '-' + d + '.json');
    try { if (fs.existsSync(file)) { fs.copyFileSync(file, dest); fs.chmodSync(dest, 0o600); } } catch {}
  }
  try {
    const groups = {};
    for (const f of fs.readdirSync(BACKUP_DIR).sort()) {
      const m = /^(state|hub|sitedata)-\d{8}\.json$/.exec(f);
      if (m) (groups[m[1]] = groups[m[1]] || []).push(f);
    }
    for (const k of Object.keys(groups)) while (groups[k].length > 7) fs.unlinkSync(path.join(BACKUP_DIR, groups[k].shift()));
  } catch {}
}

// ============ STATE ============
const state = { users: {}, sessions: {}, bots: {}, nextUid: 20000, adminUi: ADMIN_UI_DEFAULT };
{
  const r = loadJson(STATE_FILE);
  if (r) {
    const raw = r.data;
    if (raw && raw.users && raw.bots) Object.assign(state, raw);
    else if (raw && typeof raw === 'object') state.bots = raw;
    if (r.from !== STATE_FILE) console.error('main file corrupt, using backup:', r.from);
  }
}
const bots = state.bots;

function saveDisk() {
  try {
    const tmp = STATE_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(state), { mode: 0o600 });
    backupOnce(STATE_FILE);
    fs.renameSync(tmp, STATE_FILE);
    try { fs.chmodSync(STATE_FILE, 0o600); } catch {}
  } catch (e) {}
}
function save() {
  saveDisk();
  scheduleRedisSave('alexa:state', () => state, 300);
}
async function saveNow() {
  saveDisk();
  if (redisTimers['alexa:state']) { clearTimeout(redisTimers['alexa:state']); delete redisTimers['alexa:state']; }
  if (!hasRedis) return;
  await redisSet('alexa:state', JSON.stringify(state));
}

// ============ HUB ============
let hub = { posts: {}, chats: {}, media: {}, sites: {}, siteTokens: {}, siteFiles: {} };
{
  const r = loadJson(HUB_FILE);
  if (r) { hub = Object.assign(hub, r.data); if (r.from !== HUB_FILE) console.error('hub backup used'); }
}
if (!hub.siteTokens) hub.siteTokens = {};
if (!hub.siteFiles) hub.siteFiles = {};

let hubTimer = null;
function writeHubDisk() {
  try {
    const tmp = HUB_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(hub), { mode: 0o600 });
    backupOnce(HUB_FILE); fs.renameSync(tmp, HUB_FILE);
  } catch (e) {}
}
function writeHub() {
  writeHubDisk();
  scheduleRedisSave('alexa:hub', () => hub, 300);
}
function saveHub() {
  if (hubTimer) return;
  hubTimer = setTimeout(() => { hubTimer = null; try { writeHub(); } catch (e) { console.error('saveHub:', e.message); } }, 300);
}
async function saveHubNow() {
  if (hubTimer) { clearTimeout(hubTimer); hubTimer = null; }
  writeHubDisk();
  if (redisTimers['alexa:hub']) { clearTimeout(redisTimers['alexa:hub']); delete redisTimers['alexa:hub']; }
  if (!hasRedis) return;
  await redisSet('alexa:hub', JSON.stringify(hub));
}

// ============ SITEDATA ============
let sdata = { sites: {} };
{
  const r = loadJson(SITEDATA_FILE);
  if (r && r.data && r.data.sites) { sdata = r.data; if (r.from !== SITEDATA_FILE) console.error('sitedata backup used'); }
}
let sdTimer = null;
function writeSDataDisk() {
  try {
    const tmp = SITEDATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(sdata), { mode: 0o600 });
    backupOnce(SITEDATA_FILE); fs.renameSync(tmp, SITEDATA_FILE);
  } catch (e) {}
}
function writeSData() {
  writeSDataDisk();
  scheduleRedisSave('alexa:sitedata', () => sdata, 300);
}
function saveSData() {
  if (sdTimer) return;
  sdTimer = setTimeout(() => { sdTimer = null; try { writeSData(); } catch (e) { console.error('saveSData:', e.message); } }, 300);
}

const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const own = (o, k) => (hasOwn(o, k) ? o[k] : null);
const getUser = (u) => own(state.users, u);

// ============ USER HELPERS ============
function trialExpired(user) {
  if (!user) return true;
  if (isAdmin(user)) return false;
  if (user.paid) return false;
  if (!user.trialEnds) return false;
  return Date.now() > user.trialEnds;
}
function canCreate(user) {
  if (!user) return { ok: false, error: 'ไม่พบผู้ใช้' };
  if (isAdmin(user)) return { ok: true };
  if (!user.email) return { ok: false, error: 'กรุณาผูกอีเมลที่หน้าโปรไฟล์ก่อน', needsEmail: true };
  if (!user.emailVerified) return { ok: false, error: 'กรุณายืนยันอีเมลก่อน', needsVerify: true };
  if (trialExpired(user)) return { ok: false, error: TRIAL_MSG, trial: true };
  return { ok: true };
}
function maxBotsFor(u) { const x = getUser(u); return isAdmin(u) ? 999 : (x?.maxBots ?? DEFAULT_MAX_BOTS); }
function maxSitesFor(u) { const x = getUser(u); return isAdmin(u) ? 999 : (x?.maxSites ?? DEFAULT_MAX_SITES); }
function pubMe(u) {
  const x = getUser(u);
  if (!x) return null;
  return {
    user: u, admin: isAdmin(u),
    displayName: x.displayName || '', bio: x.bio || '', avatar: x.avatar || '',
    emailVerified: !!x.emailVerified, email: x.email || '', hasEmail: !!x.email,
    trialEnds: x.trialEnds || 0, trialExpired: trialExpired(x),
    paid: !!x.paid, maxBots: x.maxBots ?? DEFAULT_MAX_BOTS, maxSites: x.maxSites ?? DEFAULT_MAX_SITES
  };
}

// ============ EMAIL ============
async function sendEmail(to, subject, html) {
  if (!RESEND_API_KEY) {
    console.log('=== [DEV EMAIL] ===\nTo: ' + to + '\nSubject: ' + subject + '\n' + html + '\n===================');
    return { ok: true, dev: true };
  }
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + RESEND_API_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: EMAIL_FROM, to, subject, html })
    });
    if (!r.ok) throw new Error('HTTP ' + r.status + ' ' + (await r.text()).slice(0, 200));
    return { ok: true };
  } catch (e) { console.error('sendEmail:', e.message); return { ok: false, msg: e.message }; }
}
function genCode() { return String(Math.floor(100000 + Math.random() * 900000)); }

// ============ DISCORD ============
async function sendToDiscord(embed) {
  try {
    const r = await fetch(DISCORD_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'ALEXA HUB Report', avatar_url: 'https://cdn-icons-png.flaticon.com/512/564/564619.png', embeds: [embed] })
    });
    if (!r.ok) { const t = await r.text(); console.error('Discord webhook err:', r.status, t.slice(0, 200)); return { ok: false, msg: 'HTTP ' + r.status }; }
    return { ok: true };
  } catch (e) { console.error('Discord webhook:', e.message); return { ok: false, msg: e.message }; }
}

// ============ PROCS ============
const procs = {};
const installs = {};
let activeInstalls = 0;
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
const readCode = (b) => {
  try { return fs.readFileSync(codeFile(b), 'utf8'); }
  catch { return b.code || ''; }
};
function ensureBotFile(b) {
  if (!b) return;
  const dir = path.join(BOTS_DIR, b.id);
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
  const file = codeFile(b);
  if (!fs.existsSync(file)) {
    try { fs.writeFileSync(file, b.code || ''); console.log('  ↩️  Restored code file for bot ' + b.id); } catch (e) { console.error('ensureBotFile:', e.message); }
  }
}
const pub = (b, full) => Object.assign({
  id: b.id, publicId: b.publicId || '', name: b.name, lang: b.lang, status: b.status, owner: b.owner,
  startedAt: b.startedAt || 0, lastExit: b.lastExit || '', hasToken: !!b.token,
  expiresAt: b.expiresAt || 0,
  libs: b.libs || [], installing: !!installs[b.id]
}, full ? { code: readCode(b) } : {});

function fixPerm(b) {
  if (!ISOLATE) return;
  const u = getUser(b.owner);
  if (!u) return;
  const dir = path.join(BOTS_DIR, b.id);
  try {
    fs.chownSync(dir, u.uid, u.uid); fs.chmodSync(dir, 0o700);
    const f = codeFile(b);
    if (fs.existsSync(f)) { fs.chownSync(f, u.uid, u.uid); fs.chmodSync(f, 0o600); }
  } catch (e) { console.error('fixPerm:', e.message); }
}
const dropOpts = (owner) => (ISOLATE ? { uid: owner.uid, gid: owner.uid } : {});
const botExpired = (b) => !!b.expiresAt && Date.now() > b.expiresAt;

function createBot(owner, name, lang, code, expiresAt) {
  const id = crypto.randomBytes(4).toString('hex');
  const publicId = genBotId();
  const b = {
    id, publicId, owner, lang,
    name: String(name || 'บอทใหม่').trim().slice(0, 40) || 'บอทใหม่',
    token: '', libs: [], desired: false, status: 'stopped',
    startedAt: 0, lastExit: '', created: Date.now(),
    expiresAt: expiresAt || 0,
    code: typeof code === 'string' ? code : ''
  };
  bots[id] = b;
  ensureBotFile(b);
  fixPerm(b);
  return b;
}
const botCount = (u) => Object.values(bots).filter((b) => b.owner === u).length;

function startBot(id) {
  const b = bots[id];
  if (!b || procs[id]) return;
  const owner = getUser(b.owner);
  if (!owner) { b.status = 'error'; b.desired = false; b.lastExit = 'บอทนี้ไม่มีเจ้าของ'; save(); addLog(id, 'err', 'บอทนี้ไม่มีเจ้าของ'); return; }
  if (trialExpired(owner)) { b.status = 'stopped'; b.desired = false; b.lastExit = TRIAL_MSG; save(); addLog(id, 'err', 'ไม่สามารถรันได้: ' + TRIAL_MSG); return; }
  if (botExpired(b)) { b.status = 'stopped'; b.desired = false; b.lastExit = BOT_EXPIRED_MSG; save(); addLog(id, 'err', 'ไม่สามารถรันได้: ' + BOT_EXPIRED_MSG); return; }
  ensureBotFile(b);
  const file = codeFile(b); const dir = path.dirname(file);
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
  fixPerm(b);
  const env = {
    PATH: process.env.PATH || '/usr/local/bin:/usr/bin:/bin',
    HOME: dir, TMPDIR: dir, LANG: 'C.UTF-8',
    DISCORD_TOKEN: b.token || '',
    PYTHONPATH: path.join(dir, 'pylibs'),
    PYTHONUNBUFFERED: '1', PYTHONDONTWRITEBYTECODE: '1',
    UPSTASH_REDIS_REST_URL: REDIS_URL || '',
    UPSTASH_REDIS_REST_TOKEN: REDIS_TOKEN || '',
    BOT_ID: b.id
  };
  const opts = Object.assign({ cwd: dir, env }, dropOpts(owner));
  const py = b.lang === 'py';
  let child;
  try { child = spawn(py ? 'python3' : 'node', py ? ['-u', file] : [file], opts); }
  catch (e) { b.status = 'error'; b.desired = false; b.lastExit = 'สตาร์ทไม่ได้: ' + e.message; save(); addLog(id, 'err', b.lastExit); return; }
  procs[id] = child;
  b.status = 'running'; b.desired = true; b.startedAt = Date.now(); b.lastExit = '';
  save(); addLog(id, 'sys', 'เริ่มรันบอท (' + (py ? 'Python' : 'JavaScript') + ')');
  child.stdout.on('data', (d) => addLog(id, 'out', d));
  child.stderr.on('data', (d) => addLog(id, 'err', d));
  const done = (status, msg) => {
    if (procs[id] !== child) return;
    delete procs[id];
    if (shuttingDown) return;
    const cur = bots[id]; if (!cur) return;
    cur.status = status; cur.desired = false; cur.lastExit = msg; save();
    addLog(id, status === 'error' ? 'err' : 'sys', msg);
  };
  child.on('error', (e) => {
    let msg = 'สตาร์ทไม่ได้: ' + e.message;
    if (ISOLATE && (e.code === 'EPERM' || e.code === 'EACCES')) msg += ' (ระบบแยกผู้ใช้ใช้ไม่ได้)';
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
    const b = bots[id]; const c = procs[id];
    if (!c) { if (b) { b.desired = false; if (b.status === 'running') b.status = 'stopped'; save(); } return resolve(); }
    if (b) { b.desired = false; save(); }
    c.stopRequested = true;
    c.once('exit', () => resolve());
    c.kill('SIGTERM');
    setTimeout(() => { try { c.kill('SIGKILL'); } catch {} }, 4000);
  });
}

// ============ LIB PARSE ============
function validSpec(s) {
  return typeof s === 'string' && s.length <= 200 &&
    /^[A-Za-z0-9@][A-Za-z0-9@\/._:+#^~<>=!*|,%?&\[\]-]*$/.test(s) && !/^file:/i.test(s);
}
function keyOf(lang, spec) {
  if (/^(git\+|github:|https?:)/i.test(spec)) return spec;
  if (lang === 'py') return spec.split(/[=<>!~\[;]/)[0].toLowerCase().replace(/[-_.]+/g, '-');
  const m = /^(@[^/@]+\/[^@]+|[^@]+)/.exec(spec);
  return m ? m[1].toLowerCase() : spec;
}
const CMD_WORDS = { py: new Set(['sudo', 'python', 'python3', 'py', 'pip', 'pip3', '-m', 'install']), js: new Set(['sudo', 'npm', 'npx', 'yarn', 'pnpm', 'bun', 'install', 'i', 'add']) };
const FLAG_WITH_ARG = new Set(['-r', '--requirement', '-c', '--constraint', '-i', '--index-url', '--extra-index-url', '-f', '--find-links', '-t', '--target', '--prefix', '--registry', '--cache', '--root', '--proxy']);
function splitCommas(tok) {
  const out = []; let depth = 0, cur = '';
  for (let i = 0; i < tok.length; i++) {
    const ch = tok[i];
    if (ch === '[') depth++;
    else if (ch === ']') depth = Math.max(0, depth - 1);
    if (ch === ',' && depth === 0 && (i === tok.length - 1 || /[A-Za-z@]/.test(tok[i + 1]))) { if (cur) out.push(cur); cur = ''; continue; }
    cur += ch;
  }
  if (cur) out.push(cur);
  return out;
}
function parseSpecs(lang, text) {
  const cmds = CMD_WORDS[lang] || CMD_WORDS.py;
  const found = [], ignored = [], bad = [];
  const lines = String(text || '').replace(/&&|\|\|/g, '\n').split(/\r?\n/);
  for (let line of lines) {
    line = line.replace(/(^|\s)#.*$/, '').trim();
    if (!line) continue;
    const toks = (line.match(/"[^"]*"|'[^']*'|\S+/g) || []).map((t) => t.replace(/^["']|["']$/g, ''));
    let i = 0; while (i < toks.length && cmds.has(toks[i].toLowerCase())) i++;
    for (; i < toks.length; i++) {
      const t = toks[i]; if (!t) continue;
      if (t.startsWith('-')) { ignored.push(t); if (FLAG_WITH_ARG.has(t) && i + 1 < toks.length) { ignored.push(toks[i + 1]); i++; } continue; }
      for (const piece of splitCommas(t)) { if (validSpec(piece)) found.push(piece); else bad.push(piece); }
    }
  }
  const map = new Map();
  for (const s of found) map.set(keyOf(lang, s), s);
  return { specs: Array.from(map.values()), ignored, bad };
}
const libDir = (b) => path.join(BOTS_DIR, b.id, b.lang === 'py' ? 'pylibs' : 'node_modules');
function wipeLibs(b) {
  const dir = path.join(BOTS_DIR, b.id);
  const rm = (p) => { try { fs.rmSync(p, { recursive: true, force: true }); } catch {} };
  if (b.lang === 'py') rm(path.join(dir, 'pylibs'));
  else { rm(path.join(dir, 'node_modules')); rm(path.join(dir, 'package.json')); rm(path.join(dir, 'package-lock.json')); }
}
function dirSize(d) {
  let n = 0, ents;
  try { ents = fs.readdirSync(d, { withFileTypes: true }); } catch { return 0; }
  for (const e of ents) {
    const p = path.join(d, e.name);
    try { if (e.isDirectory()) n += dirSize(p); else if (e.isFile()) n += fs.statSync(p).size; } catch {}
  }
  return n;
}
function runInstall(id, specs) {
  return new Promise((resolve) => {
    const b = bots[id]; const owner = b && getUser(b.owner);
    if (!owner) return resolve({ ok: false, msg: 'บอทนี้ไม่มีเจ้าของ' });
    const dir = path.join(BOTS_DIR, id);
    const py = b.lang === 'py';
    fixPerm(b);
    const env = {
      PATH: process.env.PATH || '/usr/local/bin:/usr/bin:/bin',
      HOME: dir, TMPDIR: dir, LANG: 'C.UTF-8', GIT_TERMINAL_PROMPT: '0',
      PIP_DISABLE_PIP_VERSION_CHECK: '1',
      npm_config_cache: path.join(dir, '.npm-cache'),
      npm_config_update_notifier: 'false', npm_config_fund: 'false', npm_config_audit: 'false', npm_config_nodedir: '/usr/local'
    };
    const cmd = py ? 'python3' : 'npm';
    const args = py
      ? ['-m', 'pip', 'install', '--target', path.join(dir, 'pylibs'), '--no-cache-dir', '--upgrade', '--break-system-packages'].concat(specs)
      : ['install', '--prefix', dir, '--no-audit', '--no-fund'].concat(specs);
    let child;
    try { child = spawn(cmd, args, Object.assign({ cwd: dir, env }, dropOpts(owner))); }
    catch (e) { return resolve({ ok: false, msg: e.message }); }
    if (installs[id]) installs[id].child = child;
    child.stdout.on('data', (d) => addLog(id, 'out', d));
    child.stderr.on('data', (d) => addLog(id, 'out', d));
    const to = setTimeout(() => { addLog(id, 'err', 'ติดตั้งนานเกิน 10 นาที'); try { child.kill('SIGKILL'); } catch {} }, 600000);
    child.on('error', (e) => { clearTimeout(to); resolve({ ok: false, msg: 'สั่งติดตั้งไม่ได้: ' + e.message }); });
    child.on('exit', (code) => { clearTimeout(to); resolve({ ok: code === 0, msg: code === 0 ? '' : 'ติดตั้งไม่สำเร็จ (exit ' + code + ')' }); });
  });
}
async function libJob(id, o) {
  const b = bots[id]; if (!b) return;
  installs[id] = { t: Date.now(), child: null };
  activeInstalls++;
  const dir = path.join(BOTS_DIR, id);
  try {
    let libs = (b.libs || []).slice(); let specs;
    if (o.reinstall || o.remove) {
      if (o.remove) libs = libs.filter((x) => x !== o.remove);
      b.libs = libs; save(); wipeLibs(b); specs = libs;
      addLog(id, 'sys', o.remove ? 'ลบ ' + o.remove + ' แล้วติดตั้งใหม่' : 'ติดตั้งใหม่ทั้งหมด');
    } else { specs = o.add; addLog(id, 'sys', 'ติดตั้ง ' + specs.length + ' ตัว: ' + specs.join(' ')); }
    let r = { ok: true, msg: '' };
    if (specs.length) r = await runInstall(id, specs);
    if (r.ok && specs.length && dirSize(libDir(b)) > MAX_LIB) {
      const prev = (b.libs || []).slice(); wipeLibs(b);
      if (o.add && prev.length) { addLog(id, 'sys', 'ใหญ่เกิน กำลังคืนของเดิม'); await runInstall(id, prev); }
      else b.libs = [];
      r = { ok: false, msg: 'ไลบรารีรวมใหญ่เกิน ' + Math.round(MAX_LIB / 1048576) + ' MB' };
    }
    for (const c of ['.npm-cache', '.cache', '.npm']) { try { fs.rmSync(path.join(dir, c), { recursive: true, force: true }); } catch {} }
    if (bots[id]) {
      if (r.ok && o.add) {
        for (const a of o.add) { const k = keyOf(b.lang, a); libs = libs.filter((x) => keyOf(b.lang, x) !== k); libs.push(a); }
        b.libs = libs;
      }
      save(); addLog(id, r.ok ? 'sys' : 'err', r.ok ? 'ติดตั้งเสร็จ รันบอทใหม่' : r.msg);
    }
  } finally { delete installs[id]; activeInstalls--; }
}

// ============ AUTH ============
const SESSION_MS = 30 * 24 * 3600 * 1000;
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
function getCookie(req, name) {
  const c = req.headers.cookie || '';
  for (const part of c.split(';')) {
    const i = part.indexOf('=');
    if (i > 0 && part.slice(0, i).trim() === name) { try { return decodeURIComponent(part.slice(i + 1).trim()); } catch { return ''; } }
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
  let tok = '';
  const auth = String(req.headers['authorization'] || '');
  if (auth.startsWith('Bearer ')) tok = auth.slice(7).trim();
  if (!tok) tok = String(req.headers['x-session'] || '').trim();
  if (!tok) tok = getCookie(req, 'sid');
  if (!tok) return null;
  const s = state.sessions[sha(tok)];
  if (!s || s.exp < Date.now()) return null;
  return getUser(s.user) ? s.user : null;
}
function userFromQueryToken(req) {
  try {
    const u = new URL(req.url, 'http://x');
    const t = u.searchParams.get('t') || '';
    if (!t) return null;
    const s = state.sessions[sha(t)];
    if (!s || s.exp < Date.now()) return null;
    return getUser(s.user) ? s.user : null;
  } catch { return null; }
}

const ipOf = (req) => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const loginFails = new Map(), regCount = new Map(), msgRate = new Map(), emailSends = new Map(), reportRate = new Map();
function tooMany(map, ip, max, win) { const f = map.get(ip); return !!f && Date.now() - f.t < win && f.n >= max; }
function bump(map, ip, win) {
  const f = map.get(ip);
  if (!f || Date.now() - f.t >= win) map.set(ip, { n: 1, t: Date.now() });
  else f.n++;
}
function msgLimited(u) {
  const now = Date.now();
  let f = msgRate.get(u);
  if (!f || now - f.t > 60000) { f = { n: 0, t: now }; msgRate.set(u, f); }
  f.n++; return f.n > 60;
}

const pubPost = (p, me) => ({
  id: p.id, owner: p.owner, title: p.title, lang: p.lang, cover: p.cover || '',
  price: p.price, created: p.created, mine: p.owner === me,
  commentCount: (p.comments || []).length
});
const pubComment = (c) => ({ id: c.id, from: c.from, text: c.text, media: c.media || null, t: c.t });

const MIME = { 'image/jpeg': 'jpg', 'image/png': 'png', 'image/gif': 'gif', 'image/webp': 'webp', 'video/mp4': 'mp4', 'video/webm': 'webm', 'video/quicktime': 'mov' };
function extFor(ct, origName) {
  if (MIME[ct]) return MIME[ct];
  const m = /\.([a-z0-9]{1,8})$/i.exec(String(origName || ''));
  if (m) return m[1].toLowerCase();
  const sub = String(ct).split('/')[1] || 'bin';
  return sub.replace(/[^a-z0-9]/gi, '').slice(0, 8) || 'bin';
}
function kindFor(ct) { if (ct.startsWith('image/')) return 'image'; if (ct.startsWith('video/')) return 'video'; return 'file'; }
function limitForKind(kind) { if (kind === 'image') return CHAT_IMG_MAX; if (kind === 'video') return CHAT_VID_MAX; return CHAT_FILE_MAX; }

function saveStream(req, fp, limit) {
  return new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(fp);
    let n = 0, failed = false;
    const fail = (e) => { if (failed) return; failed = true; ws.destroy(); fs.unlink(fp, () => {}); reject(e); };
    req.pipe(ws);
    req.on('data', (c) => { n += c.length; if (n > limit && !failed) { req.unpipe(ws); fail(Object.assign(new Error('ไฟล์ใหญ่เกิน'), { status: 413 })); req.resume(); } });
    ws.on('finish', () => { if (failed) return; if (n === 0) { fs.unlink(fp, () => {}); return reject(Object.assign(new Error('ไฟล์ว่างเปล่า'), { status: 400 })); } resolve(n); });
    ws.on('error', fail); req.on('error', fail);
    req.on('aborted', () => fail(new Error('ถูกยกเลิก')));
  });
}
function readStreamToBuffer(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = []; let n = 0;
    req.on('data', (c) => { n += c.length; if (n > limit) { req.destroy(); return reject(Object.assign(new Error('ไฟล์ใหญ่เกิน'), { status: 413 })); } chunks.push(c); });
    req.on('end', () => { if (n === 0) return reject(Object.assign(new Error('ไฟล์ว่างเปล่า'), { status: 400 })); resolve(Buffer.concat(chunks)); });
    req.on('error', reject);
    req.on('aborted', () => reject(new Error('ถูกยกเลิก')));
  });
}

async function putMedia(buf, ext, ct) {
  const id = crypto.randomBytes(12).toString('hex');
  if (blobBuckets.length) {
    let best = blobBuckets[0];
    for (const bk of blobBuckets) if (bk.used < best.used) best = bk;
    const tries = [best].concat(blobBuckets.filter(b => b !== best));
    let uploaded = false, lastErr = null, upath = `media/${id}.${ext}`, bucketId = -1;
    for (const bk of tries) {
      try {
        await bk.bucket.put(upath, buf, { contentType: ct });
        bk.used += buf.length;
        bucketId = bk.id;
        uploaded = true;
        console.log('✓ Uploaded media/' + id + '.' + ext + ' to bucket #' + bk.id);
        break;
      } catch (e) {
        console.error('❌ put failed on bucket #' + bk.id + ':', e.message);
        lastErr = e;
      }
    }
    if (!uploaded) throw new Error('put failed all buckets' + (lastErr ? ': ' + lastErr.message : ''));
    return { id, size: buf.length, upstashPath: upath, bucketId };
  } else {
    const fp = path.join(MEDIA_DIR, id + '.' + ext);
    fs.writeFileSync(fp, buf);
    return { id, size: buf.length, upstashPath: null, bucketId: -1 };
  }
}

async function deleteMediaFile(mediaId) {
  const m = own(hub.media, mediaId);
  if (!m) return false;
  try {
    if (m.upstashPath) {
      const bk = getBucketById(m.bucketId || 0);
      if (bk) {
        try {
          await bk.bucket.delete(m.upstashPath);
          bk.used = Math.max(0, (bk.used || 0) - (m.size || 0));
        } catch (e) { console.error('delete from bucket error:', e.message); }
      }
    } else {
      fs.unlink(path.join(MEDIA_DIR, mediaId + '.' + m.ext), () => {});
    }
  } catch (e) { console.error('deleteMediaFile error:', e.message); }
  delete hub.media[mediaId];
  return true;
}

async function blobResultToBuffer(got) {
  if (!got) return null;
  if (Buffer.isBuffer(got)) return got;
  if (typeof got.arrayBuffer === 'function') return Buffer.from(await got.arrayBuffer());
  if (got.body) {
    const body = got.body;
    if (Buffer.isBuffer(body)) return body;
    if (typeof body.arrayBuffer === 'function') return Buffer.from(await body.arrayBuffer());
    if (typeof body.getReader === 'function') {
      const reader = body.getReader();
      const chunks = [];
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(Buffer.from(value));
      }
      return Buffer.concat(chunks);
    }
    if (typeof body.on === 'function') {
      return new Promise((resolve, reject) => {
        const chunks = [];
        body.on('data', (c) => chunks.push(Buffer.from(c)));
        body.on('end', () => resolve(Buffer.concat(chunks)));
        body.on('error', reject);
      });
    }
  }
  if (typeof got === 'string') return Buffer.from(got, 'binary');
  return null;
}

async function serveMedia(req, res, id, me) {
  const m = own(hub.media, id);
  if (!m) return json(res, 404, { error: 'ไม่เจอไฟล์' });
  if (m.scope === 'chat') {
    let uid = me;
    if (!uid) uid = userFromQueryToken(req);
    if (!uid) return json(res, 404, { error: 'ไม่เจอไฟล์' });
    const c = own(hub.chats, m.chatId);
    if (!c || !c.members.includes(uid)) return json(res, 404, { error: 'ไม่เจอไฟล์' });
  }
  if (m.upstashPath) {
    const bid = typeof m.bucketId === 'number' ? m.bucketId : 0;
    const bk = getBucketById(bid);
    if (!bk) return json(res, 500, { error: 'ไม่พบ bucket' });
    let buf = null;
    try {
      if (typeof bk.bucket.get === 'function') {
        const got = await bk.bucket.get(m.upstashPath);
        buf = await blobResultToBuffer(got);
      }
    } catch (e) { console.error('bucket.get error:', e.message); }
    if (!buf) {
      try {
        if (typeof bk.bucket.signedReadUrl === 'function') {
          const url = await bk.bucket.signedReadUrl(m.upstashPath, { expiresIn: 300 });
          const r = await fetch(url);
          if (r.ok) buf = Buffer.from(await r.arrayBuffer());
        }
      } catch (e) {}
    }
    if (!buf) return json(res, 500, { error: 'อ่านไฟล์จาก storage ไม่ได้' });
    const h = { 'Content-Type': m.mime || 'application/octet-stream', 'Content-Length': buf.length, 'Cache-Control': 'public, max-age=86400', 'X-Content-Type-Options': 'nosniff' };
    if (m.kind === 'file') h['Content-Disposition'] = 'attachment; filename="file.' + m.ext + '"';
    res.writeHead(200, h);
    return res.end(buf);
  }
  const fp = path.join(MEDIA_DIR, id + '.' + m.ext);
  let size;
  try { size = fs.statSync(fp).size; } catch { return json(res, 404, { error: 'ไม่เจอไฟล์' }); }
  let start = 0, end = size - 1, code = 200;
  const r = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
  if (r && (r[1] !== '' || r[2] !== '')) {
    if (r[1] === '') start = Math.max(0, size - parseInt(r[2], 10));
    else { start = parseInt(r[1], 10); if (r[2] !== '') end = Math.min(end, parseInt(r[2], 10)); }
    if (start > end || start >= size) { res.writeHead(416, { 'Content-Range': 'bytes */' + size }); return res.end(); }
    code = 206;
  }
  const isDownload = m.kind === 'file';
  const h = { 'Content-Type': m.mime || 'application/octet-stream', 'Accept-Ranges': 'bytes', 'Content-Length': end - start + 1, 'Cache-Control': 'private, max-age=86400', 'X-Content-Type-Options': 'nosniff' };
  if (isDownload) h['Content-Disposition'] = 'attachment; filename="file.' + m.ext + '"';
  if (code === 206) h['Content-Range'] = 'bytes ' + start + '-' + end + '/' + size;
  res.writeHead(code, h);
  const s = fs.createReadStream(fp, { start, end });
  s.on('error', () => res.destroy()); s.pipe(res);
}

// ============ SITE HELPERS ============
const SLUG_RE = /^[a-z0-9][a-z0-9-]{2,29}$/;
const SITE_KEY_RE = /^[A-Za-z0-9_.:-]{1,64}$/;
const ROOM_RE = /^[A-Za-z0-9_-]{1,32}$/;
const CID_RE = /^[a-f0-9]{32}$/;
const USERNAME_RE = /^[a-z0-9_]{3,20}$/;
const SANDBOX_FLAGS = 'allow-scripts allow-forms allow-popups allow-modals allow-downloads allow-pointer-lock';
const escHtml = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const pubSite = (x, me) => ({
  slug: x.slug, publicId: x.publicId || '', owner: x.owner, title: x.title,
  tier: x.tier || 'normal', mode: x.mode || 'split', public: !!x.public, views: x.views || 0,
  created: x.created, updated: x.updated, mine: x.owner === me,
  hasToken: !!findTokenOfSlug(x.slug)
});

function buildSitePage(s) {
  if (s.mode === 'full') return s.html;
  return '<!doctype html><html lang="th"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>' + escHtml(s.title) + '</title><style>\n' + (s.css || '') + '\n</style></head><body>\n' + s.html + '\n<script>\n' + (s.js || '') + '\n</script></body></html>';
}
const SDK_TAG = '<script src="/s/_sdk.js"></script>';
function injectSdk(html) {
  for (const re of [/<head(\s[^>]*)?>/i, /<html(\s[^>]*)?>/i, /<!doctype[^>]*>/i]) {
    const m = re.exec(html);
    if (m) { const i = m.index + m[0].length; return html.slice(0, i) + SDK_TAG + html.slice(i); }
  }
  return SDK_TAG + html;
}
function validateSite(d) {
  const title = String(d.title || '').trim().slice(0, 60);
  const tier = d.tier === 'online' ? 'online' : 'normal';
  const mode = d.mode === 'full' ? 'full' : 'split';
  const html = typeof d.html === 'string' ? d.html : '';
  const css = mode === 'full' ? '' : (typeof d.css === 'string' ? d.css : '');
  const js = mode === 'full' ? '' : (typeof d.js === 'string' ? d.js : '');
  const packageJson = tier === 'online' && typeof d.packageJson === 'string' ? d.packageJson : '';
  const serverCode = tier === 'online' && typeof d.serverCode === 'string' ? d.serverCode : '';
  if (!title) return { error: 'ใส่ชื่อเว็บด้วย' };
  if (html.length > SITE_FIELD_MAX || css.length > SITE_FIELD_MAX || js.length > SITE_FIELD_MAX) return { error: 'โค้ดยาวเกินไป' };
  if (packageJson.length > SITE_FIELD_MAX || serverCode.length > SITE_FIELD_MAX) return { error: 'โค้ดยาวเกินไป' };
  if (mode === 'full' ? !html.trim() : !(html.trim() || css.trim() || js.trim())) return { error: 'ใส่โค้ดอย่างน้อย 1 ช่อง' };
  return { title, tier, mode, html, css, js, packageJson, serverCode, public: d.public !== false };
}

// ============ SITE SDK (client) ============
function alexaSdk() {
  if (window.alexa) return;
  var parentWin = window.parent;
  var hasParent = !!parentWin && parentWin !== window;
  var decided = false, online = false, cid = null, seq = 0;
  var pending = {}, rooms = {};
  var resolveReady;
  var ready = new Promise(function (r) { resolveReady = r; });
  function rnd(n) { var s = ''; for (var i = 0; i < n; i++) s += Math.floor(Math.random() * 16).toString(16); return s; }
  function post(msg) { try { parentWin.postMessage(Object.assign({ alexa: 1 }, msg), '*'); } catch (e) {} }
  function call(cmd, args) {
    return new Promise(function (resolve, reject) {
      var id = ++seq;
      pending[id] = { resolve: resolve, reject: reject };
      post(Object.assign({ cmd: cmd, id: id }, args || {}));
      setTimeout(function () { if (pending[id]) { delete pending[id]; reject(new Error('หมดเวลา')); } }, 15000);
    });
  }
  window.addEventListener('message', function (e) {
    if (!hasParent || e.source !== parentWin) return;
    var m = e.data; if (!m || m.alexa !== 1) return;
    if (m.type === 'ready') { if (decided) return; decided = true; online = true; cid = m.cid; resolveReady(); return; }
    if (m.type === 'reply') { var p = pending[m.id]; if (p) { delete pending[m.id]; if (m.ok) p.resolve(m.result); else p.reject(new Error(m.error || 'error')); } return; }
    if (m.type === 'room') { var r = rooms[m.room]; if (r) r._event(m.ev, m.data); }
  });
  var tries = 0;
  function hello() { if (decided || !hasParent) return; post({ cmd: 'hello' }); if (++tries < 5) setTimeout(hello, 400); }
  hello();
  setTimeout(function () { if (decided) return; decided = true; online = false; cid = 'local' + rnd(20); resolveReady(); }, 2200);
  function Room(name, opts) {
    var self = this;
    this.name = name; this.peers = []; this.id = null; this._h = {};
    this._name = opts && opts.name ? String(opts.name).slice(0, 24) : '';
    this._joined = new Promise(function (r) { self._resolveJoined = r; });
  }
  Room.prototype.on = function (ev, fn) { (this._h[ev] = this._h[ev] || []).push(fn); return this; };
  Room.prototype._emit = function (ev, data) { (this._h[ev] || []).slice().forEach(function (f) { try { f(data); } catch (e) {} }); };
  Room.prototype._event = function (ev, d) {
    if (ev === 'hello') { this.id = d.id; this.peers = (d.peers || []).slice(); this._resolveJoined(); this._emit('hello', d); }
    else if (ev === 'join') { if (!this.peers.some(function (p) { return p.id === d.id; })) this.peers.push(d); this._emit('join', d); }
    else if (ev === 'leave') { this.peers = this.peers.filter(function (p) { return p.id !== d.id; }); this._emit('leave', d); }
    else if (ev === 'msg') { this._emit('message', d); }
    else if (ev === 'error') { this._emit('error', d); }
  };
  Room.prototype.send = function (data, o) {
    var self = this; o = o || {};
    return ready.then(function () { return self._joined; }).then(function () {
      if (online) return call('send', { room: self.name, data: data, keep: !!o.keep, self: o.self !== false });
      if (o.self !== false) { var msg = { from: cid, name: self._name, data: data, t: Date.now() }; setTimeout(function () { self._event('msg', msg); }, 0); }
      return true;
    });
  };
  Room.prototype.leave = function () { var name = this.name; delete rooms[name]; return ready.then(function () { if (online) return call('leave', { room: name }); return true; }); };
  function join(name, opts) {
    name = String(name || '');
    var room = new Room(name, opts);
    rooms[name] = room;
    ready.then(function () {
      if (online) call('join', { room: name, name: room._name }).catch(function (err) { room._emit('error', { error: err.message }); });
      else setTimeout(function () { room._event('hello', { id: cid, peers: [{ id: cid, name: room._name }], history: [] }); }, 0);
    });
    return room;
  }
  var local = { shared: {}, mine: {} };
  var has = function (o, k) { return Object.prototype.hasOwnProperty.call(o, k); };
  function localDb(op, scope, a) {
    var st = local[scope];
    var key = a.key === undefined ? '' : String(a.key);
    var k = 'k:' + key;
    if (op === 'get') return Promise.resolve(has(st, k) ? JSON.parse(st[k]) : null);
    if (op === 'set') { st[k] = JSON.stringify(a.value === undefined ? null : a.value); return Promise.resolve(true); }
    if (op === 'del') { delete st[k]; return Promise.resolve(true); }
    if (op === 'incr') { var cur = has(st, k) ? JSON.parse(st[k]) : 0; if (typeof cur !== 'number') return Promise.reject(new Error('ไม่ใช่ตัวเลข')); cur += (a.by === undefined ? 1 : Number(a.by)); st[k] = JSON.stringify(cur); return Promise.resolve(cur); }
    if (op === 'list') { var p = String(a.prefix || ''); return Promise.resolve(Object.keys(st).filter(function (x) { return x.slice(2).indexOf(p) === 0; }).sort().map(function (x) { return { key: x.slice(2), value: JSON.parse(st[x]) }; })); }
    if (op === 'clear') { local[scope] = {}; return Promise.resolve(true); }
    return Promise.reject(new Error('คำสั่งไม่ถูกต้อง'));
  }
  function dbCall(op, scope, extra) {
    scope = scope === 'mine' ? 'mine' : 'shared';
    return ready.then(function () { if (online) return call('db', Object.assign({ op: op, scope: scope }, extra)); return localDb(op, scope, extra); });
  }
  var db = {
    get: function (key, scope) { return dbCall('get', scope, { key: key }); },
    set: function (key, value, scope) { return dbCall('set', scope, { key: key, value: value }); },
    del: function (key, scope) { return dbCall('del', scope, { key: key }); },
    incr: function (key, by, scope) { return dbCall('incr', scope, { key: key, by: by === undefined ? 1 : by }); },
    list: function (prefix, scope) { return dbCall('list', scope, { prefix: prefix || '' }); },
    clear: function () { return dbCall('clear', 'mine', {}); }
  };
  var slug = null;
  var mm = /^\/s\/([a-z0-9][a-z0-9-]{2,29})/.exec(location.pathname);
  if (mm) slug = mm[1];
  function filesUpload(file, onProgress) {
    return new Promise(function (resolve, reject) {
      if (!slug) return reject(new Error('ไม่พบ slug ของเว็บ'));
      if (!file) return reject(new Error('ไม่มีไฟล์'));
      var x = new XMLHttpRequest();
      x.open('POST', '/sapi/' + slug + '/file');
      x.setRequestHeader('Content-Type', file.type || 'application/octet-stream');
      try { x.setRequestHeader('X-File-Name', encodeURIComponent(file.name || '').slice(0, 200)); } catch (e) {}
      x.withCredentials = true;
      x.upload.onprogress = function (e) { if (e.lengthComputable && onProgress) onProgress(Math.round(e.loaded / e.total * 100)); };
      x.onload = function () {
        var d = {};
        try { d = JSON.parse(x.responseText); } catch (e) {}
        if (x.status >= 200 && x.status < 300) resolve(d);
        else reject(new Error(d.error || ('HTTP ' + x.status)));
      };
      x.onerror = function () { reject(new Error('เครือข่ายขัดข้อง')); };
      x.send(file);
    });
  }
  function filesList() {
    if (!slug) return Promise.reject(new Error('ไม่พบ slug ของเว็บ'));
    return fetch('/sapi/' + slug + '/file/list', { credentials: 'same-origin' })
      .then(function (r) { return r.json().then(function (d) { if (!r.ok) throw new Error(d.error || 'HTTP ' + r.status); return d; }); });
  }
  function filesDelete(id) {
    if (!slug) return Promise.reject(new Error('ไม่พบ slug ของเว็บ'));
    return fetch('/sapi/' + slug + '/file/' + id, { method: 'DELETE', credentials: 'same-origin' })
      .then(function (r) { return r.json().then(function (d) { if (!r.ok) throw new Error(d.error || 'HTTP ' + r.status); return d; }); });
  }
  var files = { upload: filesUpload, list: filesList, delete: filesDelete, del: filesDelete, url: function (id) { return '/sapi/' + slug + '/file/' + id; }, slug: function () { return slug; } };
  var logQueue = [];
  var logTimer = null;
  var logCount = 0;
  var logResetAt = 0;
  function flushLogs() {
    logTimer = null;
    if (!logQueue.length || !slug) return;
    var batch = logQueue.splice(0, 5);
    for (var i = 0; i < batch.length; i++) {
      (function (item) {
        fetch('/sapi/' + slug + '/console', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(item), credentials: 'same-origin' }).catch(function () {});
      })(batch[i]);
    }
    if (logQueue.length) logTimer = setTimeout(flushLogs, 100);
  }
  function sendLog(level, args) {
    if (!slug) return;
    var now = Date.now();
    if (now - logResetAt > 10000) { logCount = 0; logResetAt = now; }
    if (logCount >= 100) return;
    logCount++;
    var msg = Array.prototype.map.call(args, function (a) {
      if (a === null) return 'null';
      if (a === undefined) return 'undefined';
      if (typeof a === 'string') return a;
      if (typeof a === 'number' || typeof a === 'boolean') return String(a);
      if (a instanceof Error) return a.message + '\n' + (a.stack || '');
      try { return JSON.stringify(a); } catch (e) { return String(a); }
    }).join(' ').slice(0, 1000);
    logQueue.push({ level: level, msg: msg });
    if (!logTimer) logTimer = setTimeout(flushLogs, 100);
  }
  function alexaLog() { sendLog('log', arguments); }
  function alexaInfo() { sendLog('info', arguments); }
  function alexaWarn() { sendLog('warn', arguments); }
  function alexaError() { sendLog('error', arguments); }
  if (!window.__alexaConsoleHooked) {
    window.__alexaConsoleHooked = true;
    ['log', 'info', 'warn', 'error'].forEach(function (lv) {
      var orig = console[lv] ? console[lv].bind(console) : function () {};
      console[lv] = function () {
        try { orig.apply(null, arguments); } catch (e) {}
        try { sendLog(lv, arguments); } catch (e) {}
      };
    });
    window.addEventListener('error', function (e) {
      try { sendLog('error', ['[uncaught] ' + (e.message || 'error') + ' @ ' + (e.filename || '') + ':' + (e.lineno || 0)]); } catch (x) {}
    });
    window.addEventListener('unhandledrejection', function (e) {
      try { sendLog('error', ['[unhandled promise] ' + ((e.reason && e.reason.message) || e.reason)]); } catch (x) {}
    });
  }
  var api = { join: join, db: db, ready: ready, files: files, log: alexaLog, info: alexaInfo, warn: alexaWarn, error: alexaError };
  Object.defineProperty(api, 'id', { get: function () { return cid; }, enumerable: true });
  Object.defineProperty(api, 'offline', { get: function () { return decided && !online; }, enumerable: true });
  window.alexa = api;
}

function alexaHost() {
  var mm = /^\/s\/([a-z0-9][a-z0-9-]{2,29})/.exec(location.pathname);
  if (!mm) return;
  var slug = mm[1], base = '/sapi/' + slug;
  var ROOM_RE = /^[A-Za-z0-9_-]{1,32}$/;
  var cid = null, lsKey = 'alexa_cid_' + slug;
  try { cid = localStorage.getItem(lsKey); } catch (e) {}
  if (!cid || !/^[a-f0-9]{32}$/.test(cid)) {
    var a = new Uint8Array(16);
    window.crypto.getRandomValues(a);
    cid = Array.prototype.map.call(a, function (b) { return ('0' + b.toString(16)).slice(-2); }).join('');
    try { localStorage.setItem(lsKey, cid); } catch (e) {}
  }
  var sources = {};
  function frame() { return document.getElementById('f'); }
  function toFrame(msg) { var f = frame(); if (f && f.contentWindow) f.contentWindow.postMessage(Object.assign({ alexa: 1 }, msg), '*'); }
  function reply(id, ok, result, error) { toFrame({ type: 'reply', id: id, ok: ok, result: result, error: error }); }
  function api(path, body) {
    return fetch(base + path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (d) { if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status)); return d; }); });
  }
  function closeRoom(room) { if (sources[room]) { sources[room].close(); delete sources[room]; } }
  function join(m) {
    var room = String(m.room || '');
    if (!ROOM_RE.test(room)) throw new Error('ชื่อห้องไม่ถูกต้อง');
    if (!sources[room] && Object.keys(sources).length >= 5) throw new Error('เข้าห้องได้ไม่เกิน 5 ห้อง');
    closeRoom(room);
    var url = base + '/events?cid=' + cid + '&room=' + encodeURIComponent(room) + '&name=' + encodeURIComponent(String(m.name || '').slice(0, 24));
    var es = new EventSource(url);
    sources[room] = es;
    ['hello', 'join', 'leave', 'msg'].forEach(function (ev) {
      es.addEventListener(ev, function (e) { var data; try { data = JSON.parse(e.data); } catch (x) { return; } toFrame({ type: 'room', room: room, ev: ev, data: data }); });
    });
    es.addEventListener('fatal', function (e) { var d = {}; try { d = JSON.parse(e.data); } catch (x) {} toFrame({ type: 'room', room: room, ev: 'error', data: { error: d.error || 'เข้าห้องไม่ได้' } }); closeRoom(room); });
    es.onerror = function () { if (es.readyState === 2) { toFrame({ type: 'room', room: room, ev: 'error', data: { error: 'เชื่อมต่อไม่ได้' } }); delete sources[room]; } };
  }
  window.addEventListener('message', function (e) {
    var f = frame(); if (!f || e.source !== f.contentWindow) return;
    var m = e.data; if (!m || m.alexa !== 1 || typeof m.cmd !== 'string') return;
    var id = m.id;
    if (m.cmd === 'hello') { api('/db', { cid: cid, op: 'whoami' }).then(function (d) { toFrame({ type: 'ready', cid: d.result }); }, function () {}); return; }
    try {
      if (m.cmd === 'join') { join(m); reply(id, true, true); }
      else if (m.cmd === 'leave') { closeRoom(String(m.room || '')); reply(id, true, true); }
      else if (m.cmd === 'send') { if (!ROOM_RE.test(String(m.room || ''))) throw new Error('ชื่อห้องไม่ถูกต้อง'); api('/send', { cid: cid, room: m.room, data: m.data, keep: !!m.keep, self: m.self !== false }).then(function () { reply(id, true, true); }, function (er) { reply(id, false, null, er.message); }); }
      else if (m.cmd === 'db') { api('/db', { cid: cid, op: m.op, scope: m.scope, key: m.key, value: m.value, prefix: m.prefix, by: m.by }).then(function (d) { reply(id, true, d.result); }, function (er) { reply(id, false, null, er.message); }); }
    } catch (er) { reply(id, false, null, er.message); }
  });
}
const SDK_JS = '(' + alexaSdk.toString() + ')();';
const HOST_JS = '(' + alexaHost.toString() + ')();';

function serveJs(res, src) {
  res.writeHead(200, { 'Content-Type': 'application/javascript; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' });
  res.end(src);
}

function serveSite(req, res, slug, raw) {
  const site = own(hub.sites, slug);
  const viewer = userOf(req) || userFromQueryToken(req);
  const noRobots = 'noindex, nofollow';
  const owner = site && getUser(site.owner);
  const ownerExpired = owner ? trialExpired(owner) : false;
  const isPublicVisible = site && site.public && !ownerExpired;
  if (!site || (!isPublicVisible && viewer !== site.owner)) {
    res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8', 'X-Robots-Tag': noRobots, 'Cache-Control': 'no-store' });
    return res.end('<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ไม่พบเว็บ</title><body style="background:#000;color:#fff;font-family:system-ui,sans-serif;text-align:center;padding:20vh 20px"><h2>ไม่พบเว็บนี้</h2><p><a style="color:#fff" href="/">ALEXA HUB</a></p></body>');
  }
  if (raw) {
    const dest = req.headers['sec-fetch-dest'];
    if (dest && dest !== 'iframe' && dest !== 'frame') { res.writeHead(302, { Location: '/s/' + site.slug }); return res.end(); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Security-Policy': 'sandbox ' + SANDBOX_FLAGS, 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'SAMEORIGIN', 'Referrer-Policy': 'no-referrer', 'X-Robots-Tag': noRobots, 'Cache-Control': 'no-cache' });
    return res.end(injectSdk(buildSitePage(site)));
  }
  if (viewer !== site.owner) { site.views = (site.views || 0) + 1; trackVisitor(site, ipOf(req)); saveHub(); }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; script-src 'self'; connect-src 'self'; frame-src 'self'; base-uri 'none'", 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'X-Robots-Tag': noRobots, 'Cache-Control': 'no-store' });
  const bar = SITE_BANNER ? '<div class="bar"><a href="/">ALEXA HUB</a><span>เว็บนี้สร้างโดยสมาชิก @' + escHtml(site.owner) + '</span></div>' : '';
  res.end('<!doctype html><html lang="th"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>' + escHtml(site.title) + '</title><style>html,body{margin:0;height:100%;background:#000}iframe{position:fixed;left:0;right:0;bottom:0;top:0;width:100%;height:100%;border:0;background:#fff}' + (SITE_BANNER ? 'body.b iframe{top:34px;height:calc(100% - 34px)}.bar{position:fixed;left:0;right:0;top:0;height:34px;z-index:2;display:flex;gap:12px;align-items:center;justify-content:space-between;padding:0 12px;background:#0b0b0b;border-bottom:1px solid #333;font:13px system-ui,sans-serif;color:#eee}.bar a{color:#fff;text-decoration:none;font-weight:800;letter-spacing:.2em}.bar span{color:#9a9a9a;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}' : '') + '</style><script src="/s/_host.js"></script></head><body' + (SITE_BANNER ? ' class="b"' : '') + '>' + bar + '<iframe id="f" src="/s/' + site.slug + '/raw" sandbox="' + SANDBOX_FLAGS + '" allow="fullscreen; autoplay; gamepad" referrerpolicy="no-referrer" allowfullscreen></iframe></body></html>');
}

// ============ SAPI ============
const rooms = new Map();
let sseCount = 0;
const sseByIp = new Map(), sapiHits = new Map(), buckets = new Map();
const SSE_HEADERS = { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no', 'X-Content-Type-Options': 'nosniff' };
const pidOf = (slug, cid) => crypto.createHash('sha256').update(slug + ':' + cid).digest('hex').slice(0, 16);

function sapiLimited(ip) {
  const now = Date.now();
  let f = sapiHits.get(ip);
  if (!f || now - f.t > 10000) { f = { n: 0, t: now }; sapiHits.set(ip, f); }
  f.n++; return f.n > 600;
}
function takeToken(key, rate, burst) {
  const now = Date.now();
  let b = buckets.get(key);
  if (!b) { b = { tokens: burst, last: now }; buckets.set(key, b); }
  b.tokens = Math.min(burst, b.tokens + (now - b.last) / 1000 * rate);
  b.last = now;
  if (b.tokens < 1) return false;
  b.tokens -= 1; return true;
}
function sseSend(c, ev, data) {
  try { if (c.res.writableLength > 1000000) { c.res.destroy(); return; } c.res.write('event: ' + ev + '\ndata: ' + JSON.stringify(data) + '\n\n'); } catch {}
}
function roomBroadcast(room, ev, data, exceptCid) { for (const c of room.clients.values()) if (c.cid !== exceptCid) sseSend(c, ev, data); }
function onlineCount(slug) { let n = 0; for (const r of rooms.values()) if (r.slug === slug) n += r.clients.size; return n; }
function closeSiteRooms(slug) {
  for (const [k, r] of Array.from(rooms.entries())) {
    if (r.slug !== slug) continue;
    for (const c of Array.from(r.clients.values())) { try { c.res.end(); } catch {} }
    rooms.delete(k);
  }
}
function sapiEvents(req, res, url, site, ip) {
  const cid = String(url.searchParams.get('cid') || '');
  const roomName = String(url.searchParams.get('room') || '');
  const nick = String(url.searchParams.get('name') || '').replace(/[\r\n]/g, ' ').slice(0, 24);
  if (!CID_RE.test(cid) || !ROOM_RE.test(roomName)) return json(res, 400, { error: 'ข้อมูลห้องไม่ถูกต้อง' });
  const fatal = (msg) => { res.writeHead(200, SSE_HEADERS); res.end('event: fatal\ndata: ' + JSON.stringify({ error: msg }) + '\n\n'); };
  if (sseCount >= MAX_SSE) return fatal('เต็ม');
  if ((sseByIp.get(ip) || 0) >= 40) return fatal('เชื่อมต่อจากเครื่องนี้มากเกินไป');
  const key = site.slug + '|' + roomName;
  let room = rooms.get(key);
  if (!room) {
    let n = 0;
    for (const r of rooms.values()) if (r.slug === site.slug) n++;
    if (n >= SITE_ROOMS_MAX) return fatal('เว็บนี้มีห้องมากเกินไป');
    room = { slug: site.slug, name: roomName, clients: new Map(), history: [], emptySince: 0 };
    rooms.set(key, room);
  }
  const old = room.clients.get(cid);
  if (!old && room.clients.size >= ROOM_MAX) return fatal('ห้องเต็ม');
  if (old) { room.clients.delete(cid); try { old.res.end(); } catch {} }
  req.socket.setNoDelay(true); req.socket.setTimeout(0);
  res.writeHead(200, SSE_HEADERS);
  const pid = pidOf(site.slug, cid);
  const client = { cid, pid, name: nick, res, ip };
  room.clients.set(cid, client); room.emptySince = 0;
  sseCount++; sseByIp.set(ip, (sseByIp.get(ip) || 0) + 1);
  res.write('retry: 3000\n\n');
  sseSend(client, 'hello', { id: pid, peers: Array.from(room.clients.values()).map((c) => ({ id: c.pid, name: c.name })), history: room.history });
  if (!old) roomBroadcast(room, 'join', { id: pid, name: nick }, cid);
  const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 20000);
  res.on('close', () => {
    clearInterval(ping); sseCount--;
    const left = (sseByIp.get(ip) || 1) - 1;
    if (left <= 0) sseByIp.delete(ip); else sseByIp.set(ip, left);
    if (room.clients.get(cid) === client) {
      room.clients.delete(cid);
      roomBroadcast(room, 'leave', { id: pid, name: nick });
      if (!room.clients.size) room.emptySince = Date.now();
    }
  });
}
function dbExec(slug, cid, scope, op, d) {
  const site = () => sdata.sites[slug] || null;
  const store = (create) => {
    let s = site();
    if (!s) { if (!create) return null; s = sdata.sites[slug] = { shared: {}, priv: {} }; }
    if (scope === 'shared') return s.shared;
    if (!s.priv[cid]) { if (!create) return null; s.priv[cid] = {}; }
    return s.priv[cid];
  };
  const key = d.key === undefined ? '' : String(d.key);
  if (['get', 'set', 'del', 'incr'].includes(op) && !SITE_KEY_RE.test(key)) return { status: 400, error: 'ชื่อคีย์ไม่ถูกต้อง' };
  const k = 'k:' + key;
  if (op === 'get') { const st = store(false); return { result: st && hasOwn(st, k) ? JSON.parse(st[k].v) : null }; }
  if (op === 'list') {
    const st = store(false); const prefix = String(d.prefix || '').slice(0, 64);
    const out = []; let size = 0;
    if (st) {
      const ks = Object.keys(st).filter((x) => x.slice(2).startsWith(prefix)).sort();
      for (const x of ks) { size += st[x].v.length; if (out.length >= 200 || size > 512 * 1024) break; out.push({ key: x.slice(2), value: JSON.parse(st[x].v) }); }
    }
    return { result: out };
  }
  if (op === 'del') { const st = store(false); if (st && hasOwn(st, k)) { delete st[k]; if (scope === 'mine' && !Object.keys(st).length && site()) delete site().priv[cid]; saveSData(); } return { result: true }; }
  if (op === 'clear') { if (scope !== 'mine') return { status: 403, error: 'ล้างได้เฉพาะเจ้าของ' }; const s = site(); if (s && s.priv[cid]) { delete s.priv[cid]; saveSData(); } return { result: true }; }
  if (op === 'set' || op === 'incr') {
    let value;
    if (op === 'incr') {
      const by = d.by === undefined ? 1 : Number(d.by);
      if (!Number.isFinite(by) || Math.abs(by) > 1e9) return { status: 400, error: 'ค่า incr ไม่ถูกต้อง' };
      const st0 = store(false); let curv = 0;
      if (st0 && hasOwn(st0, k)) { curv = JSON.parse(st0[k].v); if (typeof curv !== 'number') return { status: 400, error: 'คีย์นี้ไม่ใช่ตัวเลข' }; }
      value = curv + by;
    } else value = d.value === undefined ? null : d.value;
    const vs = JSON.stringify(value);
    if (vs.length > SITE_VALUE_MAX) return { status: 413, error: 'ข้อมูลใหญ่เกิน ' + Math.round(SITE_VALUE_MAX / 1024) + ' KB' };
    const s = site(); let bytes = 0, keys = 0;
    if (s) for (const st of [s.shared].concat(Object.values(s.priv))) for (const kk in st) { bytes += kk.length + st[kk].v.length; keys++; }
    const stNow = store(false); const existed = !!(stNow && hasOwn(stNow, k));
    if (existed) bytes -= k.length + stNow[k].v.length; else keys += 1;
    bytes += k.length + vs.length;
    if (bytes > SITE_DB_BYTES || keys > SITE_KEYS_MAX) return { status: 413, error: 'ที่เก็บข้อมูลเต็ม' };
    if (scope === 'mine') {
      if (stNow && !existed && Object.keys(stNow).length >= 100) return { status: 400, error: 'ส่วนตัวได้ไม่เกิน 100 คีย์' };
      if (!stNow && s && Object.keys(s.priv).length >= 500) return { status: 429, error: 'มีผู้ใช้เก็บข้อมูลส่วนตัวมากเกินไป' };
    }
    const st = store(true);
    st[k] = { v: vs, t: Date.now() };
    saveSData();
    return { result: op === 'incr' ? value : true };
  }
  return { status: 400, error: 'คำสั่งไม่ถูกต้อง' };
}
function siteDataUsage(slug) {
  const s = sdata.sites[slug]; let bytes = 0, keys = 0;
  if (s) for (const st of [s.shared].concat(Object.values(s.priv))) for (const kk in st) { bytes += kk.length + st[kk].v.length; keys++; }
  return { bytes, keys, limit: SITE_DB_BYTES, online: onlineCount(slug) };
}
async function handleSapi(req, res, url, slug, kind) {
  const site = own(hub.sites, slug);
  if (!site || (!site.public && userOf(req) !== site.owner)) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
  const owner = getUser(site.owner);
  if (owner && trialExpired(owner) && userOf(req) !== site.owner) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
  const ip = ipOf(req);
  if (sapiLimited(ip)) return json(res, 429, { error: 'ถี่เกินไป' });
  if (kind === 'events') {
    if (req.method !== 'GET') return json(res, 405, { error: 'method not allowed' });
    return sapiEvents(req, res, url, site, ip);
  }
  if (req.method !== 'POST') return json(res, 405, { error: 'method not allowed' });
  const d = await readBody(req);
  const cid = String(d.cid || '');
  if (!CID_RE.test(cid)) return json(res, 400, { error: 'รหัสผู้เข้าชมไม่ถูกต้อง' });
  if (kind === 'send') {
    const roomName = String(d.room || '');
    const room = ROOM_RE.test(roomName) ? rooms.get(site.slug + '|' + roomName) : null;
    const client = room && room.clients.get(cid);
    if (!client) return json(res, 409, { error: 'ยังไม่ได้เข้าห้อง' });
    if (!takeToken('s|' + site.slug + '|' + cid, 30, 60)) return json(res, 429, { error: 'ส่งถี่เกินไป' });
    const data = d.data === undefined ? null : d.data;
    if (JSON.stringify(data).length > 8192) return json(res, 413, { error: 'ใหญ่เกิน 8 KB' });
    const msg = { from: client.pid, name: client.name, data, t: Date.now() };
    if (d.keep) { room.history.push(msg); if (room.history.length > 50) room.history.shift(); }
    roomBroadcast(room, 'msg', msg, d.self === false ? cid : null);
    return json(res, 200, { ok: true });
  }
  const op = String(d.op || '');
  if (op === 'whoami') return json(res, 200, { result: pidOf(site.slug, cid) });
  if (!takeToken('d|' + site.slug + '|' + cid, 10, 30)) return json(res, 429, { error: 'ถี่เกินไป' });
  const scope = d.scope === 'mine' ? 'mine' : 'shared';
  const out = dbExec(site.slug, cid, scope, op, d);
  if (out.error) return json(res, out.status || 400, { error: out.error });
  return json(res, 200, { result: out.result });
}

function json(res, code, obj, headers) {
  res.writeHead(code, Object.assign({ 'Content-Type': 'application/json; charset=utf-8', 'X-Content-Type-Options': 'nosniff' }, headers || {}));
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let s = '', n = 0;
    req.on('data', (c) => { n += c.length; if (n > 1e6) { reject(Object.assign(new Error('ใหญ่เกินไป'), { status: 413 })); req.destroy(); return; } s += c; });
    req.on('end', () => { try { resolve(s ? JSON.parse(s) : {}); } catch { reject(Object.assign(new Error('JSON เสีย'), { status: 400 })); } });
    req.on('error', reject);
  });
}

// ============ SITE TIER HELPERS ============
function fmtMB(n) { return (n / 1048576).toFixed(1) + 'MB'; }
function genSiteToken() { return 'sk_' + crypto.randomBytes(24).toString('hex'); }
function hashIp(ip) { return crypto.createHash('sha256').update(String(ip) + '|alexa').digest('hex').slice(0, 12); }

function trackVisitor(site, ip) {
  if (!site.visitors) site.visitors = { total: 0, uniq: [], daily: {} };
  const v = site.visitors;
  v.total = (v.total || 0) + 1;
  const h = hashIp(ip);
  if (!Array.isArray(v.uniq)) v.uniq = [];
  if (!v.uniq.includes(h)) { v.uniq.push(h); if (v.uniq.length > 10000) v.uniq.shift(); }
  const day = new Date().toISOString().slice(0, 10);
  if (!v.daily) v.daily = {};
  v.daily[day] = (v.daily[day] || 0) + 1;
  const keys = Object.keys(v.daily);
  if (keys.length > 30) { keys.sort(); while (keys.length > 30) delete v.daily[keys.shift()]; }
}

function sitePushConsole(slug, level, msg, src) {
  const site = own(hub.sites, slug);
  if (!site) return;
  if (!site.console) site.console = [];
  site.console.push({ t: Date.now(), lv: level, m: String(msg).slice(0, 1000), src: src || 'client' });
  if (site.console.length > SITE_CONSOLE_MAX) site.console.splice(0, site.console.length - SITE_CONSOLE_MAX);
  saveHub();
}

function siteUsage(slug) {
  let bytes = 0, count = 0;
  for (const f of Object.values(hub.siteFiles)) if (f.slug === slug) { bytes += f.size || 0; count++; }
  return { bytes, count };
}

function findTokenOfSlug(slug) {
  for (const [t, info] of Object.entries(hub.siteTokens)) if (info.slug === slug) return { token: t, info };
  return null;
}

async function deleteSiteFile(fileId) {
  const f = own(hub.siteFiles, fileId);
  if (!f) return false;
  try {
    if (f.upstashPath) {
      const bk = getBucketById(f.bucketId || 0);
      if (bk) {
        try {
          await bk.bucket.delete(f.upstashPath);
          bk.used = Math.max(0, (bk.used || 0) - (f.size || 0));
        } catch (e) { console.error('del sitefile:', e.message); }
      }
    } else {
      fs.unlink(path.join(MEDIA_DIR, fileId + '.' + f.ext), () => {});
    }
  } catch (e) {}
  delete hub.siteFiles[fileId];
  return true;
}

// ============ SITE NPM INSTALL ============
const siteInstalls = {};

function sitePkgPath(slug) { return path.join(siteDir(slug), 'package.json'); }

function readSitePkg(slug) {
  try {
    const p = sitePkgPath(slug);
    if (!fs.existsSync(p)) return null;
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch (e) { return null; }
}

function writeSitePkg(slug, pkg) {
  const dir = siteDir(slug);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(sitePkgPath(slug), JSON.stringify(pkg, null, 2));
}

function runSiteNpmInstall(slug) {
  return new Promise((resolve) => {
    const dir = siteDir(slug);
    if (!fs.existsSync(path.join(dir, 'package.json'))) return resolve({ ok: true });
    const env = Object.assign({}, process.env, {
      HOME: dir,
      npm_config_cache: path.join(dir, '.npm-cache'),
      npm_config_update_notifier: 'false',
      npm_config_fund: 'false',
      npm_config_audit: 'false'
    });
    let child;
    try { child = spawn('npm', ['install', '--no-audit', '--no-fund'], { cwd: dir, env }); }
    catch (e) { return resolve({ ok: false, msg: e.message }); }
    sitePushConsole(slug, 'sys', 'npm install เริ่ม...', 'server');
    if (siteInstalls[slug]) siteInstalls[slug].child = child;
    child.stdout.on('data', (d) => sitePushConsole(slug, 'out', d, 'server'));
    child.stderr.on('data', (d) => sitePushConsole(slug, 'err', d, 'server'));
    const to = setTimeout(() => {
      try { child.kill('SIGKILL'); } catch {}
      resolve({ ok: false, msg: 'ติดตั้งนานเกิน 5 นาที' });
    }, 300000);
    child.on('exit', (code) => {
      clearTimeout(to);
      sitePushConsole(slug, code === 0 ? 'sys' : 'err', code === 0 ? 'ติดตั้งสำเร็จ' : 'ติดตั้งล้มเหลว (exit ' + code + ')', 'server');
      resolve({ ok: code === 0, msg: code ? 'exit ' + code : '' });
    });
    child.on('error', (e) => { clearTimeout(to); resolve({ ok: false, msg: e.message }); });
  });
}

// ============ FRIENDS ============
function ensureFriendArrays(x) {
  if (!Array.isArray(x.friends)) x.friends = [];
  if (!Array.isArray(x.friendReqIn)) x.friendReqIn = [];
  if (!Array.isArray(x.friendReqOut)) x.friendReqOut = [];
}
function findOrCreateChat(a, b) {
  const id = chatId(a, b);
  let c = hub.chats[id];
  if (!c) {
    c = { id, members: [a, b].sort(), created: Date.now(), updated: Date.now(), read: {}, msgs: [] };
    hub.chats[id] = c;
  }
  return c;
}

// ============ SITE RUNTIME (Node.js) ============
const siteProcs = {};
const sitePorts = new Set();

function siteDir(slug) { return path.join(SITES_DIR, slug); }
function siteServerFile(slug) { return path.join(siteDir(slug), 'server.js'); }
function sitePkgFile(slug) { return path.join(siteDir(slug), 'package.json'); }

function allocPort() {
  for (let i = SITE_RUNTIME_PORT_MIN; i <= SITE_RUNTIME_PORT_MAX; i++) {
    if (!sitePorts.has(i)) { sitePorts.add(i); return i; }
  }
  return null;
}
function releasePort(p) { sitePorts.delete(p); }

async function writeSiteFiles(site) {
  const dir = siteDir(site.slug);
  fs.mkdirSync(dir, { recursive: true });
  if (site.packageJson) fs.writeFileSync(sitePkgFile(site.slug), site.packageJson);
  if (site.serverCode) fs.writeFileSync(siteServerFile(site.slug), site.serverCode);
  if (site.html) fs.writeFileSync(path.join(dir, 'index.html'), site.html);
  if (site.css) fs.writeFileSync(path.join(dir, 'style.css'), site.css);
  if (site.js) fs.writeFileSync(path.join(dir, 'client.js'), site.js);
}

async function installSiteDeps(slug) {
  const dir = siteDir(slug);
  if (!SITE_RUNTIME_AUTO_INSTALL) return { ok: true };
  if (!fs.existsSync(path.join(dir, 'package.json'))) return { ok: true };
  return new Promise((resolve) => {
    const env = Object.assign({}, process.env, { HOME: dir, npm_config_cache: path.join(dir, '.npm-cache'), npm_config_update_notifier: 'false', npm_config_fund: 'false', npm_config_audit: 'false' });
    let child;
    try { child = spawn('npm', ['install', '--no-audit', '--no-fund'], { cwd: dir, env }); }
    catch (e) { return resolve({ ok: false, msg: e.message }); }
    sitePushConsole(slug, 'sys', 'npm install เริ่ม...', 'server');
    child.stdout.on('data', (d) => sitePushConsole(slug, 'out', d, 'server'));
    child.stderr.on('data', (d) => sitePushConsole(slug, 'err', d, 'server'));
    const to = setTimeout(() => { try { child.kill('SIGKILL'); } catch {} resolve({ ok: false, msg: 'ติดตั้งนานเกิน 5 นาที' }); }, 300000);
    child.on('exit', (code) => { clearTimeout(to); resolve({ ok: code === 0, msg: code ? 'exit ' + code : '' }); });
    child.on('error', (e) => { clearTimeout(to); resolve({ ok: false, msg: e.message }); });
  });
}

async function startSiteRuntime(slug) {
  const site = own(hub.sites, slug);
  if (!site || site.tier !== 'online') return { ok: false, msg: 'ไม่ใช่เว็บ online' };
  if (siteProcs[slug]) return { ok: true, msg: 'รันอยู่แล้ว', port: siteProcs[slug].port };
  if (!site.serverCode) return { ok: false, msg: 'ยังไม่มี server.js' };
  if (!findTokenOfSlug(slug)) return { ok: false, msg: 'ยังไม่มี token (ติดต่อแอดมิน)' };
  const port = allocPort();
  if (!port) return { ok: false, msg: 'port เต็ม (สูงสุด ' + (SITE_RUNTIME_PORT_MAX - SITE_RUNTIME_PORT_MIN + 1) + ')' };
  try { await writeSiteFiles(site); } catch (e) { releasePort(port); return { ok: false, msg: 'เขียนไฟล์ไม่ได้: ' + e.message }; }
  const r = await installSiteDeps(slug);
  if (!r.ok) { releasePort(port); sitePushConsole(slug, 'err', 'dependency ไม่ผ่าน: ' + r.msg, 'server'); return { ok: false, msg: 'dependency: ' + r.msg }; }
  const dir = siteDir(slug);
  const env = Object.assign({}, process.env, {
    PATH: process.env.PATH || '/usr/local/bin:/usr/bin:/bin',
    HOME: dir, TMPDIR: dir,
    PORT: String(port), HOST: '127.0.0.1', NODE_ENV: 'production',
    UPSTASH_REDIS_REST_URL: REDIS_URL || '',
    UPSTASH_REDIS_REST_TOKEN: REDIS_TOKEN || '',
    SITE_SLUG: slug
  });
  const tok = findTokenOfSlug(slug);
  if (tok) env.SITE_TOKEN = tok.token;
  let child;
  try { child = spawn('node', [siteServerFile(slug)], { cwd: dir, env }); }
  catch (e) { releasePort(port); return { ok: false, msg: e.message }; }
  siteProcs[slug] = { child, port, startedAt: Date.now() };
  site.desired = true; site.runtimePort = port;
  save();
  sitePushConsole(slug, 'sys', 'เริ่มรัน server ที่ port ' + port, 'server');
  child.stdout.on('data', (d) => sitePushConsole(slug, 'out', d, 'server'));
  child.stderr.on('data', (d) => sitePushConsole(slug, 'err', d, 'server'));
  child.on('error', (e) => sitePushConsole(slug, 'err', 'process error: ' + e.message, 'server'));
  child.on('exit', (code, sig) => {
    sitePushConsole(slug, 'sys', 'server exit: ' + (sig || code), 'server');
    delete siteProcs[slug];
    releasePort(port);
    const s = own(hub.sites, slug);
    if (s) { s.desired = false; save(); }
  });
  return { ok: true, port };
}

function stopSiteRuntime(slug) {
  return new Promise((resolve) => {
    const p = siteProcs[slug];
    if (!p) return resolve();
    p.child.once('exit', () => resolve());
    p.child.kill('SIGTERM');
    setTimeout(() => { try { p.child.kill('SIGKILL'); } catch {} }, 4000);
  });
}

async function proxySiteRuntime(req, res, slug, subpath) {
  const p = siteProcs[slug];
  if (!p) {
    res.writeHead(502, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end('<!doctype html><meta charset="utf-8"><body style="background:#000;color:#fff;font-family:system-ui,sans-serif;text-align:center;padding:15vh 20px"><h2>Server ยังไม่เปิด</h2><p>รอสักครู่หรือเปิดในหน้าตั้งค่า</p></body>');
  }
  const url = new URL(req.url, 'http://x');
  const targetPath = '/' + subpath + (url.search || '');
  const fwdHeaders = Object.assign({}, req.headers);
  delete fwdHeaders['x-site-token'];
  delete fwdHeaders['authorization'];
  delete fwdHeaders['cookie'];
  fwdHeaders.host = '127.0.0.1:' + p.port;
  if (req.headers.host) fwdHeaders['x-forwarded-host'] = req.headers.host;
  fwdHeaders['x-forwarded-for'] = ipOf(req);
  fwdHeaders['x-forwarded-proto'] = (req.headers['x-forwarded-proto'] || 'http').split(',')[0].trim();
  const proxy = http.request({
    hostname: '127.0.0.1', port: p.port, path: targetPath,
    method: req.method, headers: fwdHeaders
  }, (up) => {
    res.writeHead(up.statusCode, up.headers);
    up.pipe(res);
  });
  proxy.on('error', (e) => {
    if (!res.headersSent) res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('proxy error: ' + e.message);
  });
  req.pipe(proxy);
}

// ============ ADMIN DELETE ============
async function deleteUserData(username) {
  const u = String(username).toLowerCase();
  for (const [id, b] of Object.entries(bots)) {
    if (b.owner !== u) continue;
    await stopBot(id);
    try { fs.rmSync(path.join(BOTS_DIR, id), { recursive: true, force: true }); } catch {}
    delete bots[id]; delete logs[id];
  }
  for (const [id, p] of Object.entries(hub.posts)) {
    if (p.owner !== u) continue;
    if (p.cover) await deleteMediaFile(p.cover);
    delete hub.posts[id];
  }
  for (const slug of Object.keys(hub.sites)) {
    if (hub.sites[slug].owner === u) {
      closeSiteRooms(slug);
      await stopSiteRuntime(slug);
      const tok = findTokenOfSlug(slug);
      if (tok) delete hub.siteTokens[tok.token];
      for (const [fid, f] of Object.entries(hub.siteFiles)) if (f.slug === slug) await deleteSiteFile(fid);
      try { fs.rmSync(siteDir(slug), { recursive: true, force: true }); } catch {}
      delete hub.sites[slug]; delete sdata.sites[slug];
    }
  }
  for (const [id, c] of Object.entries(hub.chats)) {
    if (c.members.includes(u)) delete hub.chats[id];
  }
  for (const [id, m] of Object.entries(hub.media)) {
    if (m.owner !== u) continue;
    await deleteMediaFile(id);
  }
  for (const uu of Object.keys(state.users)) {
    const x = state.users[uu];
    ensureFriendArrays(x);
    x.friends = x.friends.filter((f) => f !== u);
    x.friendReqIn = x.friendReqIn.filter((f) => f !== u);
    x.friendReqOut = x.friendReqOut.filter((f) => f !== u);
  }
  delete state.users[u];
  for (const [k, s] of Object.entries(state.sessions)) if (s.user === u) delete state.sessions[k];
  await saveNow(); await saveHubNow(); writeSDataDisk();
}

function findByPublicId(id) {
  const target = String(id || '').trim().toUpperCase();
  if (!target) return null;
  for (const b of Object.values(bots)) if ((b.publicId || '').toUpperCase() === target) return { type: 'bot', data: b };
  for (const x of Object.values(hub.sites)) if ((x.publicId || '').toUpperCase() === target) return { type: 'site', data: x };
  return null;
}

// ============ HTTP SERVER ============
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x');
    const p = url.pathname;
    const M = req.method;

    if (M === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'X-Frame-Options': 'DENY', 'X-Content-Type-Options': 'nosniff' });
      return res.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html')));
    }
    if (M === 'GET' && p === '/s/_sdk.js') return serveJs(res, SDK_JS);
    if (M === 'GET' && p === '/s/_host.js') return serveJs(res, HOST_JS);
    const sm = p.match(/^\/s\/([a-z0-9][a-z0-9-]{2,29})(\/raw)?$/);
    if (sm && M === 'GET') return serveSite(req, res, sm[1], !!sm[2]);
    const sapi = p.match(/^\/sapi\/([a-z0-9][a-z0-9-]{2,29})\/(events|send|db)$/);
    if (sapi) return await handleSapi(req, res, url, sapi[1], sapi[2]);

    // Site Runtime Proxy
    const apm = p.match(/^\/s\/([a-z0-9][a-z0-9-]{2,29})\/app(?:\/(.*))?$/);
    if (apm && M === 'GET') {
      const site = own(hub.sites, apm[1]);
      if (!site || site.tier !== 'online') return json(res, 404, { error: 'not found' });
      return proxySiteRuntime(req, res, apm[1], apm[2] || '');
    }

    // Site File API
    const sl = p.match(/^\/sapi\/([a-z0-9][a-z0-9-]{2,29})\/file\/list$/);
    if (sl && M === 'GET') {
      const site = own(hub.sites, sl[1]);
      if (!site) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
      const tok = findTokenOfSlug(site.slug);
      if (!tok) return json(res, 400, { error: 'ยังไม่มี token' });
      const ownerOk = userOf(req) === site.owner;
      const tokenOk = String(req.headers['x-site-token'] || '') === tok.token;
      if (!ownerOk && !tokenOk) return json(res, 401, { error: 'unauthorized' });
      const files = Object.entries(hub.siteFiles).filter(([id, f]) => f.slug === site.slug).map(([id, f]) => ({ id, url: '/sapi/' + site.slug + '/file/' + id, size: f.size, mime: f.mime, name: f.originalName || '', created: f.created }));
      const usage = siteUsage(site.slug);
      return json(res, 200, { files, used: usage.bytes, quota: tok.info.quota });
    }
    const sf = p.match(/^\/sapi\/([a-z0-9][a-z0-9-]{2,29})\/file(?:\/([a-f0-9]{24}))?$/);
    if (sf) {
      const site = own(hub.sites, sf[1]);
      if (!site) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
      if (site.tier !== 'online') return json(res, 400, { error: 'เว็บนี้ไม่รองรับไฟล์' });
      const tok = findTokenOfSlug(site.slug);
      if (!tok) return json(res, 400, { error: 'ยังไม่มี token' });
      if (M === 'POST' && !sf[2]) {
        const ownerOk = userOf(req) === site.owner;
        const tokenOk = String(req.headers['x-site-token'] || '') === tok.token;
        if (!ownerOk && !tokenOk) return json(res, 401, { error: 'unauthorized' });
        const ct = String(req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
        const len = parseInt(req.headers['content-length'] || '0', 10) || 0;
        if (len > SITE_FILE_MAX) return json(res, 413, { error: 'ใหญ่เกิน ' + Math.round(SITE_FILE_MAX / 1048576) + 'MB' });
        const usage = siteUsage(site.slug);
        if (usage.bytes + len > tok.info.quota) return json(res, 413, { error: 'พื้นที่เต็ม (' + fmtMB(usage.bytes) + '/' + fmtMB(tok.info.quota) + ')' });
        let buf;
        try { buf = await readStreamToBuffer(req, SITE_FILE_MAX); }
        catch (e) { return json(res, e.status || 400, { error: e.message }); }
        const ext = extFor(ct, req.headers['x-file-name']);
        const fid = crypto.randomBytes(12).toString('hex');
        let r;
        try { r = await putMedia(buf, ext, ct); }
        catch (e) { return json(res, 500, { error: 'อัปโหลดไม่สำเร็จ' }); }
        hub.siteFiles[fid] = { slug: site.slug, token: tok.token, size: r.size, mime: ct, ext, upstashPath: r.upstashPath, bucketId: r.bucketId, originalName: String(decodeURIComponent(String(req.headers['x-file-name'] || ''))).slice(0, 200), created: Date.now() };
        await saveHubNow();
        return json(res, 200, { id: fid, url: '/sapi/' + site.slug + '/file/' + fid, size: r.size });
      }
      if (M === 'GET' && sf[2]) {
        const f = own(hub.siteFiles, sf[2]);
        if (!f || f.slug !== site.slug) return json(res, 404, { error: 'ไม่เจอไฟล์' });
        if (f.upstashPath) {
          const bk = getBucketById(f.bucketId || 0);
          if (!bk) return json(res, 500, { error: 'ไม่พบ bucket' });
          let buf = null;
          try { const got = await bk.bucket.get(f.upstashPath); buf = await blobResultToBuffer(got); } catch (e) {}
          if (!buf) return json(res, 500, { error: 'อ่านไม่ได้' });
          res.writeHead(200, { 'Content-Type': f.mime || 'application/octet-stream', 'Content-Length': buf.length, 'Cache-Control': 'public, max-age=86400', 'X-Content-Type-Options': 'nosniff' });
          return res.end(buf);
        }
        return json(res, 500, { error: 'ไฟล์หาย' });
      }
      if (M === 'DELETE' && sf[2]) {
        const ownerOk = userOf(req) === site.owner;
        const tokenOk = String(req.headers['x-site-token'] || '') === tok.token;
        if (!ownerOk && !tokenOk) return json(res, 401, { error: 'unauthorized' });
        const f = own(hub.siteFiles, sf[2]);
        if (!f || f.slug !== site.slug) return json(res, 404, { error: 'ไม่เจอไฟล์' });
        await deleteSiteFile(sf[2]);
        await saveHubNow();
        return json(res, 200, { ok: true });
      }
      return json(res, 405, { error: 'method not allowed' });
    }

    // Site Console
    const sc = p.match(/^\/sapi\/([a-z0-9][a-z0-9-]{2,29})\/console$/);
    if (sc && M === 'POST') {
      const site = own(hub.sites, sc[1]);
      if (!site) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
      const d = await readBody(req);
      const level = ['log', 'warn', 'error', 'info'].includes(d.level) ? d.level : 'log';
      const msg = String(d.msg || '').slice(0, 1000);
      if (msg) sitePushConsole(site.slug, level, msg, 'client');
      return json(res, 200, { ok: true });
    }

    // Media
    const mmPublic = p.match(/^\/media\/([a-f0-9]{24})$/);
    if (mmPublic && M === 'GET') return await serveMedia(req, res, mmPublic[1], userOf(req));
    if (p.startsWith('/media/')) return json(res, 404, { error: 'not found' });

    if (!p.startsWith('/api/')) return json(res, 404, { error: 'not found' });

    const ip = ipOf(req);

    if (p === '/api/register' && M === 'POST') {
      if (!SIGNUP_OPEN) return json(res, 403, { error: 'ปิดรับสมัคร' });
      if (tooMany(regCount, ip, 5, 3600000)) return json(res, 429, { error: 'สมัครบ่อยเกินไป' });
      const d = await readBody(req);
      const u = String(d.username || '').trim().toLowerCase();
      const pw = String(d.password || '');
      const em = String(d.email || '').trim().toLowerCase();
      if (!USERNAME_RE.test(u) || u === '__proto__') return json(res, 400, { error: 'ชื่อ 3-20 ตัว a-z 0-9 _' });
      if (pw.length < 6 || pw.length > 100) return json(res, 400, { error: 'รหัส 6-100 ตัว' });
      if (em && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) return json(res, 400, { error: 'อีเมลไม่ถูกต้อง' });
      if (getUser(u)) return json(res, 409, { error: 'ชื่อซ้ำ' });
      if (em) for (const x of Object.values(state.users)) if (x.email === em) return json(res, 409, { error: 'อีเมลซ้ำ' });
      const salt = crypto.randomBytes(16).toString('hex');
      const hash = (await scrypt(pw, salt, 64)).toString('hex');
      const code = genCode();
      state.users[u] = {
        salt, hash, uid: state.nextUid++, created: Date.now(),
        email: em || '', emailVerified: false,
        emailCode: em ? code : '', emailCodeExp: em ? Date.now() + 15 * 60 * 1000 : 0,
        displayName: '', bio: '', avatar: '',
        friends: [], friendReqIn: [], friendReqOut: [],
        maxBots: DEFAULT_MAX_BOTS, maxSites: DEFAULT_MAX_SITES,
        trialEnds: 0, paid: false
      };
      if (Object.keys(state.users).length === 1) for (const b of Object.values(bots)) if (!b.owner) { b.owner = u; fixPerm(b); }
      bump(regCount, ip, 3600000);
      await saveNow();
      let sent = { ok: true, dev: true };
      if (em) sent = await sendEmail(em, 'ALEXA HUB - ยืนยันอีเมล', '<h2>ALEXA HUB</h2><p>รหัส:</p><h1 style="font-size:32px;letter-spacing:8px">' + code + '</h1><p>อายุ 15 นาที</p>');
      const tok = newSession(u);
      const resp = { user: u, needsVerify: !!em, token: tok, info: pubMe(u) };
      if (em && !sent.ok && !sent.dev) resp.emailError = sent.msg;
      return json(res, 200, resp, { 'Set-Cookie': sessCookie(req, tok, SESSION_MS / 1000) });
    }

    if (p === '/api/verify' && M === 'POST') {
      const me = userOf(req);
      if (!me) return json(res, 401, { error: 'unauthorized' });
      const user = getUser(me);
      if (!user) return json(res, 401, { error: 'unauthorized' });
      if (!user.email) return json(res, 400, { error: 'ไม่มีอีเมล' });
      if (user.emailVerified) return json(res, 200, { ok: true, already: true });
      const d = await readBody(req);
      const code = String(d.code || '').trim();
      if (!/^\d{6}$/.test(code)) return json(res, 400, { error: 'ต้อง 6 หลัก' });
      if (!user.emailCode || !user.emailCodeExp || Date.now() > user.emailCodeExp) return json(res, 400, { error: 'รหัสหมดอายุ' });
      if (code !== user.emailCode) return json(res, 400, { error: 'รหัสไม่ถูกต้อง' });
      user.emailVerified = true; user.emailCode = ''; user.emailCodeExp = 0;
      if (!user.trialEnds || user.trialEnds < Date.now()) user.trialEnds = Date.now() + TRIAL_MS;
      await saveNow();
      return json(res, 200, { ok: true, trialEnds: user.trialEnds });
    }

    if (p === '/api/resend' && M === 'POST') {
      const me = userOf(req);
      if (!me) return json(res, 401, { error: 'unauthorized' });
      const user = getUser(me);
      if (!user || !user.email) return json(res, 400, { error: 'ไม่พบอีเมล' });
      if (user.emailVerified) return json(res, 400, { error: 'ยืนยันแล้ว' });
      if (tooMany(emailSends, ip, 3, 600000)) return json(res, 429, { error: 'รอ 10 นาที' });
      const code = genCode();
      user.emailCode = code; user.emailCodeExp = Date.now() + 15 * 60 * 1000;
      await saveNow(); bump(emailSends, ip, 600000);
      const sent = await sendEmail(user.email, 'ALEXA HUB - ยืนยันอีเมล', '<p>รหัส:</p><h1 style="font-size:32px;letter-spacing:8px">' + code + '</h1>');
      return json(res, 200, { ok: true, dev: !!sent.dev });
    }

    if (p === '/api/login' && M === 'POST') {
      if (tooMany(loginFails, ip, 10, 600000)) return json(res, 429, { error: 'ลองผิดบ่อย รอ 10 นาที' });
      const d = await readBody(req);
      const u = String(d.username || '').trim().toLowerCase();
      const pw = String(d.password || '');
      const user = getUser(u);
      const hash = await scrypt(pw, user ? user.salt : '0'.repeat(32), 64);
      const ok = user && crypto.timingSafeEqual(hash, Buffer.from(user.hash, 'hex'));
      if (!ok) { bump(loginFails, ip, 600000); return json(res, 401, { error: 'ชื่อหรือรหัสไม่ถูกต้อง' }); }
      loginFails.delete(ip);
      const tok = newSession(u);
      return json(res, 200, { user: u, token: tok, info: pubMe(u) }, { 'Set-Cookie': sessCookie(req, tok, SESSION_MS / 1000) });
    }

    if (p === '/api/logout' && M === 'POST') {
      const tok = getCookie(req, 'sid');
      if (tok) { delete state.sessions[sha(tok)]; save(); }
      return json(res, 200, { ok: true }, { 'Set-Cookie': sessCookie(req, '', 0) });
    }

    const me = userOf(req);
    if (!me) return json(res, 401, { error: 'unauthorized' });
    const meUser = getUser(me);
    ensureFriendArrays(meUser);

    if (p === '/api/me' && M === 'GET') return json(res, 200, pubMe(me));

    if (p === '/api/profile' && M === 'GET') {
      const x = meUser;
      return json(res, 200, {
        username: me, admin: isAdmin(me),
        displayName: x.displayName || '', bio: x.bio || '', avatar: x.avatar || '',
        email: x.email || '', emailVerified: !!x.emailVerified, hasEmail: !!x.email,
        trialEnds: x.trialEnds || 0, trialExpired: trialExpired(x),
        paid: !!x.paid
      });
    }
    if (p === '/api/profile' && M === 'PUT') {
      const d = await readBody(req);
      if (typeof d.displayName === 'string') meUser.displayName = d.displayName.trim().slice(0, 40);
      if (typeof d.bio === 'string') meUser.bio = d.bio.trim().slice(0, 300);
      await saveNow();
      return json(res, 200, { ok: true });
    }
    if (p === '/api/profile/avatar' && M === 'POST') {
      const ct = String(req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
      if (!ct.startsWith('image/')) return json(res, 400, { error: 'ต้องเป็นรูป' });
      const len = parseInt(req.headers['content-length'] || '0', 10) || 0;
      if (len > CHAT_IMG_MAX) return json(res, 413, { error: 'รูปใหญ่เกิน 5 MB' }, { Connection: 'close' });
      let buf;
      try { buf = await readStreamToBuffer(req, CHAT_IMG_MAX); }
      catch (e) { return json(res, e.status || 400, { error: e.message }, { Connection: 'close' }); }
      const ext = extFor(ct);
      let r;
      try { r = await putMedia(buf, ext, ct); }
      catch (e) { return json(res, 500, { error: 'อัปโหลดไม่สำเร็จ' }); }
      const old = meUser.avatar;
      if (old && hub.media[old]) await deleteMediaFile(old);
      hub.media[r.id] = { owner: me, mime: ct, ext, kind: 'image', scope: 'avatar', size: r.size, upstashPath: r.upstashPath, bucketId: r.bucketId, t: Date.now() };
      meUser.avatar = r.id;
      await saveNow(); await saveHubNow();
      return json(res, 200, { ok: true, avatar: r.id });
    }
    if (p === '/api/profile/password' && M === 'POST') {
      const d = await readBody(req);
      const cur = String(d.current || '');
      const n1 = String(d.new1 || '');
      const n2 = String(d.new2 || '');
      if (!cur) return json(res, 400, { error: 'ใส่รหัสปัจจุบัน' });
      if (n1.length < 6 || n1.length > 100) return json(res, 400, { error: 'รหัสใหม่ 6-100 ตัว' });
      if (n1 !== n2) return json(res, 400, { error: 'รหัสใหม่ไม่ตรงกัน' });
      const hash = await scrypt(cur, meUser.salt, 64);
      if (!crypto.timingSafeEqual(hash, Buffer.from(meUser.hash, 'hex'))) return json(res, 401, { error: 'รหัสปัจจุบันไม่ถูกต้อง' });
      const salt = crypto.randomBytes(16).toString('hex');
      const newHash = (await scrypt(n1, salt, 64)).toString('hex');
      meUser.salt = salt; meUser.hash = newHash;
      await saveNow();
      return json(res, 200, { ok: true });
    }
    if (p === '/api/profile/email' && M === 'POST') {
      if (tooMany(emailSends, ip, 3, 600000)) return json(res, 429, { error: 'รอ 10 นาที' });
      const d = await readBody(req);
      const em = String(d.email || '').trim().toLowerCase();
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) return json(res, 400, { error: 'อีเมลไม่ถูกต้อง' });
      for (const [k, x] of Object.entries(state.users)) if (k !== me && x.email === em) return json(res, 409, { error: 'อีเมลซ้ำ' });
      const code = genCode();
      meUser.email = em; meUser.emailVerified = false;
      meUser.emailCode = code; meUser.emailCodeExp = Date.now() + 15 * 60 * 1000;
      await saveNow(); bump(emailSends, ip, 600000);
      const sent = await sendEmail(em, 'ALEXA HUB - ยืนยันอีเมล', '<h2>ยืนยันอีเมล</h2><p>รหัส:</p><h1 style="font-size:32px;letter-spacing:8px">' + code + '</h1>');
      return json(res, 200, { ok: true, dev: !!sent.dev, error: sent.ok ? '' : sent.msg });
    }

    if (p === '/api/report' && M === 'POST') {
      if (tooMany(reportRate, ip, 5, 600000)) return json(res, 429, { error: 'ส่งบ่อยเกินไป' });
      const d = await readBody(req);
      const text = String(d.text || '').trim().slice(0, 2000);
      if (text.length < 5) return json(res, 400, { error: 'อธิบายอย่างน้อย 5 ตัวอักษร' });
      const mediaId = d.mediaId ? String(d.mediaId) : '';
      const fields = [
        { name: '👤 ผู้แจ้ง', value: '`@' + me + '`' + (meUser.displayName ? ' (' + meUser.displayName + ')' : ''), inline: true },
        { name: '🕐 เวลา', value: new Date().toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' }), inline: true }
      ];
      if (d.botId) fields.push({ name: '🤖 บอท ID', value: '`' + String(d.botId).slice(0, 20) + '`', inline: true });
      if (d.siteId) fields.push({ name: '🌐 เว็บ ID', value: '`' + String(d.siteId).slice(0, 20) + '`', inline: true });
      fields.push({ name: '📝 รายละเอียด', value: text.slice(0, 1024) });
      const embed = { title: '🚨 มีผู้แจ้งปัญหาใหม่', color: 0xff4d4f, fields, footer: { text: '🎁 รางวัลผู้เจอปัญหา: บอทฟรี 3 วัน — discord.gg/dTz2njT9fZ' }, timestamp: new Date().toISOString() };
      if (mediaId && hub.media[mediaId] && hub.media[mediaId].owner === me) {
        const m = hub.media[mediaId];
        if (m.kind === 'image') embed.image = { url: (req.headers.host ? 'https://' + req.headers.host : '') + '/media/' + mediaId };
        else fields.push({ name: '📎 ไฟล์แนบ', value: (req.headers.host ? 'https://' + req.headers.host : '') + '/media/' + mediaId });
      }
      const sent = await sendToDiscord(embed);
      bump(reportRate, ip, 600000);
      if (!sent.ok) return json(res, 500, { error: 'ส่งไม่สำเร็จ: ' + (sent.msg || 'unknown') });
      return json(res, 200, { ok: true });
    }

    // ===== Admin =====
    if (p === '/api/admin/state' && M === 'GET') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      return json(res, 200, { adminUi: !!state.adminUi });
    }
    if (p === '/api/admin/toggle' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      state.adminUi = !state.adminUi; await saveNow();
      return json(res, 200, { adminUi: !!state.adminUi });
    }
    if (p === '/api/admin/users' && M === 'GET') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const list = Object.keys(state.users).sort().map((u) => {
        const x = state.users[u];
        return {
          username: u, email: x.email || '', emailVerified: !!x.emailVerified,
          admin: isAdmin(u), paid: !!x.paid, displayName: x.displayName || '',
          created: x.created, trialEnds: x.trialEnds || 0, trialExpired: trialExpired(x),
          maxBots: x.maxBots ?? DEFAULT_MAX_BOTS, maxSites: x.maxSites ?? DEFAULT_MAX_SITES,
          bots: botCount(u), sites: Object.values(hub.sites).filter((s) => s.owner === u).length,
          posts: Object.values(hub.posts).filter((s) => s.owner === u).length,
          friends: (x.friends || []).length
        };
      });
      return json(res, 200, { users: list });
    }
    if (p === '/api/admin/update' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const u = String(d.username || '').toLowerCase();
      const x = getUser(u);
      if (!x) return json(res, 404, { error: 'ไม่เจอผู้ใช้' });
      if (typeof d.maxBots === 'number' && d.maxBots >= 0 && d.maxBots <= 999) x.maxBots = Math.floor(d.maxBots);
      if (typeof d.maxSites === 'number' && d.maxSites >= 0 && d.maxSites <= 999) x.maxSites = Math.floor(d.maxSites);
      if (typeof d.paid === 'boolean') x.paid = d.paid;
      if (typeof d.extendDays === 'number' && d.extendDays > 0 && d.extendDays <= 365) {
        const base = Math.max(Date.now(), x.trialEnds || Date.now());
        x.trialEnds = base + d.extendDays * 24 * 3600 * 1000;
      }
      if (typeof d.setTrialEnds === 'number') x.trialEnds = d.setTrialEnds;
      await saveNow();
      return json(res, 200, { ok: true });
    }
    if (p === '/api/admin/delete-user' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const u = String(d.username || '').toLowerCase();
      if (isAdmin(u)) return json(res, 400, { error: 'ลบแอดมินไม่ได้' });
      if (!getUser(u)) return json(res, 404, { error: 'ไม่เจอผู้ใช้' });
      await deleteUserData(u);
      return json(res, 200, { ok: true });
    }
    if (p === '/api/admin/lookup' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const found = findByPublicId(d.id);
      if (!found) return json(res, 404, { error: 'ไม่พบ ID' });
      const owner = getUser(found.data.owner);
      if (found.type === 'bot') {
        const b = found.data;
        return json(res, 200, { type: 'bot', owner: b.owner, ownerInfo: owner ? { email: owner.email || '', displayName: owner.displayName || '' } : null, bot: pub(b, true), running: !!procs[b.id] });
      } else {
        const x = found.data;
        const mySites = Object.values(hub.sites).filter((s) => s.owner === x.owner).length;
        return json(res, 200, { type: 'site', owner: x.owner, ownerInfo: owner ? { email: owner.email || '', displayName: owner.displayName || '' } : null, site: pubSite(x, me), html: x.html, css: x.css, js: x.js, sitesCount: mySites });
      }
    }
    if (p === '/api/admin/set-bot-timer' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const found = findByPublicId(d.id);
      if (!found) return json(res, 404, { error: 'ไม่พบ ID' });
      if (found.type !== 'bot') return json(res, 400, { error: 'ไม่ใช่บอท' });
      const b = found.data;
      const hours = Number(d.hours);
      if (!Number.isFinite(hours) || hours < 0 || hours > 8760) return json(res, 400, { error: 'ชั่วโมงไม่ถูกต้อง' });
      const fromNow = d.extend ? Math.max(Date.now(), b.expiresAt || Date.now()) : Date.now();
      b.expiresAt = hours === 0 ? 0 : fromNow + hours * 3600 * 1000;
      await saveNow(); addLog(b.id, 'sys', 'แอดมินตั้งเวลา: ' + (hours === 0 ? 'ไม่จำกัด' : hours + ' ชม.'));
      if (botExpired(b) && procs[b.id]) { addLog(b.id, 'err', 'หมดเวลา → หยุด'); stopBot(b.id).catch(() => {}); }
      return json(res, 200, { ok: true, expiresAt: b.expiresAt });
    }
    if (p === '/api/admin/bot-stop' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const found = findByPublicId(d.id);
      if (!found || found.type !== 'bot') return json(res, 404, { error: 'ไม่พบ ID' });
      await stopBot(found.data.id);
      return json(res, 200, { ok: true, bot: pub(found.data) });
    }
    if (p === '/api/admin/bot-start' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const found = findByPublicId(d.id);
      if (!found || found.type !== 'bot') return json(res, 404, { error: 'ไม่พบ ID' });
      const b = found.data;
      if (botExpired(b)) return json(res, 400, { error: BOT_EXPIRED_MSG });
      await stopBot(b.id);
      startBot(b.id);
      return json(res, 200, { ok: true, bot: pub(b) });
    }
    if (p === '/api/admin/bot-logs' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const found = findByPublicId(d.id);
      if (!found || found.type !== 'bot') return json(res, 404, { error: 'ไม่พบ ID' });
      return json(res, 200, { logs: logs[found.data.id] || [] });
    }
    if (p === '/api/admin/storage' && M === 'GET') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const totalBuckets = blobBuckets.length;
      const FREE_PER_BUCKET = 1 * 1024 * 1024 * 1024;
      const totalLimit = totalBuckets * FREE_PER_BUCKET;
      let totalUsed = 0;
      const bucketsInfo = [];
      for (const bk of blobBuckets) {
        const used = bk.used || 0;
        totalUsed += used;
        const pct = totalLimit ? (used / FREE_PER_BUCKET) * 100 : 0;
        bucketsInfo.push({ id: bk.id, used, limit: FREE_PER_BUCKET, percent: Math.min(100, pct), full: pct >= 95, nearlyFull: pct >= 80 && pct < 95 });
      }
      const fileCounts = {};
      let totalFiles = 0;
      for (const m of Object.values(hub.media)) {
        if (typeof m.bucketId === 'number') { fileCounts[m.bucketId] = (fileCounts[m.bucketId] || 0) + 1; totalFiles++; }
      }
      for (const b of bucketsInfo) b.files = fileCounts[b.id] || 0;
      const fullCount = bucketsInfo.filter(b => b.full).length;
      const nearlyCount = bucketsInfo.filter(b => b.nearlyFull).length;
      const overallPct = totalLimit ? Math.min(100, (totalUsed / totalLimit) * 100) : 0;
      return json(res, 200, { totalBuckets, totalLimit, totalUsed, overallPercent: overallPct, fullCount, nearlyCount, totalFiles, buckets: bucketsInfo });
    }
    if (p === '/api/admin/blob-debug' && M === 'GET') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const out = { buckets: [], mediaCount: Object.keys(hub.media).length, mediaSample: [] };
      for (const bk of blobBuckets) {
        const info = { id: bk.id, used: bk.used, methods: [] };
        const methods = ['put', 'get', 'delete', 'list', 'head', 'copy', 'signedReadUrl', 'presign', 'url', 'download', 'upload'];
        for (const m of methods) if (typeof bk.bucket[m] === 'function') info.methods.push(m);
        try {
          if (typeof bk.bucket.list === 'function') {
            const listed = await bk.bucket.list({ limit: 10 });
            info.listSample = listed;
          }
        } catch (e) { info.listError = e.message; }
        out.buckets.push(info);
      }
      const allMedia = Object.entries(hub.media).slice(0, 5);
      for (const [id, m] of allMedia) {
        out.mediaSample.push({ id, kind: m.kind, bucketId: m.bucketId, path: m.upstashPath, size: m.size });
      }
      return json(res, 200, out);
    }

    // ===== Site Install =====
    const instm = p.match(/^\/api\/sites\/([a-z0-9][a-z0-9-]{2,29})\/install$/);
    if (instm) {
      const site = own(hub.sites, instm[1]);
      if (!site) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
      if (site.owner !== me) return json(res, 403, { error: 'แก้ได้เฉพาะเจ้าของ' });
      if (site.tier !== 'online') return json(res, 400, { error: 'เว็บนี้ไม่รองรับ (ต้องเป็น online)' });
      if (M === 'GET') {
        let pkg = readSitePkg(site.slug);
        if (!pkg && site.packageJson) { try { pkg = JSON.parse(site.packageJson); } catch {} }
        const deps = (pkg && pkg.dependencies) || {};
        return json(res, 200, { deps, installing: !!siteInstalls[site.slug] });
      }
      if (M === 'POST') {
        if (siteInstalls[site.slug]) return json(res, 409, { error: 'กำลังติดตั้งอยู่' });
        const chk = canCreate(meUser);
        if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
        const d = await readBody(req);
        let pkg = readSitePkg(site.slug);
        if (!pkg && site.packageJson) { try { pkg = JSON.parse(site.packageJson); } catch {} }
        if (!pkg) pkg = { name: site.slug, version: '1.0.0', main: 'server.js', dependencies: {} };
        if (!pkg.dependencies) pkg.dependencies = {};
        let actionMsg = '';
        let added = [], removed = '';
        if (d.reinstall) { actionMsg = 'ติดตั้งใหม่ทั้งหมด'; added = Object.keys(pkg.dependencies); }
        else if (typeof d.remove === 'string' && d.remove.trim()) {
          const name = d.remove.trim();
          if (!pkg.dependencies[name]) return json(res, 404, { error: 'ไม่เจอ dependency นี้' });
          delete pkg.dependencies[name];
          removed = name;
          actionMsg = 'ลบ ' + name + ' แล้วติดตั้งใหม่';
        } else {
          const raw = typeof d.text === 'string' ? d.text : (Array.isArray(d.add) ? d.add.map(String).join(' ') : '');
          const ps = parseSpecs('js', raw);
          if (ps.bad.length) return json(res, 400, { error: 'ชื่อไม่ถูกต้อง: ' + ps.bad.slice(0, 5).join(', ') });
          if (!ps.specs.length) return json(res, 400, { error: 'ไม่พบชื่อไลบรารี' });
          if (ps.specs.length > 40) return json(res, 400, { error: 'ครั้งละไม่เกิน 40' });
          for (const spec of ps.specs) {
            let name, version;
            const mm2 = spec.match(/^(@[^/@]+\/[^@]+|[^@]+)(?:@(.+))?$/);
            if (mm2) { name = mm2[1]; version = mm2[2] ? '^' + mm2[2].replace(/^[\^~]/, '') : 'latest'; }
            else { name = spec; version = 'latest'; }
            pkg.dependencies[name] = version;
            added.push(name + '@' + version);
          }
          actionMsg = 'ติดตั้ง: ' + added.join(', ');
        }
        const depCount = Object.keys(pkg.dependencies).length;
        if (depCount > 100) return json(res, 400, { error: 'dependency เกิน 100 รายการ' });
        writeSitePkg(site.slug, pkg);
        site.packageJson = JSON.stringify(pkg, null, 2);
        await saveHubNow();
        const wasRunning = !!siteProcs[site.slug];
        if (wasRunning) await stopSiteRuntime(site.slug);
        siteInstalls[site.slug] = { t: Date.now(), child: null };
        sitePushConsole(site.slug, 'sys', actionMsg, 'server');
        try {
          const r = await runSiteNpmInstall(site.slug);
          delete siteInstalls[site.slug];
          if (!r.ok) return json(res, 500, { error: 'ติดตั้งไม่สำเร็จ: ' + r.msg });
        } catch (e) {
          delete siteInstalls[site.slug];
          throw e;
        }
        if (wasRunning) setTimeout(() => startSiteRuntime(site.slug).catch(() => {}), 500);
        return json(res, 200, { ok: true, deps: pkg.dependencies, added, removed });
      }
    }

    // ===== Site Settings =====
    const stg = p.match(/^\/api\/sites\/([a-z0-9][a-z0-9-]{2,29})\/settings$/);
    if (stg) {
      const site = own(hub.sites, stg[1]);
      if (!site) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
      if (site.owner !== me) return json(res, 403, { error: 'ดูได้เฉพาะเจ้าของ' });
      if (M === 'GET') {
        const tok = findTokenOfSlug(site.slug);
        const usage = siteUsage(site.slug);
        const visitor = site.visitors || { total: 0, uniq: [], daily: {} };
        return json(res, 200, {
          slug: site.slug, tier: site.tier || 'normal',
          views: site.views || 0,
          visitors: { total: visitor.total || 0, uniq: Array.isArray(visitor.uniq) ? visitor.uniq.length : 0, daily: visitor.daily || {} },
          console: site.console || [],
          token: tok ? tok.token : null,
          quota: tok ? tok.info.quota : 0,
          used: usage.bytes, fileCount: usage.count,
          runtime: siteProcs[site.slug] ? { running: true, port: siteProcs[site.slug].port, startedAt: siteProcs[site.slug].startedAt } : { running: false },
          installing: !!siteInstalls[site.slug]
        });
      }
      if (M === 'POST') {
        const d = await readBody(req);
        if (d.action === 'clear-console') { site.console = []; await saveHubNow(); return json(res, 200, { ok: true }); }
        if (d.action === 'runtime-start') { const r = await startSiteRuntime(site.slug); return json(res, r.ok ? 200 : 400, r); }
        if (d.action === 'runtime-stop') { await stopSiteRuntime(site.slug); site.desired = false; await saveHubNow(); return json(res, 200, { ok: true }); }
        if (d.action === 'runtime-restart') { await stopSiteRuntime(site.slug); const r = await startSiteRuntime(site.slug); return json(res, r.ok ? 200 : 400, r); }
        return json(res, 400, { error: 'ไม่รู้จัก action' });
      }
    }

    // ===== Admin: Site Tokens =====
    if (p === '/api/admin/site-tokens' && M === 'GET') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const list = Object.entries(hub.siteTokens).map(([t, info]) => {
        const usage = siteUsage(info.slug);
        return { token: t, slug: info.slug, quota: info.quota, used: usage.bytes, fileCount: usage.count, note: info.note || '', created: info.created };
      });
      const onlineSites = Object.values(hub.sites).filter(s => (s.tier || 'normal') === 'online').map(s => ({ slug: s.slug, title: s.title, owner: s.owner, hasToken: !!findTokenOfSlug(s.slug) }));
      return json(res, 200, { tokens: list, onlineSites });
    }
    if (p === '/api/admin/site-token/create' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const slug = String(d.slug || '').trim().toLowerCase();
      const site = own(hub.sites, slug);
      if (!site) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
      if ((site.tier || 'normal') !== 'online') return json(res, 400, { error: 'เว็บนี้ไม่ใช่โหมด online' });
      if (findTokenOfSlug(slug)) return json(res, 400, { error: 'มี token แล้ว' });
      const quotaMB = parseInt(d.quotaMB || String(SITE_QUOTA_DEFAULT_MB), 10);
      const quota = Math.max(1, Math.min(100000, quotaMB)) * 1024 * 1024;
      const token = genSiteToken();
      hub.siteTokens[token] = { slug, quota, note: String(d.note || '').slice(0, 100), created: Date.now() };
      site.token = token;
      await saveHubNow();
      return json(res, 200, { token, quota });
    }
    if (p === '/api/admin/site-token/update' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const token = String(d.token || '');
      const info = hub.siteTokens[token];
      if (!info) return json(res, 404, { error: 'ไม่เจอ token' });
      if (d.quotaMB !== undefined) {
        const quotaMB = parseInt(d.quotaMB, 10);
        if (!(quotaMB >= 1 && quotaMB <= 100000)) return json(res, 400, { error: 'โควต้าไม่ถูกต้อง' });
        info.quota = quotaMB * 1024 * 1024;
      }
      if (d.note !== undefined) info.note = String(d.note).slice(0, 100);
      await saveHubNow();
      return json(res, 200, { ok: true, quota: info.quota });
    }
    if (p === '/api/admin/site-token/delete' && M === 'POST') {
      if (!isAdmin(me)) return json(res, 403, { error: 'ไม่มีสิทธิ์' });
      const d = await readBody(req);
      const token = String(d.token || '');
      const info = hub.siteTokens[token];
      if (!info) return json(res, 404, { error: 'ไม่เจอ token' });
      delete hub.siteTokens[token];
      const site = own(hub.sites, info.slug);
      if (site) { delete site.token; await stopSiteRuntime(site.slug); }
      for (const [fid, f] of Object.entries(hub.siteFiles)) {
        if (f.token === token) await deleteSiteFile(fid);
      }
      await saveHubNow();
      return json(res, 200, { ok: true });
    }

    // ===== Stats =====
    if (p === '/api/stats' && M === 'GET') {
      const run = Object.values(bots).filter((b) => b.status === 'running');
      return json(res, 200, {
        runningUsers: new Set(run.map((b) => b.owner)).size,
        runningBots: run.length,
        users: Object.keys(state.users).length,
        posts: Object.keys(hub.posts).length,
        persistent: PERSISTENT, redis: hasRedis
      });
    }

    // ===== Friends =====
    if (p === '/api/friends' && M === 'GET') {
      const x = meUser;
      const list = (x.friends || []).map((f) => {
        const u = getUser(f);
        const cid = chatId(me, f);
        const c = hub.chats[cid];
        const last = c && c.msgs.length ? c.msgs[c.msgs.length - 1] : null;
        let text = '';
        if (last) text = last.text || (last.media ? '[' + (last.media.kind === 'image' ? 'รูป' : last.media.kind === 'video' ? 'วิดีโอ' : 'ไฟล์') + ']' : '');
        return {
          username: f, displayName: u ? u.displayName || '' : '', avatar: u ? u.avatar || '' : '',
          last: last ? { from: last.from, text: text.slice(0, 60), t: last.t } : null,
          updated: c ? c.updated : 0,
          unread: c ? Math.max(0, c.msgs.length - (c.read[me] || 0)) : 0
        };
      }).sort((a, b) => (b.updated || 0) - (a.updated || 0));
      const reqIn = (x.friendReqIn || []).map((f) => { const u = getUser(f); return { username: f, displayName: u ? u.displayName || '' : '', avatar: u ? u.avatar || '' : '' }; });
      const reqOut = (x.friendReqOut || []).map((f) => { const u = getUser(f); return { username: f, displayName: u ? u.displayName || '' : '', avatar: u ? u.avatar || '' : '' }; });
      return json(res, 200, { friends: list, requests: reqIn, sent: reqOut });
    }
    if (p === '/api/friends/search' && M === 'GET') {
      const q = String(url.searchParams.get('q') || '').trim().toLowerCase();
      if (q.length < 2) return json(res, 200, { users: [] });
      const list = Object.keys(state.users)
        .filter((u) => u !== me && (u.includes(q) || (state.users[u].displayName || '').toLowerCase().includes(q)))
        .slice(0, 20)
        .map((u) => {
          const x = state.users[u]; ensureFriendArrays(x);
          return {
            username: u, displayName: x.displayName || '', avatar: x.avatar || '',
            isFriend: (meUser.friends || []).includes(u),
            isPending: (meUser.friendReqOut || []).includes(u),
            isIncoming: (meUser.friendReqIn || []).includes(u)
          };
        });
      return json(res, 200, { users: list });
    }
    if (p === '/api/friends/add' && M === 'POST') {
      const d = await readBody(req);
      const target = String(d.username || '').trim().toLowerCase();
      if (!USERNAME_RE.test(target)) return json(res, 400, { error: 'ชื่อไม่ถูกต้อง' });
      if (target === me) return json(res, 400, { error: 'เพิ่มตัวเองไม่ได้' });
      const tu = getUser(target);
      if (!tu) return json(res, 404, { error: 'ไม่พบผู้ใช้' });
      ensureFriendArrays(tu);
      if (meUser.friends.includes(target)) return json(res, 400, { error: 'เป็นเพื่อนแล้ว' });
      if (meUser.friendReqOut.includes(target)) return json(res, 400, { error: 'ส่งแล้ว' });
      if (meUser.friendReqIn.includes(target)) {
        meUser.friendReqIn = meUser.friendReqIn.filter((u) => u !== target);
        tu.friendReqOut = tu.friendReqOut.filter((u) => u !== me);
        meUser.friends.push(target); tu.friends.push(me);
        await saveNow();
        return json(res, 200, { ok: true, accepted: true });
      }
      meUser.friendReqOut.push(target); tu.friendReqIn.push(me);
      await saveNow();
      return json(res, 200, { ok: true });
    }
    if (p === '/api/friends/accept' && M === 'POST') {
      const d = await readBody(req);
      const target = String(d.username || '').trim().toLowerCase();
      if (!meUser.friendReqIn.includes(target)) return json(res, 400, { error: 'ไม่มีคำขอ' });
      const tu = getUser(target);
      if (!tu) return json(res, 404, { error: 'ไม่พบผู้ใช้' });
      ensureFriendArrays(tu);
      meUser.friendReqIn = meUser.friendReqIn.filter((u) => u !== target);
      tu.friendReqOut = tu.friendReqOut.filter((u) => u !== me);
      if (!meUser.friends.includes(target)) meUser.friends.push(target);
      if (!tu.friends.includes(me)) tu.friends.push(me);
      await saveNow();
      return json(res, 200, { ok: true });
    }
    if (p === '/api/friends/reject' && M === 'POST') {
      const d = await readBody(req);
      const target = String(d.username || '').trim().toLowerCase();
      meUser.friendReqIn = meUser.friendReqIn.filter((u) => u !== target);
      const tu = getUser(target);
      if (tu) { ensureFriendArrays(tu); tu.friendReqOut = tu.friendReqOut.filter((u) => u !== me); }
      await saveNow();
      return json(res, 200, { ok: true });
    }
    if (p === '/api/friends/remove' && M === 'POST') {
      const d = await readBody(req);
      const target = String(d.username || '').trim().toLowerCase();
      meUser.friends = meUser.friends.filter((u) => u !== target);
      const tu = getUser(target);
      if (tu) { ensureFriendArrays(tu); tu.friends = tu.friends.filter((u) => u !== me); }
      await saveNow();
      return json(res, 200, { ok: true });
    }

    // ===== Chat =====
    if (p === '/api/chat' && M === 'POST') {
      const d = await readBody(req);
      const target = String(d.username || '').trim().toLowerCase();
      if (!meUser.friends.includes(target)) return json(res, 403, { error: 'ต้องเป็นเพื่อนกันก่อน' });
      const c = findOrCreateChat(me, target);
      saveHub();
      return json(res, 200, { id: c.id });
    }
    const chatm = p.match(/^\/api\/chat\/([a-f0-9]{8})(?:\/(messages))?$/);
    if (chatm) {
      const c = own(hub.chats, chatm[1]);
      if (!c || !c.members.includes(me)) return json(res, 404, { error: 'ไม่เจอแชท' });
      const other = c.members.find((x) => x !== me) || '';
      if (!chatm[2] && M === 'GET') {
        const since = Math.max(0, parseInt(url.searchParams.get('since') || '0', 10) || 0);
        const msgs = c.msgs.slice(since).map((m, i) => Object.assign({ i: since + i }, m));
        if ((c.read[me] || 0) !== c.msgs.length) { c.read[me] = c.msgs.length; saveHub(); }
        const oUser = getUser(other);
        return json(res, 200, { msgs, total: c.msgs.length, other, otherDisplay: oUser ? oUser.displayName || '' : '', otherAvatar: oUser ? oUser.avatar || '' : '' });
      }
      if (chatm[2] === 'messages' && M === 'POST') {
        if (msgLimited(me)) return json(res, 429, { error: 'ถี่เกินไป' });
        const d = await readBody(req);
        let text = typeof d.text === 'string' ? d.text.slice(0, MSG_MAX) : '';
        let media = null;
        if (d.mediaId) {
          const m = own(hub.media, String(d.mediaId));
          if (!m || m.owner !== me || m.scope !== 'chat' || m.chatId !== c.id) return json(res, 400, { error: 'ไฟล์ไม่ถูกต้อง' });
          media = { id: String(d.mediaId), kind: m.kind, name: m.originalName || '', size: m.size, mime: m.mime };
        }
        if (!text.trim() && !media) return json(res, 400, { error: 'ว่างเปล่า' });
        if (c.msgs.length >= 5000) return json(res, 400, { error: 'แชทเต็ม' });
        c.msgs.push({ t: Date.now(), from: me, text, media });
        c.updated = Date.now(); c.read[me] = c.msgs.length;
        saveHub();
        return json(res, 200, { ok: true, total: c.msgs.length });
      }
      return json(res, 405, { error: 'method not allowed' });
    }

    // ===== Upload =====
    if (p === '/api/upload' && M === 'POST') {
      const ct = String(req.headers['content-type'] || '').split(';')[0].trim().toLowerCase();
      const kind = kindFor(ct);
      const limit = limitForKind(kind);
      const len = parseInt(req.headers['content-length'] || '0', 10) || 0;
      if (len > limit) return json(res, 413, { error: 'ใหญ่เกิน ' + (limit / 1048576) + ' MB' }, { Connection: 'close' });
      let scope = 'cover', chatIdParam = '';
      const qChat = url.searchParams.get('chat');
      if (qChat) {
        const c = own(hub.chats, String(qChat));
        if (!c || !c.members.includes(me)) return json(res, 404, { error: 'ไม่เจอแชท' });
        scope = 'chat'; chatIdParam = c.id;
      } else if (url.searchParams.get('scope') === 'report') scope = 'report';
      else if (url.searchParams.get('scope') === 'comment') scope = 'comment';
      const used = Object.values(hub.media).filter((x) => x.owner === me).reduce((a, x) => a + x.size, 0);
      if (used + len > QUOTA) return json(res, 413, { error: 'เต็ม' }, { Connection: 'close' });
      let buf;
      try { buf = await readStreamToBuffer(req, limit); }
      catch (e) { return json(res, e.status || 400, { error: e.message }, { Connection: 'close' }); }
      const ext = extFor(ct);
      let r;
      try { r = await putMedia(buf, ext, ct); }
      catch (e) { console.error('putMedia failed:', e.message); return json(res, 500, { error: 'อัปโหลดไม่สำเร็จ: ' + e.message }); }
      hub.media[r.id] = {
        owner: me, mime: ct, ext, kind, scope, chatId: chatIdParam || undefined,
        originalName: String(decodeURIComponent(String(req.headers['x-file-name'] || ''))).slice(0, 100),
        size: r.size, upstashPath: r.upstashPath, bucketId: r.bucketId, t: Date.now()
      };
      await saveHubNow();
      console.log('📤 Uploaded ' + r.id + ' (' + kind + ', ' + buf.length + ' bytes) to bucket #' + r.bucketId);
      return json(res, 200, { id: r.id, kind });
    }

    // ===== Posts =====
    if (p === '/api/posts') {
      if (M === 'GET') {
        const list = Object.values(hub.posts).sort((a, b) => b.created - a.created).slice(0, 200);
        return json(res, 200, { posts: list.map((x) => pubPost(x, me)) });
      }
      if (M === 'POST') {
        const chk = canCreate(meUser);
        if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
        const d = await readBody(req);
        const title = String(d.title || '').trim().slice(0, 60);
        const code = typeof d.code === 'string' ? d.code : '';
        if (!title) return json(res, 400, { error: 'ใส่ชื่อโค้ด' });
        if (d.lang !== 'py' && d.lang !== 'js') return json(res, 400, { error: 'เลือกภาษา' });
        if (!code.trim()) return json(res, 400, { error: 'ใส่โค้ด' });
        if (code.length > 200000) return json(res, 400, { error: 'โค้ดยาวเกินไป' });
        let price = 0;
        if (d.paid) { price = Math.floor(Number(d.price)); if (!(price >= 1 && price <= 1000000)) return json(res, 400, { error: 'ราคา 1-1,000,000' }); }
        let cover = '';
        if (d.cover) {
          cover = String(d.cover);
          const m = own(hub.media, cover);
          if (!m || m.owner !== me || m.scope !== 'cover' || m.kind !== 'image') return json(res, 400, { error: 'รูปปกไม่ถูกต้อง' });
          if (Object.values(hub.posts).some((x) => x.cover === cover)) return json(res, 400, { error: 'รูปปกซ้ำ' });
        }
        if (Object.values(hub.posts).filter((x) => x.owner === me).length >= MAX_POSTS) return json(res, 400, { error: 'โพสต์สูงสุด ' + MAX_POSTS });
        const id = crypto.randomBytes(6).toString('hex');
        hub.posts[id] = { id, owner: me, title, lang: d.lang, cover, code, price, created: Date.now(), comments: [] };
        await saveHubNow();
        return json(res, 200, pubPost(hub.posts[id], me));
      }
    }
    const pm = p.match(/^\/api\/posts\/([a-f0-9]{12})(?:\/(use|comments))?$/);
    if (pm) {
      const post = own(hub.posts, pm[1]);
      if (!post) return json(res, 404, { error: 'ไม่เจอโพสต์' });
      const open = post.price === 0 || post.owner === me;
      if (pm[2] === 'comments') {
        if (!Array.isArray(post.comments)) post.comments = [];
        if (M === 'GET') return json(res, 200, { comments: post.comments.map(pubComment) });
        if (M === 'POST') {
          if (msgLimited(me)) return json(res, 429, { error: 'ถี่เกินไป' });
          const d = await readBody(req);
          const text = typeof d.text === 'string' ? d.text.trim().slice(0, 500) : '';
          let media = null;
          if (d.mediaId) {
            const m = own(hub.media, String(d.mediaId));
            if (!m || m.owner !== me || m.scope !== 'comment') return json(res, 400, { error: 'ไฟล์ไม่ถูกต้อง' });
            media = { id: String(d.mediaId), kind: m.kind };
          }
          if (!text && !media) return json(res, 400, { error: 'ว่างเปล่า' });
          if (post.comments.length >= 500) return json(res, 400, { error: 'คอมเมนต์เต็ม' });
          const cm = { id: crypto.randomBytes(5).toString('hex'), from: me, text, media, t: Date.now() };
          post.comments.push(cm);
          await saveHubNow();
          return json(res, 200, { ok: true, comment: pubComment(cm) });
        }
        if (M === 'DELETE') {
          const d = await readBody(req);
          const cid = String(d.id || '');
          const before = post.comments.length;
          post.comments = post.comments.filter((c) => c.id !== cid || (c.from !== me && !isAdmin(me)));
          if (post.comments.length === before) return json(res, 404, { error: 'ไม่พบคอมเมนต์' });
          await saveHubNow();
          return json(res, 200, { ok: true });
        }
        return json(res, 405, { error: 'method not allowed' });
      }
      if (!pm[2] && M === 'GET') return json(res, 200, Object.assign(pubPost(post, me), { locked: !open }, open ? { code: post.code } : {}));
      if (!pm[2] && M === 'DELETE') {
        if (post.owner !== me) return json(res, 403, { error: 'ลบได้เฉพาะของตัวเอง' });
        if (post.cover) await deleteMediaFile(post.cover);
        delete hub.posts[post.id]; await saveHubNow();
        return json(res, 200, { ok: true });
      }
      if (pm[2] === 'use' && M === 'POST') {
        const chk = canCreate(meUser);
        if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
        if (!open) return json(res, 403, { error: 'โค้ดเสียเงิน' });
        if (botCount(me) >= maxBotsFor(me)) return json(res, 400, { error: 'บอทสูงสุด ' + maxBotsFor(me) });
        const b = createBot(me, post.title, post.lang, post.code);
        await saveNow();
        return json(res, 200, pub(b));
      }
      return json(res, 405, { error: 'method not allowed' });
    }

    // ===== Sites =====
    if (p === '/api/sites') {
      if (M === 'GET') {
        const list = Object.values(hub.sites).filter((x) => {
          const o = getUser(x.owner);
          const visible = x.public && !(o && trialExpired(o));
          return visible || x.owner === me;
        }).sort((a, b) => b.updated - a.updated).slice(0, 200);
        return json(res, 200, { sites: list.map((x) => pubSite(x, me)) });
      }
      if (M === 'POST') {
        const chk = canCreate(meUser);
        if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
        const d = await readBody(req);
        const v = validateSite(d);
        if (v.error) return json(res, 400, { error: v.error });
        let slug = String(d.slug || '').trim().toLowerCase();
        if (!slug) { do { slug = crypto.randomBytes(4).toString('hex'); } while (own(hub.sites, slug)); }
        if (!SLUG_RE.test(slug)) return json(res, 400, { error: 'ลิงก์ 3-30 ตัว a-z 0-9 -' });
        if (own(hub.sites, slug)) return json(res, 409, { error: 'ลิงก์ซ้ำ' });
        const myCount = Object.values(hub.sites).filter((x) => x.owner === me).length;
        if (myCount >= maxSitesFor(me)) return json(res, 400, { error: 'เว็บสูงสุด ' + maxSitesFor(me) });
        const now = Date.now();
        const publicId = genSiteId();
        hub.sites[slug] = Object.assign({ slug, publicId, owner: me, views: 0, created: now, updated: now, visitors: { total: 0, uniq: [], daily: {} }, console: [] }, v);
        await saveHubNow();
        return json(res, 200, pubSite(hub.sites[slug], me));
      }
    }
    const stm = p.match(/^\/api\/sites\/([a-z0-9][a-z0-9-]{2,29})(?:\/(data))?$/);
    if (stm) {
      const site = own(hub.sites, stm[1]);
      if (!site || (!site.public && site.owner !== me)) return json(res, 404, { error: 'ไม่เจอเว็บนี้' });
      if (stm[2] === 'data') {
        if (site.owner !== me) return json(res, 403, { error: 'ดูได้เฉพาะเจ้าของ' });
        if (M === 'GET') {
          const s = sdata.sites[site.slug]; const items = [];
          if (s) for (const kx of Object.keys(s.shared).sort().slice(0, 300)) {
            const v = s.shared[kx].v;
            items.push({ key: kx.slice(2), size: v.length, preview: v.length > 120 ? v.slice(0, 120) + '...' : v, json: v.length > 2000 ? '' : v, truncated: v.length > 2000 });
          }
          return json(res, 200, { usage: siteDataUsage(site.slug), items });
        }
        if (M === 'PUT') {
          const d = await readBody(req);
          const out = dbExec(site.slug, '', 'shared', 'set', { key: d.key, value: d.value });
          if (out.error) return json(res, out.status || 400, { error: out.error });
          return json(res, 200, { ok: true });
        }
        if (M === 'DELETE') {
          const d = await readBody(req);
          if (d.all) { delete sdata.sites[site.slug]; writeSDataDisk(); return json(res, 200, { ok: true }); }
          const out = dbExec(site.slug, '', 'shared', 'del', { key: d.key });
          if (out.error) return json(res, out.status || 400, { error: out.error });
          return json(res, 200, { ok: true });
        }
        return json(res, 405, { error: 'method not allowed' });
      }
      if (M === 'GET') return json(res, 200, Object.assign(pubSite(site, me), { html: site.html, css: site.css, js: site.js, packageJson: site.packageJson || '', serverCode: site.serverCode || '' }));
      if (site.owner !== me) return json(res, 403, { error: 'แก้ได้เฉพาะของตัวเอง' });
      if (M === 'PUT') {
        const chk = canCreate(meUser);
        if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
        const d = await readBody(req);
        const v = validateSite(d);
        if (v.error) return json(res, 400, { error: v.error });
        const wasRunning = !!siteProcs[site.slug];
        Object.assign(site, v, { updated: Date.now() });
        if (!site.publicId) site.publicId = genSiteId();
        await saveHubNow();
        if (!site.public) closeSiteRooms(site.slug);
        if (wasRunning && site.tier === 'online') {
          stopSiteRuntime(site.slug).then(() => startSiteRuntime(site.slug)).catch(e => console.error('auto-restart:', e.message));
        }
        return json(res, 200, pubSite(site, me));
      }
      if (M === 'DELETE') {
        await stopSiteRuntime(site.slug);
        const tok = findTokenOfSlug(site.slug);
        if (tok) delete hub.siteTokens[tok.token];
        for (const [fid, f] of Object.entries(hub.siteFiles)) if (f.slug === site.slug) await deleteSiteFile(fid);
        try { fs.rmSync(siteDir(site.slug), { recursive: true, force: true }); } catch {}
        delete hub.sites[site.slug]; delete sdata.sites[site.slug];
        closeSiteRooms(site.slug);
        await saveHubNow(); writeSDataDisk();
        return json(res, 200, { ok: true });
      }
      return json(res, 405, { error: 'method not allowed' });
    }

    // ===== Bots =====
    if (p === '/api/bots') {
      if (M === 'GET') {
        const list = Object.values(bots).filter((b) => b.owner === me).sort((a, b) => (a.created || 0) - (b.created || 0));
        return json(res, 200, { bots: list.map((b) => pub(b)) });
      }
      if (M === 'POST') {
        const chk = canCreate(meUser);
        if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
        const d = await readBody(req);
        if (d.lang !== 'py' && d.lang !== 'js') return json(res, 400, { error: 'lang ต้องเป็น py หรือ js' });
        if (botCount(me) >= maxBotsFor(me)) return json(res, 400, { error: 'บอทสูงสุด ' + maxBotsFor(me) });
        const b = createBot(me, d.name, d.lang, typeof d.code === 'string' ? d.code : '');
        await saveNow();
        return json(res, 200, pub(b, true));
      }
    }
    const m = p.match(/^\/api\/bots\/([a-f0-9]{8})(?:\/(start|stop|logs|libs))?$/);
    if (!m || !bots[m[1]] || bots[m[1]].owner !== me) return json(res, 404, { error: 'ไม่เจอบอทนี้' });
    const id = m[1], b = bots[id], sub = m[2];
    if (!sub && M === 'GET') return json(res, 200, pub(b, true));
    if (!sub && M === 'PUT') {
      const chk = canCreate(meUser);
      if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
      const d = await readBody(req);
      if (typeof d.name === 'string' && d.name.trim()) b.name = d.name.trim().slice(0, 40);
      if (typeof d.code === 'string') {
        b.code = d.code;
        try { fs.writeFileSync(codeFile(b), d.code); } catch (e) { console.error('write code:', e.message); }
        fixPerm(b);
      }
      if (typeof d.token === 'string' && d.token.trim()) b.token = d.token.trim();
      if (!b.publicId) b.publicId = genBotId();
      await saveNow();
      return json(res, 200, pub(b, true));
    }
    if (!sub && M === 'DELETE') {
      if (installs[id]) return json(res, 409, { error: 'กำลังติดตั้ง' });
      await stopBot(id);
      try { fs.rmSync(path.join(BOTS_DIR, id), { recursive: true, force: true }); } catch {}
      delete bots[id]; delete logs[id];
      await saveNow();
      return json(res, 200, { ok: true });
    }
    if (sub === 'start' && M === 'POST') {
      const chk = canCreate(meUser);
      if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
      if (botExpired(b)) return json(res, 403, { error: BOT_EXPIRED_MSG, expired: true });
      await stopBot(id);
      startBot(id);
      await saveNow();
      return json(res, 200, pub(b));
    }
    if (sub === 'stop' && M === 'POST') {
      await stopBot(id);
      if (b.status === 'error') { b.status = 'stopped'; }
      await saveNow();
      return json(res, 200, pub(b));
    }
    if (sub === 'logs' && M === 'GET') return json(res, 200, { logs: logs[id] || [] });
    if (sub === 'libs' && M === 'POST') {
      const chk = canCreate(meUser);
      if (!chk.ok) return json(res, 403, { error: chk.error, trial: chk.trial, needsVerify: chk.needsVerify, needsEmail: chk.needsEmail });
      const d = await readBody(req);
      if (installs[id]) return json(res, 409, { error: 'กำลังติดตั้งอยู่' });
      if (activeInstalls >= MAX_INSTALLS) return json(res, 429, { error: 'รอสักครู่' });
      const o = {};
      let parsed = [], ignored = [];
      if (d.reinstall) o.reinstall = true;
      else if (typeof d.remove === 'string') {
        if (!(b.libs || []).includes(d.remove)) return json(res, 404, { error: 'ไม่เจอไลบรารี' });
        o.remove = d.remove;
      } else {
        const raw = typeof d.text === 'string' ? d.text : (Array.isArray(d.add) ? d.add.map(String).join(' ') : '');
        const ps = parseSpecs(b.lang, raw);
        if (ps.bad.length) return json(res, 400, { error: 'ชื่อไม่ถูกต้อง: ' + ps.bad.slice(0, 5).join(', ') });
        if (!ps.specs.length) return json(res, 400, { error: 'ไม่พบชื่อไลบรารี' });
        if (ps.specs.length > 40) return json(res, 400, { error: 'ครั้งละไม่เกิน 40' });
        if ((b.libs || []).length + ps.specs.length > 100) return json(res, 400, { error: 'มากเกิน 100' });
        o.add = ps.specs; parsed = ps.specs; ignored = ps.ignored;
      }
      libJob(id, o).catch((e) => { console.error(e); addLog(id, 'err', 'ติดตั้งผิดพลาด: ' + e.message); });
      return json(res, 200, Object.assign(pub(b), { parsed, ignored }));
    }
    return json(res, 405, { error: 'method not allowed' });
  } catch (e) {
    console.error(e);
    try { json(res, e.status || 500, { error: e.status ? e.message : 'เกิดข้อผิดพลาด' }); } catch {}
  }
});

// ============ SHUTDOWN ============
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('🛑 Shutting down — flushing to Redis...');
  flushRedis().catch(() => {});
  try { saveDisk(); } catch (e) { console.error('saveDisk:', e.message); }
  try { writeHubDisk(); } catch (e) { console.error('writeHubDisk:', e.message); }
  try { writeSDataDisk(); } catch (e) { console.error('writeSDataDisk:', e.message); }
  for (const c of Object.values(procs)) { try { c.kill('SIGTERM'); } catch {} }
  for (const i of Object.values(installs)) { try { if (i.child) i.child.kill('SIGKILL'); } catch {} }
  for (const slug of Object.keys(siteProcs)) { try { siteProcs[slug].child.kill('SIGTERM'); } catch {} }
  setTimeout(() => process.exit(0), 4000);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('uncaughtException', (e) => console.error('uncaught:', e));
process.on('unhandledRejection', (e) => console.error('unhandled:', e));

// ============ BOOT ============
(async () => {
  if (hasRedis) {
    console.log('🔌 Upstash Redis detected, loading state...');
    try {
      const [rs, rh, rd] = await Promise.all([redisGet('alexa:state'), redisGet('alexa:hub'), redisGet('alexa:sitedata')]);
      if (rs && typeof rs === 'object' && (rs.users || rs.bots)) {
        state.users = rs.users || {};
        state.sessions = rs.sessions || {};
        const nb = rs.bots || {};
        for (const k of Object.keys(state.bots)) delete state.bots[k];
        Object.assign(state.bots, nb);
        if (rs.nextUid) state.nextUid = rs.nextUid;
        if (rs.adminUi !== undefined) state.adminUi = rs.adminUi;
        console.log('✓ state loaded from Redis (' + Object.keys(state.users).length + ' users, ' + Object.keys(state.bots).length + ' bots)');
      } else console.log('• Redis ไม่มี state เก่า');
      if (rh && typeof rh === 'object') {
        Object.assign(hub, rh);
        if (!hub.siteTokens) hub.siteTokens = {};
        if (!hub.siteFiles) hub.siteFiles = {};
        console.log('✓ hub loaded');
      }
      if (rd && typeof rd === 'object' && rd.sites) { sdata.sites = rd.sites; console.log('✓ sitedata loaded'); }
    } catch (e) { console.error('Redis boot error:', e.message); }
  } else {
    console.log('⚠️  ไม่มี UPSTASH_REDIS → ใช้ไฟล์ในเครื่อง');
  }

  for (const u of Object.keys(state.users)) {
    const usr = state.users[u];
    if (usr.maxBots === undefined) usr.maxBots = DEFAULT_MAX_BOTS;
    if (usr.maxSites === undefined) usr.maxSites = DEFAULT_MAX_SITES;
    if (usr.emailVerified === undefined) usr.emailVerified = !!usr.email;
    if (usr.email === undefined) usr.email = '';
    if (usr.displayName === undefined) usr.displayName = '';
    if (usr.bio === undefined) usr.bio = '';
    if (usr.avatar === undefined) usr.avatar = '';
    if (usr.paid === undefined) usr.paid = false;
    if (usr.trialEnds === undefined) usr.trialEnds = 0;
    ensureFriendArrays(usr);
  }
  for (const b of Object.values(bots)) {
    if (!b.publicId) b.publicId = genBotId();
    if (b.expiresAt === undefined) b.expiresAt = 0;
    if (b.code === undefined) {
      try { b.code = fs.readFileSync(codeFile(b), 'utf8'); } catch { b.code = ''; }
    }
  }
  for (const x of Object.values(hub.sites)) {
    if (!x.publicId) x.publicId = genSiteId();
    if (!x.tier) x.tier = 'normal';
    if (!x.mode) x.mode = 'split';
    if (!x.visitors) x.visitors = { total: 0, uniq: [], daily: {} };
    if (!x.console) x.console = [];
  }
  for (const p of Object.values(hub.posts)) if (!Array.isArray(p.comments)) p.comments = [];
  if (state.adminUi === undefined) state.adminUi = ADMIN_UI_DEFAULT;

  let restored = 0;
  for (const b of Object.values(bots)) {
    const file = codeFile(b);
    if (!fs.existsSync(file) && b.code) {
      try {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, b.code);
        fixPerm(b);
        restored++;
      } catch (e) { console.error('restore code for ' + b.id + ':', e.message); }
    }
  }
  if (restored) console.log('↩️  Restored ' + restored + ' bot code file(s) from Redis state');

  if (blobBuckets.length) {
    const usedPerBucket = {};
    for (const m of Object.values(hub.media)) {
      if (typeof m.bucketId === 'number' && m.size) usedPerBucket[m.bucketId] = (usedPerBucket[m.bucketId] || 0) + m.size;
    }
    for (const bk of blobBuckets) bk.used = usedPerBucket[bk.id] || 0;
    console.log('📊 Blob usage: ' + blobBuckets.map(b => '#' + b.id + '=' + (b.used / 1048576).toFixed(1) + 'MB').join(' '));
  }

  snapshot();
  setInterval(snapshot, 6 * 3600 * 1000);

  setInterval(() => {
    const now = Date.now();
    for (const [k, r] of Array.from(rooms.entries())) if (!r.clients.size && r.emptySince && now - r.emptySince > 30 * 60 * 1000) rooms.delete(k);
    for (const [k, b] of Array.from(buckets.entries())) if (now - b.last > 10 * 60 * 1000) buckets.delete(k);
    for (const [k, f] of Array.from(sapiHits.entries())) if (now - f.t > 60000) sapiHits.delete(k);
    for (const [k, f] of Array.from(loginFails.entries())) if (now - f.t > 3600000) loginFails.delete(k);
    for (const [k, f] of Array.from(msgRate.entries())) if (now - f.t > 120000) msgRate.delete(k);
    for (const [k, f] of Array.from(emailSends.entries())) if (now - f.t > 3600000) emailSends.delete(k);
    for (const [k, f] of Array.from(reportRate.entries())) if (now - f.t > 3600000) reportRate.delete(k);
  }, 5 * 60 * 1000);

  setInterval(() => {
    for (const b of Object.values(bots)) {
      const o = getUser(b.owner);
      if (b.status === 'running' && o && trialExpired(o)) { addLog(b.id, 'err', 'หมดทดลอง: หยุดบอท'); stopBot(b.id).catch(() => {}); }
      else if (b.status === 'running' && botExpired(b)) { addLog(b.id, 'err', 'หมดเวลา: หยุดบอท'); stopBot(b.id).catch(() => {}); }
    }
  }, 60 * 1000);

  for (const [k, s] of Object.entries(state.sessions)) if (s.exp < Date.now()) delete state.sessions[k];
  for (const b of Object.values(bots)) fixPerm(b);

  let i = 0;
  for (const b of Object.values(bots)) {
    if (b.desired) {
      const o = getUser(b.owner);
      if (o && trialExpired(o)) { b.status = 'stopped'; b.desired = false; b.lastExit = TRIAL_MSG; continue; }
      if (botExpired(b)) { b.status = 'stopped'; b.desired = false; b.lastExit = BOT_EXPIRED_MSG; continue; }
      setTimeout(() => {
        addLog(b.id, 'sys', 'เซิร์ฟเวอร์รีสตาร์ท - เปิดบอทอัตโนมัติ');
        startBot(b.id);
      }, 800 + i++ * 1500);
    } else if (b.status === 'running') {
      b.status = 'stopped';
    }
  }

  let siteIdx = 0;
  for (const site of Object.values(hub.sites)) {
    if (site.tier === 'online' && site.desired && site.serverCode && findTokenOfSlug(site.slug)) {
      setTimeout(() => {
        sitePushConsole(site.slug, 'sys', 'เซิร์ฟเวอร์รีสตาร์ท - เปิด runtime อัตโนมัติ', 'server');
        startSiteRuntime(site.slug).catch(e => console.error('restart site runtime ' + site.slug + ':', e.message));
      }, 1500 + siteIdx++ * 2000);
    }
  }

  await saveNow();

  server.listen(PORT, '0.0.0.0', () => {
    console.log('Alexa Hub @ ' + PORT + ' | ADMIN=' + ADMIN_USER + ' | Volume=' + (PERSISTENT ? 'yes' : 'NO') +
      ' | Redis=' + (hasRedis ? 'yes' : 'no') + ' | Blobs=' + blobBuckets.length +
      ' | Trial=' + TRIAL_DAYS + 'd | Email=' + (RESEND_API_KEY ? 'resend' : 'dev-console'));
  });
})();
