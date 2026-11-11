// Hero Music Live: serve il gioco e porta like, follow e regali della tua LIVE TikTok al gioco.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { WebSocketServer } from 'ws';
import { TikTokLiveConnection } from 'tiktok-live-connector';

const PORT = process.env.PORT || 10000;
const FILES = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/index.html': ['index.html', 'text/html; charset=utf-8'],
  '/song.mp3': ['song.mp3', 'audio/mpeg'],
};

const server = http.createServer(async (req, res) => {
  const path = new URL(req.url, 'http://local').pathname;
  if (path === '/health') { res.end('ok'); return; }
  const f = FILES[path];
  if (!f) { res.writeHead(404); res.end('Not found'); return; }
  try {
    const data = await readFile(new URL('./' + f[0], import.meta.url));
    res.writeHead(200, { 'Content-Type': f[1], 'Cache-Control': 'no-cache' });
    res.end(data);
  } catch {
    res.writeHead(404); res.end('Missing file: ' + f[0]);
  }
});

const wss = new WebSocketServer({ server, path: '/live' });
let tt = null, watching = '', retryTimer = null;
let lastStatus = { event: 'status', state: 'idle', text: 'Not connected' };

function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}
function status(state, text) {
  lastStatus = { event: 'status', state, text };
  broadcast(lastStatus);
  console.log('[status]', state, text);
}
const who = d => ({
  nickname: d?.user?.nickname || d?.nickname || '',
  uniqueId: d?.user?.uniqueId || d?.uniqueId || '',
});

function stop() {
  clearTimeout(retryTimer);
  if (tt) { const c = tt; tt = null; c.disconnect().catch(() => {}); }
}
function scheduleRetry() {
  clearTimeout(retryTimer);
  if (!watching || wss.clients.size === 0) return;
  retryTimer = setTimeout(() => watch(watching, true), 20000);
}

async function watch(username, isRetry) {
  username = String(username || '').trim().replace(/^@/, '').toLowerCase();
  if (!username) { status('error', 'Type your TikTok username first.'); return; }
  if (!isRetry && tt && watching === username && lastStatus.state === 'connected') { broadcast(lastStatus); return; }
  stop();
  watching = username;
  const conn = new TikTokLiveConnection(username, {
    enableExtendedGiftInfo: true,
    processInitialData: false,
    signApiKey: process.env.EULER_API_KEY || undefined,
  });
  tt = conn;

  conn.on('like', d => broadcast({ event: 'like', data: { ...who(d), likeCount: Number(d.likeCount) || 1, totalLikeCount: Number(d.totalLikeCount) || 0 } }));
  conn.on('follow', d => broadcast({ event: 'follow', data: who(d) }));
  conn.on('share', d => broadcast({ event: 'share', data: who(d) }));
  conn.on('subscribe', d => broadcast({ event: 'subscribe', data: who(d) }));
  conn.on('gift', d => {
    const gd = d.giftDetails || {}, ext = d.extendedGiftInfo || {};
    const type = gd.giftType ?? d.giftType;
    if (type === 1 && !d.repeatEnd) return; // aspetta la fine della serie di regali
    const diamonds = Number(gd.diamondCount ?? ext.diamond_count ?? ext.diamondCount ?? d.diamondCount ?? 1) || 1;
    broadcast({ event: 'gift', data: { ...who(d), giftName: gd.giftName || ext.name || d.giftName || 'gift', diamondCount: diamonds, repeatCount: Number(d.repeatCount) || 1 } });
  });
  conn.on('streamEnd', () => { if (tt === conn) { status('offline', 'Your LIVE has ended.'); scheduleRetry(); } });
  conn.on('disconnected', () => { if (tt === conn) { status('offline', 'Connection dropped, trying again…'); scheduleRetry(); } });
  conn.on('error', e => console.log('[tiktok error]', e?.info || e?.message || e));

  if (!isRetry) status('connecting', 'Connecting to @' + username + '…');
  try {
    await conn.connect();
    if (tt !== conn) return;
    status('connected', 'Connected to @' + username + '’s LIVE');
  } catch (e) {
    if (tt !== conn) return;
    const msg = String(e?.message || e);
    const offline = e?.name === 'UserOfflineError' || /offline|not live|isn't live|is not online/i.test(msg);
    console.log('[connect failed]', msg);
    status('offline', offline ? 'Waiting for @' + username + ' to go LIVE…' : 'Can’t see @' + username + '’s LIVE yet. Checking again every 20 seconds…');
    scheduleRetry();
  }
}

wss.on('connection', ws => {
  ws.send(JSON.stringify(lastStatus));
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'watch') watch(m.username);
    else if (m.type === 'stop') { watching = ''; stop(); status('idle', 'Not connected'); }
    else if (m.type === 'ping') ws.send('{"event":"pong"}');
  });
  ws.on('close', () => {
    if (wss.clients.size === 0) setTimeout(() => {
      if (wss.clients.size === 0) { watching = ''; stop(); lastStatus = { event: 'status', state: 'idle', text: 'Not connected' }; }
    }, 120000);
  });
});

server.listen(PORT, () => console.log('Hero Music Live is running on port', PORT));
