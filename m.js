const dns = require('dns');
try { dns.setDefaultResultOrder('ipv4first'); dns.setServers(['8.8.8.8', '8.8.4.4']); } catch (e) {}

const {
  Client, GatewayIntentBits, Events, EmbedBuilder,
  ActionRowBuilder, StringSelectMenuBuilder,
  ButtonBuilder, ButtonStyle, ChannelType, MessageFlags
} = require('discord.js');
const {
  joinVoiceChannel, createAudioPlayer, createAudioResource,
  AudioPlayerStatus, VoiceConnectionStatus, entersState,
  StreamType, NoSubscriberBehavior
} = require('@discordjs/voice');
const { spawn } = require('child_process');
const fs   = require('fs');
const path = require('path');
const mongoose = require('mongoose');

// ==================== Config ====================
const TOKEN = process.env.DISCORD_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const YTDLP    = process.env.YTDLP_PATH  || 'yt-dlp';
const FFMPEG   = process.env.FFMPEG_PATH || 'ffmpeg';

if (!TOKEN)     { console.error('❌ ไม่พบ DISCORD_TOKEN'); process.exit(1); }
if (!MONGO_URI) { console.error('❌ ไม่พบ MONGO_URI');     process.exit(1); }

const TMP_DIR      = path.join(__dirname, 'tmp');
const QUEUE_LIMIT  = 50;
const META_TTL     = 60 * 60 * 1000; // 1h cache
const META_TIMEOUT = 15000;          // 15s yt-dlp timeout

if (!fs.existsSync(TMP_DIR)) fs.mkdirSync(TMP_DIR, { recursive: true });

// ==================== MongoDB ====================
const guildConfigSchema = new mongoose.Schema({
  guildId:          { type: String, required: true, unique: true },
  requestChannelId: { type: String, default: null },
});
const GuildConfig = mongoose.model('GuildConfig', guildConfigSchema);

// ==================== Logger ====================
function ts() {
  const d = new Date();
  return [d.getHours(), d.getMinutes(), d.getSeconds()]
    .map(n => String(n).padStart(2, '0')).join(':');
}
const C = {
  r:'\x1b[0m', g:'\x1b[32m', y:'\x1b[33m', b:'\x1b[34m',
  m:'\x1b[35m', c:'\x1b[36m', x:'\x1b[31m', gr:'\x1b[90m',
};
const log = {
  info:  (m, ...a) => console.log(`${C.gr}[${ts()}]${C.r} ${C.g}OK${C.r} ${m}`, ...a),
  warn:  (m, ...a) => console.log(`${C.gr}[${ts()}]${C.r} ${C.y}!!${C.r} ${m}`, ...a),
  error: (m, ...a) => console.log(`${C.gr}[${ts()}]${C.r} ${C.x}XX${C.r} ${m}`, ...a),
  music: (m, ...a) => console.log(`${C.gr}[${ts()}]${C.r} ${C.m}MS${C.r} ${m}`, ...a),
  voice: (m, ...a) => console.log(`${C.gr}[${ts()}]${C.r} ${C.b}VC${C.r} ${m}`, ...a),
};

process.on('uncaughtException',  e => { if (!e?.message?.includes('Unknown')) log.error('Uncaught:', e.message); });
process.on('unhandledRejection', e => { if (!e?.message?.includes('Unknown')) log.error('Unhandled:', e?.message || e); });

// ==================== State ====================
const queues           = {};
const requestChannels  = {};
const controlMessages  = {};
const currentEffects   = {};
const nowPlaying       = {};
const connections      = {};
const players          = {};
const currentProcs     = {};
const volumes          = {};
const loopModes        = {};
const history          = {};
const lastVoiceChannel = {};
const uiDebounce       = {};
const prefetchedMeta   = {}; // query → meta, pre-warmed

const metaCache = new Map();

// ==================== DB ====================
async function loadChannelsFromDB() {
  try {
    const docs = await GuildConfig.find({});
    docs.forEach(doc => {
      if (doc.requestChannelId) requestChannels[doc.guildId] = doc.requestChannelId;
    });
    log.info('โหลดข้อมูลห้องขอเพลงจาก MongoDB สำเร็จนะเหมียว');
  } catch (e) { log.error('โหลด DB ไม่สำเร็จ:', e.message); }
}

async function saveChannelToDB(guildId, channelId) {
  try {
    await GuildConfig.findOneAndUpdate(
      { guildId },
      { requestChannelId: channelId },
      { upsert: true, new: true },
    );
  } catch (e) { log.error('บันทึก DB ไม่สำเร็จ:', e.message); }
}

// ==================== Utils ====================
function fmtDuration(s) {
  if (!s) return '0:00';
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = Math.floor(s % 60);
  return h > 0
    ? `${h}:${String(m).padStart(2, '0')}:${String(x).padStart(2, '0')}`
    : `${m}:${String(x).padStart(2, '0')}`;
}

const EFFECTS = {
  nightcore: 'asetrate=44100*1.25,aresample=48000',
  bassboost: 'bass=g=10',
  slowmo:    'atempo=0.8',
  '8d':      'apulsator=hz=0.125',
  karaoke:   'stereotools=mlev=0.01',
  vaporwave: 'asetrate=44100*0.8,aresample=48000',
};

// ==================== Meta Fetcher (timeout + retry) ====================
async function getMeta(query, retries = 2) {
  const cached = metaCache.get(query);
  if (cached && Date.now() - cached.ts < META_TTL) return cached.data;

  // return prefetched if ready
  if (prefetchedMeta[query]) {
    const d = prefetchedMeta[query];
    delete prefetchedMeta[query];
    return d;
  }

  for (let attempt = 0; attempt <= retries; attempt++) {
    const result = await new Promise(resolve => {
      let proc;
      const timer = setTimeout(() => {
        try { proc?.kill('SIGKILL'); } catch (e) {}
        resolve(null);
      }, META_TIMEOUT);

      proc = spawn(YTDLP, [
        '--print', '%(title)s|||%(uploader)s|||%(duration)s|||%(thumbnail)s|||%(webpage_url)s',
        '--no-warnings', '--no-playlist', '--default-search', 'ytsearch1',
        '--skip-download', '--no-check-certificates', '--quiet',
        '--extractor-args', 'youtube:skip=dash,translated_subs',
        '--socket-timeout', '10',
        query,
      ], { stdio: ['ignore', 'pipe', 'pipe'] });

      let b = '';
      proc.stdout.on('data', d => { b += d; });
      proc.on('close', code => {
        clearTimeout(timer);
        if (code !== 0 || !b.trim()) return resolve(null);
        try {
          const [title, author, duration, thumbnail, url] = b.trim().split('|||');
          const res = {
            title:     title     || '?',
            author:    author    || '?',
            duration:  parseInt(duration) || 0,
            thumbnail: thumbnail || null,
            url:       url       || null,
          };
          metaCache.set(query, { data: res, ts: Date.now() });
          resolve(res);
        } catch { resolve(null); }
      });
      proc.on('error', () => { clearTimeout(timer); resolve(null); });
    });

    if (result) return result;
    if (attempt < retries) await new Promise(r => setTimeout(r, 600 * (attempt + 1)));
  }
  return null;
}

// pre-warm meta สำหรับเพลงถัดไปใน queue
function prefetchNextMeta(g) {
  const q = queues[g] || [];
  if (!q.length) return;
  const next = q[0];
  if (!next || metaCache.get(next.query)) return;
  getMeta(next.query).then(meta => {
    if (meta) prefetchedMeta[next.query] = meta;
  }).catch(() => {});
}

// ==================== Audio Stream ====================
function createStream(query, effect) {
  const yt = spawn(YTDLP, [
    '-f', 'bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best',
    '-o', '-',
    '--quiet', '--no-warnings', '--no-playlist', '--no-check-certificates',
    '--default-search', 'ytsearch1',
    '--buffer-size', '32M',
    '--http-chunk-size', '1M',
    '--concurrent-fragments', '16',
    '--no-part', '--no-mtime', '--no-continue',
    '--socket-timeout', '10',
    '--retries', '5', '--fragment-retries', '5', '--retry-sleep', '1',
    '--extractor-args', 'youtube:skip=dash,translated_subs',
    '--no-call-home',
    query,
  ], { stdio: ['ignore', 'pipe', 'pipe'] });

  const baseFilter = effect && EFFECTS[effect] ? EFFECTS[effect] : null;
  const ffArgs = [
    '-nostdin', '-i', 'pipe:0', '-vn',
    '-f', 's16le', '-ar', '48000', '-ac', '2',
    '-threads', '4',
    '-bufsize', '8M',
    '-loglevel', 'error',
    '-fflags', '+genpts+igndts+discardcorrupt',
    '-analyzeduration', '1M', '-probesize', '1M',
    '-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '3',
  ];
  if (baseFilter) ffArgs.push('-af', baseFilter);
  ffArgs.push('pipe:1');

  const ff = spawn(FFMPEG, ffArgs, { stdio: ['pipe', 'pipe', 'pipe'] });
  yt.stdout.pipe(ff.stdin);
  yt.stderr.on('data', () => {});
  ff.stderr.on('data', () => {});

  return { yt, ff, stream: ff.stdout };
}

function killProcs(g) {
  const p = currentProcs[g]; if (!p) return;
  // unpipe ก่อน destroy — ป้องกัน EPIPE
  try { p.yt?.stdout?.unpipe?.(); } catch (e) {}
  for (const s of [p.ff?.stdout, p.ff?.stdin, p.yt?.stdout]) {
    try { s?.destroy?.(); } catch (e) {}
  }
  for (const proc of [p.ff, p.yt]) {
    try { proc?.kill('SIGKILL'); } catch (e) {}
  }
  delete currentProcs[g];
}

function applyVol(g) {
  try {
    const p = players[g], v = volumes[g] ?? 1.0;
    if (p?.state?.resource?.volume) p.state.resource.volume.setVolume(v);
  } catch (e) {}
}

// ==================== Voice ====================
async function ensureVoice(guild, voiceChannel) {
  const existing = connections[guild.id];
  if (
    existing &&
    existing.joinConfig?.channelId === voiceChannel.id &&
    existing.state.status === VoiceConnectionStatus.Ready
  ) return existing;

  if (existing) { try { existing.destroy(); } catch (e) {} delete connections[guild.id]; }

  const conn = joinVoiceChannel({
    channelId:      voiceChannel.id,
    guildId:        guild.id,
    adapterCreator: guild.voiceAdapterCreator,
    selfDeaf: true, selfMute: false,
  });
  connections[guild.id] = conn;

  conn.on('stateChange', (o, n) => {
    if (n.status === VoiceConnectionStatus.Disconnected) {
      entersState(conn, VoiceConnectionStatus.Ready, 5000).catch(() => {
        try { conn.destroy(); } catch (e) {}
        delete connections[guild.id];
      });
    }
    if (n.status === VoiceConnectionStatus.Ready && players[guild.id]) {
      conn.subscribe(players[guild.id]);
    }
  });

  try {
    await entersState(conn, VoiceConnectionStatus.Ready, 20000);
    return conn;
  } catch (e) {
    try { conn.destroy(); } catch (x) {}
    delete connections[guild.id];
    throw new Error('เชื่อมต่อห้องเสียงไม่ได้นะเหมียว');
  }
}

// ==================== Player ====================
function ensurePlayer(g, client) {
  const existing = players[g];
  if (existing && existing.state?.status !== 'destroyed') return existing;

  const player = createAudioPlayer({
    behaviors: { noSubscriber: NoSubscriberBehavior.Play },
  });

  player.on(AudioPlayerStatus.Idle, () => {
    killProcs(g);
    playNext(g, client);
  });

  player.on('error', err => {
    log.error(`Player [${g}]:`, err.message);
    killProcs(g);
    setTimeout(() => playNext(g, client), 1000);
  });

  player.on(AudioPlayerStatus.Playing, () => scheduleUI(client, g));

  players[g] = player;
  if (connections[g]) connections[g].subscribe(player);
  return player;
}

// ==================== UI Debounce ====================
function scheduleUI(client, g, delay = 400) {
  if (uiDebounce[g]) clearTimeout(uiDebounce[g]);
  uiDebounce[g] = setTimeout(async () => {
    delete uiDebounce[g];
    await updateUI(client, g);
  }, delay);
}

// ==================== Playback ====================
async function playNext(g, client) {
  const q    = queues[g]    || [];
  const loop = loopModes[g] || 'off';
  const np   = nowPlaying[g];

  if (q.length === 0) {
    if (loop === 'single' && np) {
      queues[g] = [{ query: np.query, requester: np.requester, roomName: np.roomName }];
    } else if (loop === 'queue' && history[g]?.length) {
      queues[g] = history[g].slice(); history[g] = [];
    } else {
      nowPlaying[g] = null;
      await updateUI(client, g);
      setTimeout(async () => {
        if ((queues[g] || []).length === 0 && connections[g]) {
          try { connections[g].destroy(); } catch (e) {}
          delete connections[g];
          await updateUI(client, g);
        }
      }, 60000);
      return;
    }
  }

  const song = queues[g].shift();
  const vc   = lastVoiceChannel[g];
  if (!vc) { nowPlaying[g] = null; await updateUI(client, g); return; }

  killProcs(g);
  nowPlaying[g] = {
    title: 'โหลด...', author: '...', duration: '0:00',
    requester: song.requester, thumbnail: null,
    query: song.query, roomName: song.roomName,
  };
  scheduleUI(client, g, 0);

  let conn;
  try {
    const guild = client.guilds.cache.get(g);
    conn = await ensureVoice(guild, vc);
  } catch (e) {
    nowPlaying[g] = null;
    await updateUI(client, g);
    return;
  }

  // ดึง meta + เปิด stream พร้อมกัน
  const [meta, streamData] = await Promise.all([
    getMeta(song.query),
    Promise.resolve(createStream(song.query, currentEffects[g])),
  ]);

  if (meta) {
    nowPlaying[g].title     = meta.title;
    nowPlaying[g].author    = meta.author;
    nowPlaying[g].duration  = fmtDuration(meta.duration);
    nowPlaying[g].thumbnail = meta.thumbnail;
  }

  if (!history[g]) history[g] = [];
  history[g].push({ query: song.query, requester: song.requester, roomName: song.roomName });
  if (history[g].length > 20) history[g].shift();

  const { yt, ff, stream } = streamData;
  currentProcs[g] = { yt, ff };

  const player = ensurePlayer(g, client);
  conn.subscribe(player);

  try {
    const resource = createAudioResource(stream, { inputType: StreamType.Raw, inlineVolume: true });
    if (resource.volume) resource.volume.setVolume(volumes[g] ?? 1.0);
    player.play(resource);

    setTimeout(() => applyVol(g), 300);
    scheduleUI(client, g, 800);

    // pre-warm เพลงถัดไปใน background
    setTimeout(() => prefetchNextMeta(g), 3000);
  } catch (e) {
    log.error(`playNext error [${g}]:`, e.message);
    killProcs(g);
    setTimeout(() => playNext(g, client), 1000);
  }
}

async function enqueue(guild, voiceChannel, query, requesterStr, channel) {
  if (!queues[guild.id]) queues[guild.id] = [];
  if (queues[guild.id].length >= QUEUE_LIMIT) {
    const m = await channel.send('❌ คิวเต็มแล้วนะเหมียว');
    setTimeout(() => m.delete().catch(() => {}), 5000);
    return false;
  }

  lastVoiceChannel[guild.id] = voiceChannel;
  try { await ensureVoice(guild, voiceChannel); } catch (e) {
    const m = await channel.send('❌ ' + e.message);
    setTimeout(() => m.delete().catch(() => {}), 5000);
    return false;
  }

  const player = ensurePlayer(guild.id, guild.client);
  if (connections[guild.id]) connections[guild.id].subscribe(player);

  // prefetch meta ทันทีตอน queue
  getMeta(query).catch(() => {});

  queues[guild.id].push({ query, requester: requesterStr, roomName: voiceChannel.name });
  const isPlaying = players[guild.id]?.state?.status !== AudioPlayerStatus.Idle;

  if (isPlaying) {
    const m = await channel.send(`➕ **${query}** (คิวที่ ${queues[guild.id].length}) นะเหมียว`);
    setTimeout(() => m.delete().catch(() => {}), 10000);
    scheduleUI(guild.client, guild.id);
  } else {
    await playNext(guild.id, guild.client);
  }
  return true;
}

// ==================== UI ====================
function makeRows() {
  const sel = new StringSelectMenuBuilder()
    .setCustomId('effect_select')
    .setPlaceholder('เลือกเอฟเฟกต์เสียงนะเหมียว')
    .addOptions([
      { label: 'NORMAL',    emoji: '🎶', value: 'normal'    },
      { label: 'NIGHTCORE', emoji: '🌙', value: 'nightcore' },
      { label: 'BASSBOOST', emoji: '🔊', value: 'bassboost' },
      { label: 'SLOWMO',    emoji: '🐢', value: 'slowmo'    },
      { label: '8D',        emoji: '🎧', value: '8d'        },
      { label: 'KARAOKE',   emoji: '🎤', value: 'karaoke'   },
      { label: 'VAPORWAVE', emoji: '🌊', value: 'vaporwave' },
    ]);

  return [
    new ActionRowBuilder().addComponents(sel),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('pause').setEmoji('⏸️').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('skip').setEmoji('⏭️').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('stop').setEmoji('⏹️').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('loop').setEmoji('🔁').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('shuffle').setEmoji('🔀').setStyle(ButtonStyle.Secondary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('vol_down').setEmoji('🔉').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('vol_up').setEmoji('🔊').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('vol_mute').setEmoji('🔇').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('queue').setLabel('คิว').setEmoji('📋').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('np').setEmoji('🎵').setStyle(ButtonStyle.Secondary),
    ),
  ];
}

async function updateUI(client, g) {
  const cid = requestChannels[g]; if (!cid) return;
  const ch  = client.channels.cache.get(cid); if (!ch) return;

  const playing = nowPlaying[g] && players[g] &&
    [AudioPlayerStatus.Playing, AudioPlayerStatus.Paused].includes(players[g]?.state?.status);
  const q     = queues[g] || [];
  const np    = nowPlaying[g];
  const guild = client.guilds.cache.get(g);
  const vc    = guild?.members?.me?.voice?.channel;

  let emb, rows;
  if (playing && np) {
    const lp  = loopModes[g] || 'off';
    const vol = Math.round((volumes[g] ?? 1.0) * 100);
    emb = new EmbedBuilder()
      .setTitle('🎵 ' + np.title)
      .setColor(0x1DB954)
      .addFields(
        { name: '✨ เจ้าของเพลง', value: '`' + (np.author   || '?')     + '`', inline: true },
        { name: '⏱️ ความยาว',    value: '`' + (np.duration || '0:00') + '`', inline: true },
        { name: '🎵 คิว',         value: '`' + q.length + '` เพลง',           inline: true },
        { name: '👤 ขอโดย',       value: np.requester,                         inline: true },
        { name: '🔊 ห้องเสียง',   value: vc ? vc.name : '-',                  inline: true },
        { name: '🔁 Loop',        value: '`' + lp + '`',                       inline: true },
        { name: '🔊 Volume',      value: '`' + vol + '%`',                     inline: true },
        { name: '✨ Effect',      value: '`' + (currentEffects[g] || 'NORMAL').toUpperCase() + '`', inline: true },
      );
    if (q.length > 0) {
      const nx = q.slice(0, 5).map((s, i) =>
        `\`${i + 1}.\` ${s.query.length > 40 ? s.query.slice(0, 40) + '...' : s.query}`
      ).join('\n');
      emb.addFields({ name: '📋 คิวถัดไป', value: nx + (q.length > 5 ? `\n_...อีก ${q.length - 5}_` : ''), inline: false });
    }
    if (np.thumbnail) emb.setImage(np.thumbnail);
    rows = makeRows();
  } else {
    emb  = new EmbedBuilder()
      .setTitle('🎶 Music Bot')
      .setDescription('พิมพ์ชื่อเพลงหรือลิงก์ YouTube ในช่องนี้ หรือใช้ `/play` นะเหมียว ✨')
      .setColor(0x1DB954);
    rows = [];
  }

  try {
    if (controlMessages[g]) {
      await controlMessages[g].edit({ embeds: [emb], components: rows });
    } else {
      controlMessages[g] = await ch.send({ embeds: [emb], components: rows });
    }
  } catch (e) {
    try { controlMessages[g] = await ch.send({ embeds: [emb], components: rows }); } catch (x) {}
  }
}

// ==================== Discord Client ====================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildVoiceStates,
  ],
});

client.once(Events.ClientReady, async c => {
  log.info('บอทออนไลน์: ' + c.user.tag);

  try {
    await mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 10000 });
    log.info('MongoDB เชื่อมต่อสำเร็จ');
    await loadChannelsFromDB();
  } catch (e) { log.error('MongoDB Error:', e.message); }

  try {
    await c.application.commands.set([
      { name: 'setup', description: 'สร้างห้องขอเพลงอัตโนมัตินะเหมียว' },
      {
        name: 'play',
        description: 'เล่นเพลงจากชื่อหรือลิงก์ YouTube นะเหมียว',
        options: [{ name: 'query', description: 'ชื่อเพลงหรือลิงก์นะเหมียว', type: 3, required: true }],
      },
      { name: 'join',   description: 'เข้าห้องเสียงนะเหมียว' },
      { name: 'leave',  description: 'ออกจากห้องเสียงนะเหมียว' },
      { name: 'skip',   description: 'ข้ามเพลงนะเหมียว' },
      { name: 'queue',  description: 'ดูคิวนะเหมียว' },
      { name: 'clear',  description: 'ล้างคิวนะเหมียว' },
      { name: 'pause',  description: 'หยุด/เล่นต่อนะเหมียว' },
      {
        name: 'volume',
        description: 'ตั้ง volume (0–200) นะเหมียว',
        options: [{
          name: 'percent', description: 'เปอร์เซ็นต์ (0–200)',
          type: 4, required: true, min_value: 0, max_value: 200,
        }],
      },
    ]);
    log.info('Sync commands สำเร็จ');
  } catch (e) { log.error('Sync:', e.message); }

  // ล้าง tmp เก่า
  try {
    fs.readdirSync(TMP_DIR).forEach(f => {
      try { fs.unlinkSync(path.join(TMP_DIR, f)); } catch (e) {}
    });
  } catch (e) {}
});

// ==================== Message Handler ====================
client.on(Events.MessageCreate, async msg => {
  if (msg.author.bot || !msg.guild) return;
  const g = msg.guild.id;
  if (!requestChannels[g] || msg.channel.id !== requestChannels[g]) return;

  const query = msg.content.trim(); if (!query) return;
  try { await msg.delete(); } catch (e) {}

  const vc = msg.member?.voice?.channel;
  if (!vc) {
    const m = await msg.channel.send('❌ ' + msg.author + ' เข้าห้องเสียงก่อนนะเหมียว!');
    setTimeout(() => m.delete().catch(() => {}), 5000);
    return;
  }
  await enqueue(msg.guild, vc, query, msg.author.toString(), msg.channel);
});

// ==================== Interaction Handler ====================
client.on(Events.InteractionCreate, async it => {
  if (!it.guild) return;
  const g     = it.guild.id;
  const flags = MessageFlags.Ephemeral;

  // ─── Slash Commands ───────────────────────────────────────
  if (it.isChatInputCommand()) {
    const cmd = it.commandName;

    if (cmd === 'setup') {
      try { await it.deferReply({ flags }); } catch (e) { return; }
      let ch = it.guild.channels.cache.find(c => c.name === '🎵・ห้องขอเพลง');
      if (ch) {
        requestChannels[g] = ch.id;
        await saveChannelToDB(g, ch.id);
        return it.editReply({ content: '📌 มีอยู่แล้วนะเหมียว: ' + ch });
      }
      try {
        ch = await it.guild.channels.create({
          name: '🎵・ห้องขอเพลง', type: ChannelType.GuildText,
          topic: 'พิมพ์ชื่อเพลงได้เลยนะเหมียว',
        });
        requestChannels[g] = ch.id;
        await saveChannelToDB(g, ch.id);
        const emb = new EmbedBuilder()
          .setTitle('🎶 Music Bot')
          .setDescription('พิมพ์ชื่อเพลงหรือลิงก์ YouTube หรือใช้ `/play` นะเหมียว ✨')
          .setColor(0x1DB954);
        controlMessages[g] = await ch.send({ embeds: [emb] });
        return it.editReply({ content: '✅ สร้างห้องให้แล้วนะเหมียว: ' + ch });
      } catch (e) { return it.editReply({ content: '❌ ' + e.message }); }
    }

    if (cmd === 'play') {
      const query = it.options.getString('query');
      const vc    = it.member?.voice?.channel;
      if (!vc) return it.reply({ content: '❌ เข้าห้องเสียงก่อนนะเหมียว!', flags }).catch(() => {});

      try { await it.deferReply({ flags }); } catch (e) { return; }
      lastVoiceChannel[g] = vc;

      let conn;
      try { conn = await ensureVoice(it.guild, vc); } catch (e) {
        return it.editReply({ content: '❌ ' + e.message }).catch(() => {});
      }

      const player = ensurePlayer(g, client);
      conn.subscribe(player);

      if (!queues[g]) queues[g] = [];
      if (queues[g].length >= QUEUE_LIMIT)
        return it.editReply({ content: '❌ คิวเต็มแล้วนะเหมียว' }).catch(() => {});

      getMeta(query).catch(() => {}); // prefetch
      queues[g].push({ query, requester: it.user.toString(), roomName: vc.name });
      const isPlaying = players[g]?.state?.status !== AudioPlayerStatus.Idle;

      if (isPlaying) {
        await it.editReply({ content: `➕ **${query}** (คิวที่ ${queues[g].length}) นะเหมียว` }).catch(() => {});
        scheduleUI(client, g);
      } else {
        await it.editReply({ content: `▶️ กำลังโหลด: **${query}** นะเหมียว` }).catch(() => {});
        await playNext(g, client);
      }
      return;
    }

    if (cmd === 'join') {
      const vc = it.member?.voice?.channel;
      if (!vc) return it.reply({ content: '❌ เข้าห้องเสียงก่อนนะเหมียว!', flags }).catch(() => {});
      try {
        lastVoiceChannel[g] = vc;
        await ensureVoice(it.guild, vc);
        ensurePlayer(g, client);
        return it.reply({ content: '📥 เข้าห้อง ' + vc.name + ' แล้วนะเหมียว', flags }).catch(() => {});
      } catch (e) { return it.reply({ content: '❌ ' + e.message, flags }).catch(() => {}); }
    }

    if (cmd === 'leave') {
      killProcs(g);
      queues[g] = []; nowPlaying[g] = null;
      try { players[g]?.stop(); } catch (e) {} delete players[g];
      try { connections[g]?.destroy(); } catch (e) {} delete connections[g];
      delete lastVoiceChannel[g];
      await updateUI(client, g);
      return it.reply({ content: '👋 ออกแล้วนะเหมียว', flags }).catch(() => {});
    }

    if (cmd === 'pause') {
      const p = players[g];
      if (!p) return it.reply({ content: '❌ ไม่มีเพลงอยู่นะเหมียว', flags }).catch(() => {});
      if (p.state.status === AudioPlayerStatus.Playing) {
        p.pause(); return it.reply({ content: '⏸️ พักก่อนนะเหมียว', flags }).catch(() => {});
      }
      if (p.state.status === AudioPlayerStatus.Paused) {
        p.unpause(); return it.reply({ content: '▶️ เล่นต่อนะเหมียว', flags }).catch(() => {});
      }
      return it.reply({ content: '❌ ทำไม่ได้นะเหมียว', flags }).catch(() => {});
    }

    if (cmd === 'skip') {
      if (!players[g]) return it.reply({ content: '❌ ไม่มีเพลงอยู่นะเหมียว', flags }).catch(() => {});
      killProcs(g);
      try { players[g].stop(); } catch (e) {}
      return it.reply({ content: '⏭️ ข้ามเพลงแล้วนะเหมียว', flags }).catch(() => {});
    }

    if (cmd === 'queue') {
      const q = queues[g] || [], np = nowPlaying[g];
      const emb = new EmbedBuilder().setTitle('📋 คิวนะเหมียว').setColor(0x1DB954);
      if (np) emb.addFields({ name: '▶️ กำลังเล่น', value: '**' + np.title + '**\n┗ ' + np.requester });
      if (!q.length) emb.addFields({ name: 'ต่อไป', value: '*(ว่างเปล่านะเหมียว)*' });
      else q.slice(0, 10).forEach((s, i) => emb.addFields({ name: `${i + 1}. ${s.query}`, value: '┗ ' + s.requester }));
      return it.reply({ embeds: [emb] }).catch(() => {});
    }

    if (cmd === 'clear') {
      queues[g] = [];
      await updateUI(client, g);
      return it.reply({ content: '🗑️ ล้างคิวให้แล้วนะเหมียว', flags }).catch(() => {});
    }

    if (cmd === 'volume') {
      const pct = it.options.getInteger('percent');
      volumes[g] = pct / 100;
      applyVol(g);
      await updateUI(client, g);
      return it.reply({ content: `🔊 ตั้ง Volume เป็น **${pct}%** แล้วนะเหมียว`, flags }).catch(() => {});
    }
  }

  // ─── Buttons ─────────────────────────────────────────────
  if (it.isButton()) {
    const id = it.customId;
    try { await it.deferReply({ flags }); } catch (e) { return; }

    if (id === 'stop') {
      queues[g] = []; killProcs(g);
      try { players[g]?.stop(); } catch (e) {} delete players[g];
      try { connections[g]?.destroy(); } catch (e) {} delete connections[g];
      nowPlaying[g] = null; delete lastVoiceChannel[g];
      await updateUI(client, g);
      return it.editReply({ content: '⏹️ หยุดเล่นแล้วนะเหมียว' }).catch(() => {});
    }
    if (id === 'pause') {
      const p = players[g];
      if (!p) return it.editReply({ content: '❌ ไม่มีเพลงอยู่' }).catch(() => {});
      if (p.state.status === AudioPlayerStatus.Playing) { p.pause();   return it.editReply({ content: '⏸️ พักก่อนนะเหมียว' }).catch(() => {}); }
      if (p.state.status === AudioPlayerStatus.Paused)  { p.unpause(); return it.editReply({ content: '▶️ เล่นต่อนะเหมียว' }).catch(() => {}); }
      return it.editReply({ content: '❌ ทำไม่ได้' }).catch(() => {});
    }
    if (id === 'skip') {
      if (!players[g]) return it.editReply({ content: '❌ ไม่มีเพลงอยู่' }).catch(() => {});
      killProcs(g);
      try { players[g].stop(); } catch (e) {}
      return it.editReply({ content: '⏭️ ข้ามเพลงแล้วนะเหมียว' }).catch(() => {});
    }
    if (id === 'shuffle') {
      const q = queues[g] || [];
      if (q.length > 1) {
        for (let i = q.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [q[i], q[j]] = [q[j], q[i]];
        }
        scheduleUI(client, g);
        return it.editReply({ content: '🔀 สลับคิวให้แล้วนะเหมียว' }).catch(() => {});
      }
      return it.editReply({ content: '❌ สลับไม่ได้นะเหมียว' }).catch(() => {});
    }
    if (id === 'loop') {
      const c = loopModes[g] || 'off';
      loopModes[g] = c === 'off' ? 'single' : (c === 'single' ? 'queue' : 'off');
      scheduleUI(client, g);
      return it.editReply({ content: '🔁 Loop: **' + loopModes[g] + '**' }).catch(() => {});
    }
    if (id === 'vol_up') {
      volumes[g] = Math.min(2.0, (volumes[g] ?? 1.0) + 0.1); applyVol(g); scheduleUI(client, g);
      return it.editReply({ content: '🔊 Volume: ' + Math.round(volumes[g] * 100) + '%' }).catch(() => {});
    }
    if (id === 'vol_down') {
      volumes[g] = Math.max(0, (volumes[g] ?? 1.0) - 0.1); applyVol(g); scheduleUI(client, g);
      return it.editReply({ content: '🔉 Volume: ' + Math.round(volumes[g] * 100) + '%' }).catch(() => {});
    }
    if (id === 'vol_mute') {
      const c = volumes[g] ?? 1.0;
      volumes[g] = c > 0 ? 0 : 1.0; applyVol(g); scheduleUI(client, g);
      return it.editReply({ content: volumes[g] === 0 ? '🔇 ปิดเสียง' : '🔊 เปิดเสียง' }).catch(() => {});
    }
    if (id === 'queue') {
      const q = queues[g] || [], np = nowPlaying[g];
      const emb = new EmbedBuilder().setTitle('📋 คิวนะเหมียว').setColor(0x1DB954);
      if (np) emb.addFields({ name: '▶️ กำลังเล่น', value: '**' + np.title + '**' });
      if (!q.length) emb.addFields({ name: 'ต่อไป', value: '*(ว่างเปล่า)*' });
      else q.slice(0, 10).forEach((s, i) => emb.addFields({ name: `${i + 1}. ${s.query}`, value: '┗ ' + s.requester }));
      return it.editReply({ embeds: [emb] }).catch(() => {});
    }
    if (id === 'np') {
      const np = nowPlaying[g];
      if (!np) return it.editReply({ content: '❌ ไม่มีเพลงที่เล่นอยู่' }).catch(() => {});
      const emb = new EmbedBuilder()
        .setTitle('🎵 กำลังเล่นอยู่นะเหมียว')
        .setColor(0x1DB954)
        .addFields(
          { name: 'ชื่อ',    value: np.title    },
          { name: 'ผู้แต่ง', value: np.author   },
          { name: 'ความยาว', value: np.duration  },
          { name: 'ขอโดย',   value: np.requester },
        );
      if (np.thumbnail) emb.setImage(np.thumbnail);
      return it.editReply({ embeds: [emb] }).catch(() => {});
    }
  }

  // ─── Select Menu ─────────────────────────────────────────
  if (it.isStringSelectMenu() && it.customId === 'effect_select') {
    const c = it.values[0];
    currentEffects[g] = c === 'normal' ? null : c;
    try { await it.deferReply({ flags }); } catch (e) { return; }
    scheduleUI(client, g);
    return it.editReply({ content: '✨ Effect: ' + c.toUpperCase() }).catch(() => {});
  }
});

client.login(TOKEN);
