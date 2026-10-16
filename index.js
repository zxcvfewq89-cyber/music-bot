const dns = require('dns');
try { dns.setDefaultResultOrder('ipv4first'); dns.setServers(['8.8.8.8','8.8.4.4']); } catch(e){}

const { Client, GatewayIntentBits, Events, EmbedBuilder, ActionRowBuilder, StringSelectMenuBuilder, ButtonBuilder, ButtonStyle, ChannelType, MessageFlags } = require('discord.js');
const { joinVoiceChannel, createAudioPlayer, createAudioResource, AudioPlayerStatus, VoiceConnectionStatus, entersState, StreamType, NoSubscriberBehavior } = require('@discordjs/voice');
const { spawn, execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) { console.error('ไม่พบ DISCORD_TOKEN'); process.exit(1); }

const CONFIG_FILE = path.join(__dirname, 'request_channels.json');
const QUEUE_LIMIT = 50;

// ==================== Logger ====================
function ts() { const d=new Date(); return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0')+':'+String(d.getSeconds()).padStart(2,'0'); }
const C={r:'\x1b[0m',g:'\x1b[32m',y:'\x1b[33m',b:'\x1b[34m',m:'\x1b[35m',c:'\x1b[36m',x:'\x1b[31m',gr:'\x1b[90m'};
const log={
  info:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.g}OK${C.r} ${m}`,...a),
  warn:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.y}!!${C.r} ${m}`,...a),
  error:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.x}XX${C.r} ${m}`,...a),
  music:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.m}MS${C.r} ${m}`,...a),
  voice:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.b}VC${C.r} ${m}`,...a),
  cmd:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.y}>>${C.r} ${m}`,...a),
  perf:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.c}P:${C.r} ${m}`,...a),
  debug:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.gr}D:${C.r} ${m}`,...a),
};

process.on('uncaughtException', (e) => { if (e?.message?.includes('Unknown')) return; log.error('Uncaught:', e.message); });
process.on('unhandledRejection', (e) => { if (e?.message?.includes('Unknown')) return; log.error('Unhandled:', e?.message||e); });

// ==================== Auto Install ====================
function autoInstall() {
  const binDir = path.join(__dirname, 'bin');
  if (!fs.existsSync(binDir)) fs.mkdirSync(binDir, { recursive: true });
  
  const ytdlpPath = path.join(binDir, 'yt-dlp');
  if (!fs.existsSync(ytdlpPath)) {
    log.warn('ไม่พบ yt-dlp — กำลังติดตั้ง...');
    try {
      execSync(`curl -sL https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o "${ytdlpPath}" && chmod +x "${ytdlpPath}"`, { stdio: 'inherit', timeout: 120000 });
      log.info('ติดตั้ง yt-dlp สำเร็จ');
    } catch (e) { log.error('ติดตั้ง yt-dlp ไม่ได้: ' + e.message); }
  } else { log.info('พบ yt-dlp'); }
  
  let ffmpegBin = 'ffmpeg';
  try {
    const ffmpegStatic = require('ffmpeg-static');
    if (ffmpegStatic && fs.existsSync(ffmpegStatic)) {
      ffmpegBin = ffmpegStatic;
      log.info('ใช้ ffmpeg-static');
    }
  } catch (e) { log.warn('ไม่พบ ffmpeg-static'); }
  
  process.env.PATH = `${binDir}:${process.env.PATH}`;
  process.env.FFMPEG_PATH = ffmpegBin;
  
  return { ytdlpPath, ffmpegBin };
}

let YTDLP_PATH = 'yt-dlp';
let FFMPEG_PATH = 'ffmpeg';

// ==================== Storage ====================
const queues={},controlMessages={},currentEffects={},nowPlaying={},connections={},players={},currentProcs={},volumes={},loopModes={},history={},metaCache=new Map();
let requestChannels = {};

function loadChannels(){
  if(!fs.existsSync(CONFIG_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(CONFIG_FILE,'utf-8')); } 
  catch(e){ return {}; }
}

function saveChannels(){
  try { fs.writeFileSync(CONFIG_FILE, JSON.stringify(requestChannels, null, 2)); } 
  catch(e){}
}

function fmtDuration(s){ if(!s)return'0:00'; const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),x=Math.floor(s%60); return h>0?`${h}:${String(m).padStart(2,'0')}:${String(x).padStart(2,'0')}`:`${m}:${String(x).padStart(2,'0')}` }

const EFFECTS={nightcore:'asetrate=44100*1.25,aresample=48000',bassboost:'bass=g=10',slowmo:'atempo=0.8','8d':'apulsator=hz=0.125',karaoke:'stereotools=mlev=0.01',vaporwave:'asetrate=44100*0.8,aresample=48000'};

// ==================== Meta ====================
async function getMeta(query){
  if(metaCache.has(query)) return metaCache.get(query);
  return new Promise(r=>{
    const p=spawn(YTDLP_PATH,['--dump-single-json','--no-warnings','--no-playlist','--no-check-certificates','--default-search','ytsearch1','--quiet',query],{stdio:['ignore','pipe','pipe']});
    let b=''; p.stdout.on('data',d=>b+=d);
    p.on('close',()=>{
      try{ const d=JSON.parse(b); const i=d.entries?d.entries[0]:d;
        const res={title:i.title||'?',author:i.uploader||'?',duration:i.duration||0,thumbnail:i.thumbnail||null};
        metaCache.set(query,res); if(metaCache.size>100)metaCache.delete(metaCache.keys().next().value);
        r(res);
      }catch(err){ r(null); }
    });
    p.on('error',()=>r(null));
  });
}

// ==================== Stream ====================
function createStream(query, effect) {
  const yt = spawn(YTDLP_PATH, [
    '-f', 'bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best',
    '-o', '-', '--quiet', '--no-warnings', '--no-playlist',
    '--no-check-certificates', '--prefer-free-formats',
    '--default-search', 'ytsearch1', '--buffer-size', '5M',
    '--http-chunk-size', '10M', '--no-part', '--no-mtime',
    '--no-continue', '--socket-timeout', '30', '--retries', '10',
    '--fragment-retries', '10', '--retry-sleep', '1', '--no-call-home',
    '--extractor-args', 'youtube:player_client=android,web', query
  ], { stdio: ['ignore', 'pipe', 'pipe'] });

  const ffArgs = [
    '-nostdin', '-i', 'pipe:0', '-vn', '-f', 's16le', '-ar', '48000',
    '-ac', '2', '-loglevel', 'error', '-fflags', '+genpts+igndts',
    '-analyzeduration', '10M', '-probesize', '10M',
  ];
  if (effect && EFFECTS[effect]) ffArgs.push('-filter:a', EFFECTS[effect]);
  ffArgs.push('pipe:1');

  const ff = spawn(FFMPEG_PATH, ffArgs, { stdio: ['pipe', 'pipe', 'pipe'] });
  yt.stderr.on('data', d => { const s=d.toString().trim(); if(s) log.warn('yt: '+s.slice(0,120)); });
  ff.stderr.on('data', d => { const s=d.toString().trim(); if(s) log.warn('ff: '+s.slice(0,120)); });
  yt.stdout.pipe(ff.stdin);
  yt.on('error', e => log.error('yt spawn:', e.message));
  ff.on('error', e => log.error('ff spawn:', e.message));
  return { yt, ff, stream: ff.stdout };
}

// ==================== Kill Procs ====================
function killProcs(g){
  const p=currentProcs[g]; if(!p)return;
  try{p.yt?.stdout?.destroy()}catch(e){}
  try{p.yt?.kill('SIGKILL')}catch(e){}
  try{p.ff?.stdin?.destroy()}catch(e){}
  try{p.ff?.stdout?.destroy()}catch(e){}
  try{p.ff?.kill('SIGKILL')}catch(e){}
  delete currentProcs[g];
}

function applyVol(g){
  const p=players[g], v=volumes[g]??1.0;
  try{ if(p?.state?.resource?.volume) p.state.resource.volume.setVolume(v); }catch(e){}
}

// ==================== Voice ====================
async function ensureVoice(guild,voiceChannel){
  let conn=connections[guild.id];
  if(conn&&conn.joinConfig.channelId===voiceChannel.id&&conn.state.status===VoiceConnectionStatus.Ready) return conn;
  if(conn){ try{conn.destroy()}catch(e){} delete connections[guild.id]; }
  log.voice(`เข้าห้อง: ${voiceChannel.name}`);
  conn=joinVoiceChannel({channelId:voiceChannel.id,guildId:guild.id,adapterCreator:guild.voiceAdapterCreator,selfDeaf:true,selfMute:false});
  connections[guild.id]=conn;
  conn.on('stateChange',(o,n)=>{ if(o.status!==n.status) log.voice(`${o.status} -> ${n.status}`); });
  conn.on('error',e=>log.error('Voice:',e.message));
  try{ await entersState(conn,VoiceConnectionStatus.Ready,20000); return conn; }
  catch(e){ try{conn.destroy()}catch(x){} delete connections[guild.id]; throw new Error('เชื่อมต่อห้องเสียงไม่ได้'); }
}

// ==================== Play ====================
async function playNext(g,client){
  const q=queues[g]||[], loop=loopModes[g]||'off', np=nowPlaying[g];
  if(q.length===0){
    if(loop==='single'&&np) queues[g].push({query:np.query,requester:np.requester,roomName:np.roomName});
    else if(loop==='queue'&&history[g]?.length){ queues[g]=history[g].slice(); history[g]=[]; }
    else{
      nowPlaying[g]=null; await updateUI(client,g);
      setTimeout(async()=>{ if((queues[g]||[]).length===0&&connections[g]){ try{connections[g].destroy()}catch(e){} delete connections[g]; await updateUI(client,g); }},60000);
      return;
    }
  }
  const song=queues[g].shift(); killProcs(g);
  nowPlaying[g]={title:'โหลด...',author:'...',duration:'0:00',requester:song.requester,thumbnail:null,query:song.query,roomName:song.roomName};
  await updateUI(client,g);
  const meta=await getMeta(song.query);
  if(meta){ nowPlaying[g].title=meta.title; nowPlaying[g].author=meta.author; nowPlaying[g].duration=fmtDuration(meta.duration); nowPlaying[g].thumbnail=meta.thumbnail; }
  if(!history[g])history[g]=[];
  history[g].push({query:song.query,requester:song.requester,roomName:song.roomName});
  if(history[g].length>20)history[g].shift();
  
  const { yt, ff, stream } = createStream(song.query, currentEffects[g]);
  currentProcs[g] = { yt, ff };
  
  if(!players[g]){
    players[g]=createAudioPlayer({behaviors:{noSubscriber:NoSubscriberBehavior.Play}});
    players[g].on(AudioPlayerStatus.Idle,()=>{ killProcs(g); playNext(g,client); });
    players[g].on('error',e=>{ log.error('Player:',e.message); killProcs(g); playNext(g,client); });
  }
  if(connections[g]) connections[g].subscribe(players[g]);
  try{
    const res=createAudioResource(stream,{inputType:StreamType.Raw,inlineVolume:true});
    if(res.volume) res.volume.setVolume(volumes[g]??1.0);
    players[g].play(res);
    setTimeout(()=>{ applyVol(g); updateUI(client,g); },1500);
  }catch(e){ log.error('Play:',e.message); killProcs(g); playNext(g,client); }
}

// ==================== UI ====================
function makeRows(){
  const sel=new StringSelectMenuBuilder().setCustomId('effect_select').setPlaceholder('เลือกเอฟเฟกต์เสียง')
    .addOptions([{label:'NORMAL',emoji:'🎶',value:'normal'},{label:'NIGHTCORE',emoji:'🌙',value:'nightcore'},{label:'BASSBOOST',emoji:'🔊',value:'bassboost'},{label:'SLOWMO',emoji:'🐢',value:'slowmo'},{label:'8D',emoji:'🎧',value:'8d'},{label:'KARAOKE',emoji:'🎤',value:'karaoke'},{label:'VAPORWAVE',emoji:'🌊',value:'vaporwave'}]);
  const r1=new ActionRowBuilder().addComponents(sel);
  const r2=new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('pause').setEmoji('⏸️').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('skip').setEmoji('⏭️').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('stop').setEmoji('⏹️').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('loop').setEmoji('🔁').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('shuffle').setEmoji('🔀').setStyle(ButtonStyle.Secondary));
  const r3=new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('vol_down').setEmoji('🔉').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('vol_up').setEmoji('🔊').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('vol_mute').setEmoji('🔇').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('queue').setLabel('คิว').setEmoji('📋').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('np').setEmoji('🎵').setStyle(ButtonStyle.Secondary));
  return [r1,r2,r3];
}

async function updateUI(client,g){
  const cid=requestChannels[g]; if(!cid) return;
  const ch=client.channels.cache.get(cid); if(!ch) return;
  const playing=nowPlaying[g]&&players[g]&&(players[g].state.status===AudioPlayerStatus.Playing||players[g].state.status===AudioPlayerStatus.Paused);
  const q=queues[g]||[], np=nowPlaying[g], guild=client.guilds.cache.get(g), vc=guild?.members?.me?.voice?.channel;
  let emb,rows;
  if(playing){
    const lp=loopModes[g]||'off', vol=Math.round((volumes[g]??1.0)*100);
    emb=new EmbedBuilder().setTitle('🎵 '+np.title).setColor(0x1DB954)
      .addFields(
        {name:'✨ เจ้าของเพลง',value:'`'+(np.author||'?')+'`',inline:true},
        {name:'⏱️ ความยาว',value:'`'+np.duration+'`',inline:true},
        {name:'🎵 คิว',value:'`'+q.length+'` เพลง',inline:true},
        {name:'👤 ขอโดย',value:np.requester,inline:true},
        {name:'🔊 ห้องเสียง',value:vc?vc.name:'-',inline:true},
        {name:'🔁 Loop',value:'`'+lp+'`',inline:true},
        {name:'🔊 Volume',value:'`'+vol+'%`',inline:true},
        {name:'✨ Effect',value:'`'+(currentEffects[g]||'NORMAL').toUpperCase()+'`',inline:true});
    if(np.thumbnail) emb.setImage(np.thumbnail);
    rows=makeRows();
  } else {
    emb=new EmbedBuilder().setTitle('🎶 แมวส้มชาวด์').setDescription('พิมพ์ชื่อเพลงหรือลิงก์ YouTube! ✨').setColor(0x1DB954);
    rows=[];
  }
  try{ if(controlMessages[g]) await controlMessages[g].edit({embeds:[emb],components:rows}); else controlMessages[g]=await ch.send({embeds:[emb],components:rows}); }
  catch(e){ try{ controlMessages[g]=await ch.send({embeds:[emb],components:rows}); }catch(e){} }
}

// ==================== Client ====================
const client=new Client({intents:[GatewayIntentBits.Guilds,GatewayIntentBits.GuildMessages,GatewayIntentBits.MessageContent,GatewayIntentBits.GuildVoiceStates]});

client.once(Events.ClientReady, async c=>{
  log.info('บอทออนไลน์: '+c.user.tag);
  const deps = autoInstall();
  YTDLP_PATH = deps.ytdlpPath;
  FFMPEG_PATH = deps.ffmpegBin;
  requestChannels = loadChannels();
  try{
    await c.application.commands.set([
      {name:'setup',description:'สร้างห้องขอเพลง'},
      {name:'join',description:'เข้าห้องเสียง'},
      {name:'leave',description:'ออกจากห้องเสียง'},
      {name:'skip',description:'ข้ามเพลง'},
      {name:'queue',description:'ดูคิว'},
      {name:'clear',description:'ล้างคิว'}
    ]);
    log.info('ซิงค์คำสั่งสำเร็จ');
  }catch(e){ log.error('Sync:',e.message); }
  log.info('พร้อมใช้งาน 🎵');
});

client.on(Events.MessageCreate, async msg=>{
  if(msg.author.bot||!msg.guild) return;
  const g=msg.guild.id;
  if(!requestChannels[g]||msg.channel.id!==requestChannels[g]) return;
  const q=msg.content.trim(); if(!q) return;
  try{ await msg.delete(); }catch(e){}
  const vc=msg.member?.voice?.channel;
  if(!vc){ const m=await msg.channel.send('❌ เข้าห้องเสียงก่อน!'); setTimeout(()=>m.delete().catch(()=>{}),5000); return; }
  try{ await ensureVoice(msg.guild,vc); }catch(e){ const m=await msg.channel.send('❌ '+e.message); setTimeout(()=>m.delete().catch(()=>{}),5000); return; }
  if(!queues[g]) queues[g]=[];
  queues[g].push({query:q,requester:msg.author.toString(),roomName:vc.name});
  const pl=players[g];
  if(pl&&pl.state.status!==AudioPlayerStatus.Idle){ await updateUI(client,g); }
  else playNext(g,client);
});

client.on(Events.InteractionCreate, async it=>{
  if(!it.guild) return;
  const g=it.guild.id, flags=MessageFlags.Ephemeral;

  if(it.isChatInputCommand()){
    const cmd=it.commandName;
    if(cmd==='setup'){
      try{ await it.deferReply({flags}); }catch(e){ return; }
      let ch=it.guild.channels.cache.find(c=>c.name==='🎵・ห้องขอเพลง');
      if(ch){ requestChannels[g]=ch.id; saveChannels(); return it.editReply({content:'📌 มีแล้ว: '+ch}); }
      try{
        ch=await it.guild.channels.create({name:'🎵・ห้องขอเพลง',type:ChannelType.GuildText});
        requestChannels[g]=ch.id; saveChannels();
        const emb=new EmbedBuilder().setTitle('🎶 แมวส้มชาวด์').setDescription('พิมพ์ชื่อเพลง! ✨').setColor(0x1DB954);
        controlMessages[g]=await ch.send({embeds:[emb]});
        await it.editReply({content:'✅ สร้างแล้ว: '+ch});
      }catch(e){ await it.editReply({content:'❌ '+e.message}); }
      return;
    }
    if(cmd==='join'){ const vc=it.member?.voice?.channel; if(!vc) return it.reply({content:'❌ เข้าห้องเสียงก่อน',flags}).catch(()=>{}); try{ await ensureVoice(it.guild,vc); return it.reply({content:'📥 '+vc.name,flags}).catch(()=>{}); }catch(e){ return it.reply({content:'❌ '+e.message,flags}).catch(()=>{}); } }
    if(cmd==='leave'){ killProcs(g); try{connections[g]?.destroy()}catch(e){} delete connections[g]; return it.reply({content:'👋',flags}).catch(()=>{}); }
    if(cmd==='skip'){ if(players[g]){ killProcs(g); try{players[g].stop()}catch(e){} return it.reply({content:'⏭️',flags}).catch(()=>{}); } return it.reply({content:'❌',flags}).catch(()=>{}); }
    if(cmd==='clear'){ queues[g]=[]; return it.reply({content:'🗑️',flags}).catch(()=>{}); }
  }

  if(it.isButton()){
    const id=it.customId;
    try{ await it.deferReply({flags}); }catch(e){ return; }
    if(id==='stop'){ queues[g]=[]; killProcs(g); try{players[g]?.stop()}catch(e){} delete players[g]; try{connections[g]?.destroy()}catch(e){} delete connections[g]; nowPlaying[g]=null; await updateUI(client,g); return it.editReply({content:'⏹️'}).catch(()=>{}); }
    if(id==='pause'){ const p=players[g]; if(!p) return it.editReply({content:'❌'}).catch(()=>{}); if(p.state.status===AudioPlayerStatus.Playing){p.pause(); return it.editReply({content:'⏸️'}).catch(()=>{});} if(p.state.status===AudioPlayerStatus.Paused){p.unpause(); return it.editReply({content:'▶️'}).catch(()=>{});} return it.editReply({content:'❌'}).catch(()=>{}); }
    if(id==='skip'){ if(players[g]){ killProcs(g); try{players[g].stop()}catch(e){} return it.editReply({content:'⏭️'}).catch(()=>{}); } return it.editReply({content:'❌'}).catch(()=>{}); }
    if(id==='vol_up'){ const c=volumes[g]??1.0; volumes[g]=Math.min(2.0,c+0.1); applyVol(g); await updateUI(client,g); return it.editReply({content:'🔊'}).catch(()=>{}); }
    if(id==='vol_down'){ const c=volumes[g]??1.0; volumes[g]=Math.max(0,c-0.1); applyVol(g); await updateUI(client,g); return it.editReply({content:'🔉'}).catch(()=>{}); }
    if(id==='vol_mute'){ const c=volumes[g]??1.0; volumes[g]=c>0?0:1.0; applyVol(g); await updateUI(client,g); return it.editReply({content:'🔇'}).catch(()=>{}); }
    if(id==='shuffle'){ const q=queues[g]||[]; if(q.length>1){ for(let i=q.length-1;i>0;i--){ const j=Math.floor(Math.random()*(i+1)); [q[i],q[j]]=[q[j],q[i]]; } await updateUI(client,g); return it.editReply({content:'🔀'}).catch(()=>{}); } return it.editReply({content:'❌'}).catch(()=>{}); }
    if(id==='loop'){ const c=loopModes[g]||'off'; loopModes[g]=c==='off'?'single':(c==='single'?'queue':'off'); await updateUI(client,g); return it.editReply({content:'🔁'}).catch(()=>{}); }
  }

  if(it.isStringSelectMenu()&&it.customId==='effect_select'){
    const c=it.values[0];
    currentEffects[g]=c==='normal'?null:c;
    try{ await it.deferReply({flags}); }catch(e){ return; }
    await updateUI(client,g);
    return it.editReply({content:'✨ '+c.toUpperCase()}).catch(()=>{});
  }
});

client.login(TOKEN).catch(e=>{ log.error('Login:',e.message); process.exit(1); });