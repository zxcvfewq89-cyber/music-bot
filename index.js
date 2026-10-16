const dns = require('dns');
try { dns.setDefaultResultOrder('ipv4first'); dns.setServers(['8.8.8.8','8.8.4.4']); } catch(e){}

const { Client, GatewayIntentBits, Events, EmbedBuilder, ActionRowBuilder, StringSelectMenuBuilder, ButtonBuilder, ButtonStyle, ChannelType, MessageFlags } = require('discord.js');
const { joinVoiceChannel, createAudioPlayer, createAudioResource, AudioPlayerStatus, VoiceConnectionStatus, entersState, StreamType, NoSubscriberBehavior } = require('@discordjs/voice');
const play = require('play-dl');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const TOKEN = process.env.DISCORD_TOKEN;
if (!TOKEN) { console.error('ไม่พบ DISCORD_TOKEN'); process.exit(1); }

const CONFIG_FILE = path.join(__dirname, 'request_channels.json');

// ==================== Logger ====================
function ts() { const d=new Date(); return String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0')+':'+String(d.getSeconds()).padStart(2,'0'); }
const C={r:'\x1b[0m',g:'\x1b[32m',y:'\x1b[33m',b:'\x1b[34m',m:'\x1b[35m',c:'\x1b[36m',x:'\x1b[31m',gr:'\x1b[90m'};
const log={
  info:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.g}OK${C.r} ${m}`,...a),
  warn:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.y}!!${C.r} ${m}`,...a),
  error:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.x}XX${C.r} ${m}`,...a),
  music:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.m}MS${C.r} ${m}`,...a),
  voice:(m,...a)=>console.log(`${C.gr}[${ts()}]${C.r} ${C.b}VC${C.r} ${m}`,...a),
};

process.on('uncaughtException', (e) => { if (e?.message?.includes('Unknown')) return; log.error('Uncaught:', e.message); });
process.on('unhandledRejection', (e) => { if (e?.message?.includes('Unknown')) return; log.error('Unhandled:', e?.message||e); });

// ==================== Storage ====================
const queues={},controlMessages={},currentEffects={},nowPlaying={},connections={},players={},volumes={},loopModes={},history={},metaCache=new Map();
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

// ==================== Voice Connection ====================
async function ensureVoice(guild, voiceChannel){
  let conn = connections[guild.id];
  if(conn && conn.joinConfig.channelId === voiceChannel.id && conn.state.status === VoiceConnectionStatus.Ready) return conn;
  if(conn){ try{conn.destroy()}catch(e){} delete connections[guild.id]; }
  
  log.voice(`เข้าห้อง: ${voiceChannel.name}`);
  conn = joinVoiceChannel({
    channelId: voiceChannel.id,
    guildId: guild.id,
    adapterCreator: guild.voiceAdapterCreator,
    selfDeaf: true,
    selfMute: false
  });
  
  connections[guild.id] = conn;
  conn.on('stateChange', (o, n) => { if (o.status !== n.status) log.voice(`${o.status} -> ${n.status}`); });
  conn.on('error', e => log.error('Voice:', e.message));

  try { 
    await entersState(conn, VoiceConnectionStatus.Ready, 20000); 
    return conn; 
  } catch(e) { 
    try{conn.destroy()}catch(x){} 
    delete connections[guild.id]; 
    throw new Error('เชื่อมต่อห้องเสียงไม่ได้ (การส่งสัญญาณล้มเหลว)'); 
  }
}

// ==================== Play Next ====================
async function playNext(g, client){
  const q = queues[g] || [], loop = loopModes[g] || 'off', np = nowPlaying[g];
  if(q.length === 0){
    if(loop === 'single' && np) queues[g].push({query: np.query, requester: np.requester, roomName: np.roomName});
    else if(loop === 'queue' && history[g]?.length){ queues[g] = history[g].slice(); history[g] = []; }
    else {
      nowPlaying[g] = null; await updateUI(client, g);
      setTimeout(async () => {
        if((queues[g]||[]).length === 0 && connections[g]){
          try{connections[g].destroy()}catch(e){}
          delete connections[g]; await updateUI(client, g);
        }
      }, 60000);
      return;
    }
  }

  const song = queues[g].shift();
  nowPlaying[g] = { title: 'กำลังโหลด...', author: '...', duration: '0:00', requester: song.requester, thumbnail: null, query: song.query, roomName: song.roomName };
  await updateUI(client, g);

  try {
    // ค้นหาและสร้างสตรีมเพลงด้วย play-dl
    const yt_info = await play.search(song.query, { limit: 1 });
    if (!yt_info || yt_info.length === 0) throw new Error('ไม่พบเพลงที่ค้นหา');

    const video = yt_info[0];
    nowPlaying[g].title = video.title;
    nowPlaying[g].author = video.channel?.name || 'Unknown';
    nowPlaying[g].duration = fmtDuration(video.durationInSec);
    nowPlaying[g].thumbnail = video.thumbnails[0]?.url || null;

    const stream = await play.stream(video.url);
    const resource = createAudioResource(stream.stream, {
      inputType: stream.type,
      inlineVolume: true
    });

    if (resource.volume) resource.volume.setVolume(volumes[g] ?? 1.0);

    if(!players[g]){
      players[g] = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Play } });
      players[g].on(AudioPlayerStatus.Idle, () => playNext(g, client));
      players[g].on('error', e => { log.error('Player:', e.message); playNext(g, client); });
    }

    if(connections[g]) connections[g].subscribe(players[g]);
    players[g].play(resource);
    await updateUI(client, g);

  } catch(e) {
    log.error('Play:', e.message);
    playNext(g, client);
  }
}

// ==================== UI ====================
function makeRows(){
  const r2 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('pause').setEmoji('⏸️').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('skip').setEmoji('⏭️').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('stop').setEmoji('⏹️').setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId('loop').setEmoji('🔁').setStyle(ButtonStyle.Secondary)
  );
  return [r2];
}

async function updateUI(client, g){
  const cid = requestChannels[g]; if(!cid) return;
  const ch = client.channels.cache.get(cid); if(!ch) return;
  const playing = nowPlaying[g] && players[g] && (players[g].state.status === AudioPlayerStatus.Playing || players[g].state.status === AudioPlayerStatus.Paused);
  const q = queues[g] || [], np = nowPlaying[g], guild = client.guilds.cache.get(g), vc = guild?.members?.me?.voice?.channel;
  let emb, rows;
  if(playing){
    const vol = Math.round((volumes[g] ?? 1.0) * 100);
    emb = new EmbedBuilder().setTitle('🎵 ' + np.title).setColor(0x1DB954)
      .addFields(
        {name:'✨ ช่อง', value:'`'+np.author+'`', inline:true},
        {name:'⏱️ ความยาว', value:'`'+np.duration+'`', inline:true},
        {name:'🎵 คิว', value:'`'+q.length+'` เพลง', inline:true},
        {name:'👤 ขอโดย', value:np.requester, inline:true},
        {name:'🔊 ห้องเสียง', value:vc ? vc.name : '-', inline:true}
      );
    if(np.thumbnail) emb.setImage(np.thumbnail);
    rows = makeRows();
  } else {
    emb = new EmbedBuilder().setTitle('🎶 แมวส้มชาวด์').setDescription('พิมพ์ชื่อเพลงหรือลิงก์ YouTube ได้เลย! ✨').setColor(0x1DB954);
    rows = [];
  }
  try{ if(controlMessages[g]) await controlMessages[g].edit({embeds:[emb], components:rows}); else controlMessages[g] = await ch.send({embeds:[emb], components:rows}); }
  catch(e){ try{ controlMessages[g] = await ch.send({embeds:[emb], components:rows}); }catch(e){} }
}

// ==================== Client ====================
const client = new Client({intents:[GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent, GatewayIntentBits.GuildVoiceStates]});

client.once(Events.ClientReady, async c => {
  log.info('บอทออนไลน์: ' + c.user.tag);
  requestChannels = loadChannels();
  try {
    await c.application.commands.set([
      {name:'setup', description:'สร้างห้องขอเพลง'},
      {name:'join', description:'เข้าห้องเสียง'},
      {name:'leave', description:'ออกจากห้องเสียง'},
      {name:'skip', description:'ข้ามเพลง'}
    ]);
  } catch(e){}
  log.info('พร้อมใช้งาน 🎵');
});

client.on(Events.MessageCreate, async msg => {
  if(msg.author.bot || !msg.guild) return;
  const g = msg.guild.id;
  if(!requestChannels[g] || msg.channel.id !== requestChannels[g]) return;
  const q = msg.content.trim(); if(!q) return;
  try{ await msg.delete(); }catch(e){}

  const vc = msg.member?.voice?.channel;
  if(!vc){ const m = await msg.channel.send('❌ เข้าห้องเสียงก่อน!'); setTimeout(()=>m.delete().catch(()=>{}),5000); return; }

  try{ await ensureVoice(msg.guild, vc); } catch(e){ const m = await msg.channel.send('❌ '+e.message); setTimeout(()=>m.delete().catch(()=>{}),5000); return; }

  if(!queues[g]) queues[g] = [];
  queues[g].push({query:q, requester:msg.author.toString(), roomName:vc.name});

  const pl = players[g];
  if(pl && pl.state.status !== AudioPlayerStatus.Idle){ await updateUI(client, g); }
  else playNext(g, client);
});

client.on(Events.InteractionCreate, async it => {
  if(!it.guild) return;
  const g = it.guild.id, flags = MessageFlags.Ephemeral;

  if(it.isChatInputCommand()){
    const cmd = it.commandName;
    if(cmd === 'setup'){
      try{ await it.deferReply({flags}); }catch(e){ return; }
      let ch = it.guild.channels.cache.find(c => c.name === '🎵・ห้องขอเพลง');
      if(ch){ requestChannels[g] = ch.id; saveChannels(); return it.editReply({content:'📌 มีห้องขอเพลงอยู่แล้ว: '+ch}); }
      try{
        ch = await it.guild.channels.create({name:'🎵・ห้องขอเพลง', type:ChannelType.GuildText});
        requestChannels[g] = ch.id; saveChannels();
        const emb = new EmbedBuilder().setTitle('🎶 แมวส้มชาวด์').setDescription('พิมพ์ชื่อเพลงหรือลิงก์ YouTube ได้เลย! ✨').setColor(0x1DB954);
        controlMessages[g] = await ch.send({embeds:[emb]});
        await it.editReply({content:'✅ สร้างห้องสำเร็จ: '+ch});
      }catch(e){ await it.editReply({content:'❌ '+e.message}); }
      return;
    }
    if(cmd === 'join'){
      const vc = it.member?.voice?.channel;
      if(!vc) return it.reply({content:'❌ กรุณาเข้าห้องเสียงก่อน', flags}).catch(()=>{});
      try{ await ensureVoice(it.guild, vc); return it.reply({content:'📥 เข้าห้อง: '+vc.name, flags}).catch(()=>{}); }
      catch(e){ return it.reply({content:'❌ '+e.message, flags}).catch(()=>{}); }
    }
    if(cmd === 'leave'){
      try{connections[g]?.destroy()}catch(e){} delete connections[g];
      return it.reply({content:'👋 ออกจากห้องเสียงแล้ว', flags}).catch(()=>{});
    }
    if(cmd === 'skip'){
      if(players[g]){ try{players[g].stop()}catch(e){} return it.reply({content:'⏭️ ข้ามเพลงแล้ว', flags}).catch(()=>{}); }
      return it.reply({content:'❌ ไม่มีเพลงกำลังเล่น', flags}).catch(()=>{});
    }
  }

  if(it.isButton()){
    const id = it.customId;
    try{ await it.deferReply({flags}); }catch(e){ return; }
    if(id === 'stop'){ queues[g] = []; try{players[g]?.stop()}catch(e){} delete players[g]; try{connections[g]?.destroy()}catch(e){} delete connections[g]; nowPlaying[g] = null; await updateUI(client, g); return it.editReply({content:'⏹️ หยุดเพลง'}).catch(()=>{}); }
    if(id === 'pause'){ const p = players[g]; if(!p) return it.editReply({content:'❌'}).catch(()=>{}); if(p.state.status === AudioPlayerStatus.Playing){p.pause(); return it.editReply({content:'⏸️ พักเพลง'}).catch(()=>{});} if(p.state.status === AudioPlayerStatus.Paused){p.unpause(); return it.editReply({content:'▶️ เล่นต่อ'}).catch(()=>{});} return it.editReply({content:'❌'}).catch(()=>{}); }
    if(id === 'skip'){ if(players[g]){ try{players[g].stop()}catch(e){} return it.editReply({content:'⏭️ ข้ามเพลง'}).catch(()=>{}); } return it.editReply({content:'❌'}).catch(()=>{}); }
  }
});

client.login(TOKEN).catch(e => { log.error('Login:', e.message); process.exit(1); });
