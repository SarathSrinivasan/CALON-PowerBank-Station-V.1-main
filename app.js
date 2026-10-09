/* CALON MQTT CORE
   - Preferred: one SharedWorker connection per site origin.
   - Fallback: direct MQTT.js connection for local file testing / browsers without SharedWorker.
   - No page asks the customer to connect manually.
*/
const CALON = {
  stationId: 'POWERBANK001',
  stationLabel: 'CALON Station 001',
  broker: 'wss://broker.emqx.io:8084/mqtt',
  mqttCdn: 'https://unpkg.com/mqtt@5.14.1/dist/mqtt.min.js',
  topics: {
    rent: 'POWERBANK/001/RENT_REQUEST',
    rentResponse: 'POWERBANK/001/RENT_RESPONSE',
    rentPlan: 'POWERBANK/001/RENT_PLAN',
    availability: 'POWERBANK/001/AVAILABILITY',
    select: 'POWERBANK/001/POWERBANK_SELECT',
    payment: 'POWERBANK/001/PAYMENT',
    paymentResponse: 'POWERBANK/001/PAYMENT_RESPONSE',
    slot: 'POWERBANK/001/SLOT_STATUS',
    return: 'POWERBANK/001/RETURN_REQUEST',
    returnResponse: 'POWERBANK/001/RETURN_RESPONSE',
    returnStatus: 'POWERBANK/001/RETURN_STATUS',
    error: 'POWERBANK/001/ERROR'
  }
};

const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);
const esc = s => String(s ?? '').replace(/[&<>'"]/g, c => ({
  '&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'
}[c]));
function id(prefix) {
  const rnd = globalThis.crypto?.randomUUID
    ? crypto.randomUUID().replaceAll('-','').slice(0,10).toUpperCase()
    : Math.random().toString(36).slice(2,12).toUpperCase();
  return `${prefix}-${rnd}`;
}
function getSession(){ try { return JSON.parse(localStorage.getItem('calon_session') || '{}'); } catch { return {}; } }
function setSession(values){ localStorage.setItem('calon_session', JSON.stringify({...getSession(), ...values})); }
function clearSession(){ localStorage.removeItem('calon_session'); }
function clearRental(){
  const s=getSession();
  localStorage.setItem('calon_session', JSON.stringify({
    stationId: CALON.stationId,
    customerId: s.customerId || id('CUS')
  }));
}
function log(msg){
  const el=$('#log');
  if(el){ el.textContent += `[${new Date().toLocaleTimeString()}] ${msg}\n`; el.scrollTop=el.scrollHeight; }
}
function status(text,type=''){
  const e=$('#mqttStatus');
  if(e){
    e.className=`status ${type}`;
    e.innerHTML=`<span class="status-dot"></span><span>${esc(text)}</span>`;
  }
}
function headerStatus(state){
  const on=state==='connected';
  $$( '[data-mqtt-dot]' ).forEach(e=>e.classList.toggle('on',on));
  $$('[data-mqtt-label]').forEach(e=>{
    e.textContent = state==='connected' ? 'Connected' :
      state==='reconnecting' ? 'Reconnecting' :
      state==='connecting' ? 'Connecting' : 'Offline';
  });
  $$('[data-mqtt-pill]').forEach(e=>e.className=`mqtt-pill ${on?'online':''}`);
}

let transport = null;
let mqttReady = false;
let initPromise = null;
const messageCache = new Map();
const messageListeners = new Map();
const publishQueue = [];

function dispatchMessage(m){
  if(!m || m.type!=='message') return;
  messageCache.set(m.topic, m);
  log(`${m.topic} → ${m.raw}`);
  window.dispatchEvent(new CustomEvent('calon-message',{detail:m}));
  const list=messageListeners.get(m.topic) || [];
  list.forEach(fn=>{ try{ fn(m.data,m.raw); }catch(e){ console.error(e); } });
}

function handleTransportMessage(m){
  if(!m) return;
  if(m.type==='status'){
    mqttReady=m.status==='connected';
    headerStatus(m.status);
    status(
      m.status==='connected' ? 'Station messaging is live' :
      m.status==='reconnecting' ? 'Reconnecting to station…' :
      m.status==='connecting' ? 'Connecting to station…' :
      'Station messaging offline',
      m.status==='connected' ? 'success' :
      m.status==='reconnecting'||m.status==='connecting' ? 'warn' : 'error'
    );
    window.dispatchEvent(new CustomEvent('calon-status',{detail:m}));
    if(m.status==='connected') flushPublishQueue();
  }
  if(m.type==='message') dispatchMessage(m);
  if(m.type==='published') log(`${m.topic} ← ${JSON.stringify(m.payload)}`);
  if(m.type==='publish_error'){
    mqttReady=false;
    status(m.message || 'MQTT publish failed','error');
    log(`Publish error: ${m.message || 'unknown error'}`);
  }
  if(m.type==='error'){
    log(`MQTT error: ${m.message || 'unknown error'}`);
  }
  if(m.type==='fatal'){
    log(`MQTT transport fallback: ${m.message || 'SharedWorker unavailable'}`);
    startDirectFallback();
  }
}

function attachWorker(){
  try{
    const w = new SharedWorker(new URL('mqtt-worker.js', location.href));
    w.port.start();
    w.port.onmessage = e => handleTransportMessage(e.data || {});
    w.port.postMessage({type:'ping'});
    transport = { type:'worker', port:w.port };
    return true;
  }catch(e){
    console.warn('SharedWorker unavailable:',e);
    return false;
  }
}

function loadMqttScript(){
  return new Promise((resolve,reject)=>{
    if(window.mqtt) return resolve(window.mqtt);
    const existing=document.querySelector('script[data-calon-mqtt]');
    if(existing){
      existing.addEventListener('load',()=>resolve(window.mqtt),{once:true});
      existing.addEventListener('error',()=>reject(new Error('MQTT.js CDN failed to load')),{once:true});
      return;
    }
    const s=document.createElement('script');
    s.src=CALON.mqttCdn;
    s.async=true;
    s.dataset.calonMqtt='1';
    s.onload=()=>window.mqtt ? resolve(window.mqtt) : reject(new Error('MQTT.js loaded but was unavailable'));
    s.onerror=()=>reject(new Error('MQTT.js CDN failed to load'));
    document.head.appendChild(s);
  });
}

async function startDirectFallback(){
  if(transport?.type==='direct' && transport.client) return;
  if(transport?.type==='direct' && transport.loading) return;
  const holder={type:'direct',loading:true,client:null};
  transport=holder;
  headerStatus('connecting');
  status('Connecting to MQTT…','warn');
  try{
    const mqtt = await loadMqttScript();
    const client = mqtt.connect(CALON.broker,{
      clientId:'CALON_WEB_' + Math.random().toString(16).slice(2,12),
      clean:true,
      reconnectPeriod:3000,
      connectTimeout:10000,
      keepalive:45,
      resubscribe:true
    });
    holder.loading=false;
    holder.client=client;
    client.on('connect',()=>{
      client.subscribe([CALON.topics.availability,CALON.topics.slot,CALON.topics.returnStatus,CALON.topics.error],{qos:1},err=>{
        if(err){
          handleTransportMessage({type:'error',message:'Subscription failed: '+err.message});
          headerStatus('offline');
          return;
        }
        handleTransportMessage({type:'status',status:'connected'});
      });
    });
    client.on('reconnect',()=>handleTransportMessage({type:'status',status:'reconnecting'}));
    client.on('offline',()=>handleTransportMessage({type:'status',status:'offline'}));
    client.on('close',()=>{ if(!client.connected) handleTransportMessage({type:'status',status:'offline'}); });
    client.on('error',err=>handleTransportMessage({type:'error',message:err?.message||'MQTT error'}));
    client.on('message',(topic,payload)=>{
      const raw=payload.toString();
      let data=raw; try{data=JSON.parse(raw)}catch{}
      dispatchMessage({type:'message',topic,data,raw});
    });
  }catch(e){
    holder.loading=false;
    transport=null;
    headerStatus('offline');
    status('MQTT unavailable: '+e.message,'error');
    log('MQTT fallback failed: '+e.message);
  }
}

function initMQTT(){
  if(initPromise) return initPromise;
  initPromise=(async()=>{
    headerStatus('connecting');
    status('Connecting to MQTT…','warn');
    // On normal HTTPS/GitHub Pages, SharedWorker is the single connection.
    // On file:// or browsers without SharedWorker, direct MQTT.js is used so testing still works.
    const workerOK=attachWorker();
    if(!workerOK) await startDirectFallback();
    return transport;
  })();
  return initPromise;
}

function publish(topic,payload){
  const message={topic,payload};
  if(mqttReady && transport){
    if(transport.type==='worker'){
      transport.port.postMessage({type:'publish',topic,payload});
      return true;
    }
    if(transport.type==='direct' && transport.client){
      transport.client.publish(topic,typeof payload==='string'?payload:JSON.stringify(payload),{qos:1,retain:false},err=>{
        if(err) handleTransportMessage({type:'publish_error',message:err.message,topic});
        else handleTransportMessage({type:'published',topic,payload});
      });
      return true;
    }
  }
  // Queue the command; it will be published immediately after MQTT connects.
  publishQueue.push(message);
  status('Waiting for MQTT connection…','warn');
  log(`Queued publish: ${topic}`);
  initMQTT();
  return true;
}

function flushPublishQueue(){
  if(!mqttReady || !publishQueue.length) return;
  const queued=publishQueue.splice(0);
  queued.forEach(m=>{
    if(transport?.type==='worker') transport.port.postMessage({type:'publish',topic:m.topic,payload:m.payload});
    else if(transport?.type==='direct' && transport.client) transport.client.publish(m.topic,typeof m.payload==='string'?m.payload:JSON.stringify(m.payload),{qos:1,retain:false},err=>{
      if(err) handleTransportMessage({type:'publish_error',message:err.message,topic:m.topic});
      else handleTransportMessage({type:'published',topic:m.topic,payload:m.payload});
    });
  });
}

function onMessage(topic,fn){
  if(!messageListeners.has(topic)) messageListeners.set(topic,[]);
  messageListeners.get(topic).push(fn);
  // Critical: immediately deliver the latest cached message so navigation
  // cannot miss an availability/status message that arrived before DOM setup.
  const cached=messageCache.get(topic);
  if(cached) setTimeout(()=>{ try{fn(cached.data,cached.raw)}catch(e){console.error(e)} },0);
  return ()=>messageListeners.set(topic,(messageListeners.get(topic)||[]).filter(x=>x!==fn));
}

function showError(msg){const e=$('#flowError');if(e){e.textContent=msg;e.classList.remove('hidden')}else alert(msg);}
function hideError(){const e=$('#flowError');if(e)e.classList.add('hidden');}
function common(){
  initMQTT();
  const s=getSession();
  $$('[data-session-id]').forEach(e=>e.textContent=s.requestId||'—');
  $$('[data-station-id]').forEach(e=>e.textContent=s.stationId||CALON.stationId);
  $$('[data-customer-id]').forEach(e=>e.textContent=s.customerId||'—');
}
document.addEventListener('DOMContentLoaded',common);
