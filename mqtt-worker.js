/* CALON Shared MQTT Worker
   ONE broker connection per normal web origin.
   Pages attach to this worker; they never connect directly when SharedWorker works.
*/
let mqttLoaded = false;
let mqttLoadError = null;
try {
  importScripts('https://unpkg.com/mqtt@5.14.1/dist/mqtt.min.js');
  mqttLoaded = typeof mqtt !== 'undefined';
  if (!mqttLoaded) mqttLoadError = 'MQTT.js loaded but global mqtt is missing';
} catch (e) {
  mqttLoadError = e?.message || 'MQTT.js could not be loaded inside SharedWorker';
}

const BROKER='wss://broker.emqx.io:8084/mqtt';
const topics={
  rent:'POWERBANK/001/RENT_REQUEST',
  rentResponse:'POWERBANK/001/RENT_RESPONSE',
  rentPlan:'POWERBANK/001/RENT_PLAN',
  availability:'POWERBANK/001/AVAILABILITY',
  select:'POWERBANK/001/POWERBANK_SELECT',
  payment:'POWERBANK/001/PAYMENT',
  paymentResponse:'POWERBANK/001/PAYMENT_RESPONSE',
  slot:'POWERBANK/001/SLOT_STATUS',
  return:'POWERBANK/001/RETURN_REQUEST',
  returnResponse:'POWERBANK/001/RETURN_RESPONSE',
  returnStatus:'POWERBANK/001/RETURN_STATUS',
  error:'POWERBANK/001/ERROR'
};
let client=null,connecting=false,connectionState='offline';
const ports=new Set(),lastMessages=new Map();

function post(p,m){try{p.postMessage(m)}catch{}}
function broadcast(m){for(const p of ports)post(p,m)}
function state(s,extra={}){connectionState=s;broadcast({type:'status',status:s,...extra})}
function sendState(p){
  post(p,{type:'status',status:connectionState});
  post(p,{type:'topics',topics});
  for(const m of lastMessages.values()) post(p,m);
  if(mqttLoadError) post(p,{type:'fatal',message:mqttLoadError});
}
function connect(){
  if(!mqttLoaded){
    state('offline',{message:mqttLoadError||'MQTT.js unavailable'});
    broadcast({type:'fatal',message:mqttLoadError||'MQTT.js unavailable'});
    return;
  }
  if(client && (client.connected||client.reconnecting||connecting)) return;
  connecting=true; state('connecting');
  client=mqtt.connect(BROKER,{
    clientId:'CALON_SHARED_'+Math.random().toString(16).slice(2,12),
    clean:true,reconnectPeriod:3000,connectTimeout:10000,keepalive:45,resubscribe:true
  });
  client.on('connect',()=>{
    connecting=false;
    client.subscribe([topics.availability,topics.slot,topics.returnStatus,topics.error],{qos:1},err=>{
      if(err){state('offline',{message:'Subscription failed: '+err.message});return;}
      state('connected');
    });
  });
  client.on('reconnect',()=>{connecting=false;state('reconnecting')});
  client.on('offline',()=>{connecting=false;state('offline')});
  client.on('close',()=>{connecting=false;if(!client.connected)state('offline')});
  client.on('error',err=>broadcast({type:'error',message:err?.message||'MQTT connection error'}));
  client.on('message',(topic,payload)=>{
    const raw=payload.toString();
    let data=raw;try{data=JSON.parse(raw)}catch{}
    const m={type:'message',topic,data,raw};
    lastMessages.set(topic,m);broadcast(m);
  });
}
self.onconnect=function(ev){
  const port=ev.ports[0]; ports.add(port); port.start();
  sendState(port); connect();
  port.onmessage=function(e){
    const m=e.data||{};
    if(m.type==='ping'){sendState(port);connect();return;}
    if(m.type==='publish'){
      if(!client||!client.connected){
        post(port,{type:'publish_error',message:'MQTT is not connected yet',topic:m.topic});
        return;
      }
      const body=typeof m.payload==='string'?m.payload:JSON.stringify(m.payload);
      client.publish(m.topic,body,{qos:1,retain:false},err=>{
        if(err)post(port,{type:'publish_error',message:err.message,topic:m.topic});
        else post(port,{type:'published',topic:m.topic,payload:m.payload});
      });
    }
  };
};
