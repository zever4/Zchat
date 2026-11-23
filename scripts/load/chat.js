import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import exec from 'k6/execution';
import { WebSocket } from 'k6/websockets';
import { setInterval, clearInterval, setTimeout, clearTimeout } from 'k6/timers';
import { Counter, Trend, Rate } from 'k6/metrics';
import { expireDeliveries, receiveDelivery } from './delivery.mjs';

const pairs=new SharedArray('private lab pairs',()=>JSON.parse(open('/data/accounts.json')).pairs);
const profile=__ENV.LOAD_PROFILE||'smoke', base='http://nginx';
const profiles={
 'profile-write':{stages:[{duration:'30s',target:50},{duration:'45s',target:50},{duration:'10s',target:0}],http:[],messageInterval:250},
 'profile-mixed':{stages:[{duration:'30s',target:50},{duration:'45s',target:50},{duration:'10s',target:0}],http:[{duration:'30s',target:100},{duration:'45s',target:100},{duration:'10s',target:1}],messageInterval:250},
 smoke:{stages:[{duration:'20s',target:2}],http:[{duration:'20s',target:1}]},
 baseline:{stages:[{duration:'30s',target:10},{duration:'2m',target:10},{duration:'30s',target:0}],http:[{duration:'30s',target:5},{duration:'2m',target:5},{duration:'30s',target:1}]},
 stress:{stages:[{duration:'30s',target:10},{duration:'1m',target:10},{duration:'30s',target:25},{duration:'1m',target:25},{duration:'30s',target:50},{duration:'1m',target:50},{duration:'1m',target:100},{duration:'1m',target:100},{duration:'2m',target:200},{duration:'1m',target:200},{duration:'30s',target:0},{duration:'1m',target:2},{duration:'30s',target:2}],http:[{duration:'30s',target:5},{duration:'1m',target:5},{duration:'30s',target:10},{duration:'1m',target:10},{duration:'30s',target:25},{duration:'1m',target:25},{duration:'1m',target:50},{duration:'1m',target:50},{duration:'2m',target:100},{duration:'1m',target:100},{duration:'30s',target:1},{duration:'1m',target:1},{duration:'30s',target:1}]},
 spike:{stages:[{duration:'30s',target:10},{duration:'5s',target:100},{duration:'30s',target:100},{duration:'5s',target:2},{duration:'1m',target:2}],http:[{duration:'30s',target:5},{duration:'5s',target:50},{duration:'30s',target:50},{duration:'5s',target:1},{duration:'1m',target:1}]},
 soak:{stages:[{duration:'30s',target:20},{duration:'15m',target:20},{duration:'30s',target:0}],http:[{duration:'30s',target:10},{duration:'15m',target:10},{duration:'30s',target:1}]}
};
if(!profiles[profile])throw Error('Unknown LOAD_PROFILE');
const seconds=v=>Number(v.slice(0,-1))*(v.endsWith('m')?60:1);
const plan=profiles[profile],totalSeconds=plan.stages.reduce((n,s)=>n+seconds(s.duration),0),maxPairs=Math.max(...plan.stages.map(s=>s.target));
const historyMax={smoke:4,baseline:10,stress:40,spike:20,soak:20,'profile-write':0,'profile-mixed':40}[profile],requiredPairs=maxPairs+historyMax;
if(pairs.length<requiredPairs)throw Error(`Seed at least ${requiredPairs} pairs before this profile (global VU IDs include HTTP VUs)`);
const sent=new Counter('chat_sent'),received=new Counter('chat_received'),missed=new Counter('chat_missed'),late=new Counter('chat_late'),duplicates=new Counter('chat_duplicates'),errors=new Counter('chat_connection_errors'),delivery=new Trend('chat_delivery',true),deliveryFailed=new Rate('chat_delivery_failed'),persistFailed=new Rate('chat_persistence_failed'),historyErrors=new Counter('chat_history_verification_errors');
export const options={
 summaryTrendStats:['avg','min','med','p(90)','p(95)','p(99)','max'],
 systemTags:['status','method','name','scenario','check','expected_response','error_code','proto'],
 tags:{profile},
 scenarios:{
  // Each VU owns one pair throughout the run. Timers ramp connected pairs
  // while established sockets stay open. Idle allocated VUs are not users.
  chat:{executor:'per-vu-iterations',exec:'chat',vus:maxPairs,iterations:1,maxDuration:(totalSeconds+40)+'s'},
  // Preallocate the mixed diagnostic profile's existing cap to avoid VU creation during measurement.
  ...(plan.http.length?{history:{executor:'ramping-arrival-rate',exec:'history',startRate:1,timeUnit:'1s',preAllocatedVUs:profile==='profile-mixed'?historyMax:4,maxVUs:historyMax,stages:plan.http,gracefulStop:'30s'}}:{})
 },
 thresholds:{
  'http_req_failed{phase:load}':[{threshold:'rate<0.05',abortOnFail:true,delayAbortEval:'45s'}],
  chat_delivery_failed:[{threshold:'rate<0.05',abortOnFail:true,delayAbortEval:'45s'}],
  chat_persistence_failed:['rate==0'],chat_missed:['count==0'],chat_duplicates:['count==0'],chat_received:['count>0'],chat_connection_errors:['count==0'],
  dropped_iterations:['count==0'],
  'iterations{scenario:chat}':[`count==${maxPairs}`]
 }
};
function params(token,name){return{headers:{Authorization:'Bearer '+token},tags:{name,phase:'load'},timeout:'8s',responseType:'text'}}
function grant(account){const r=http.post(base+'/api/ws-ticket',null,params(account.token,'POST /api/ws-ticket'));if(!check(r,{'ticket issued':r=>r.status===200}))return null;try{return r.json('ticket')}catch{return null}}
export function history(){const p=pairs[(exec.vu.idInTest-1)%pairs.length];const responses=http.batch([
 ['GET',base+'/api/chats',null,params(p.sender.token,'GET /api/chats')],
 ['GET',base+`/api/chats/${p.privateChatID}/messages?limit=20`,null,params(p.sender.token,'GET /api/chats/{id}/messages')]
]);for(const r of responses)check(r,{'history HTTP200':r=>r.status===200})}
function targetAt(elapsed){
 let previous=profile==='smoke'?2:0;
 for(const stage of plan.stages){const duration=seconds(stage.duration)*1000;if(elapsed<duration)return Math.floor(previous+(stage.target-previous)*elapsed/duration);elapsed-=duration;previous=stage.target}
 return 0;
}
export function chat(){
 missed.add(0);late.add(0);duplicates.add(0);errors.add(0);
 const p=pairs[exec.vu.idInTest-1],rank=exec.scenario.iterationInTest;
 if(!p)throw Error('Chat VU has no exclusive account pair');
 let session=null,done=false,blocked=false,heartbeat=0;
 const elapsed=()=>Date.now()-exec.scenario.startTime;
 function open(){
  const state={ending:false,pending:new Map(),seen:new Set(),seq:0,lastPrivate:'',started:false};session=state;
  const id=`${Date.now()}-${exec.vu.idInTest}-`,wsParams={headers:{Origin:base},tags:{name:'WS /ws',phase:'load'}};
  const a=grant(p.sender),b=grant(p.receiver);
  async function close(){
   if(state.ending)return;state.ending=true;if(state.tick)clearInterval(state.tick);if(state.handshake)clearTimeout(state.handshake);
   // Drain the outstanding deliveries before checking history and closing.
   setTimeout(async()=>{
    for(const _ of state.pending){missed.add(1);deliveryFailed.add(true)}state.pending.clear();
    if(state.lastPrivate){try{
     const r=await http.asyncRequest('GET',base+`/api/chats/${p.privateChatID}/messages?limit=50`,null,params(p.sender.token,'GET /api/chats/{id}/messages'));
     if(r.status!==200){persistFailed.add(true);historyErrors.add(1,{reason:'http_error'})}
     else{const missing=!(r.json('messages')||[]).some(m=>m.content===state.lastPrivate);persistFailed.add(missing);if(missing)historyErrors.add(1,{reason:'missing_message'})}
    }catch{persistFailed.add(true);historyErrors.add(1,{reason:'request_or_response_error'})}}
    state.sender?.close();state.receiver?.close();if(session===state)session=null;
   },5000);
  }
  state.close=close;
  if(!a||!b){errors.add(1);blocked=true;close();return}
  function ready(){if(state.started||state.ending||state.sender.readyState!==WebSocket.OPEN||state.receiver.readyState!==WebSocket.OPEN)return;state.started=true;clearTimeout(state.handshake);
   state.tick=setInterval(()=>{
    const now=Date.now(),expired=expireDeliveries(state,now);if(expired)missed.add(expired);for(let i=0;i<expired;i++)deliveryFailed.add(true);
    if(state.pending.size>100){errors.add(1);blocked=true;close();return}
    const kind=state.seq++%2?'group':'private',content=id+state.seq;
    if(kind==='private')state.lastPrivate=content;
    state.pending.set(content,now);sent.add(1,{kind});
    try{state.sender.send(JSON.stringify({chat_id:kind==='group'?p.groupChatID:p.privateChatID,content}))}catch{errors.add(1);blocked=true;close()}
   },plan.messageInterval||1000);
  }
  try{
   state.sender=new WebSocket('ws://nginx/ws?ticket='+encodeURIComponent(a),null,wsParams);
   state.receiver=new WebSocket('ws://nginx/ws?ticket='+encodeURIComponent(b),null,wsParams);
  }catch{errors.add(1);blocked=true;close();return}
  state.sender.onopen=ready;state.receiver.onopen=ready;
  state.handshake=setTimeout(()=>{if(!state.started&&!state.ending){errors.add(1);blocked=true;close()}},5000);
  state.receiver.onmessage=e=>{for(const line of String(e.data).split('\n')){let m;try{m=JSON.parse(line)}catch{continue}if(typeof m.content!=='string'||!m.content.startsWith(id))continue;
   const result=receiveDelivery(state,m.content,Date.now());
   if(result.kind==='duplicate'){duplicates.add(1);continue}
   if(result.latency!==undefined)delivery.add(result.latency,{kind:m.chat_id===p.privateChatID?'private':'group'});
   if(result.kind==='late'){late.add(1);if(result.newlyMissed){missed.add(1);deliveryFailed.add(true)}continue}
   deliveryFailed.add(false);received.add(1);
  }};
  for(const socket of [state.sender,state.receiver]){
   socket.onerror=()=>{errors.add(1);blocked=true;close()};socket.onclose=()=>{if(!state.ending){errors.add(1);blocked=true;close()}};
  }
 }
 const clock=setInterval(()=>{
  if(done)return;
  // Unchanged zero counters otherwise disappear from Prometheus after its
  // lookback window, making a healthy long run display "No data".
  if(rank===0&&Date.now()-heartbeat>=1000){heartbeat=Date.now();missed.add(0);late.add(0);duplicates.add(0);errors.add(0)}
  const shouldConnect=rank<targetAt(elapsed());
  if(!shouldConnect){blocked=false;session?.close()}
  else if(!session&&!blocked)open();
 },250);
 setTimeout(()=>{done=true;clearInterval(clock);session?.close()},Math.max(1,totalSeconds*1000-elapsed()));
}
export function handleSummary(data){return{'/results/summary.json':JSON.stringify(data,null,2),'stdout':JSON.stringify({profile,thresholds:data.metrics?Object.fromEntries(Object.entries(data.metrics).filter(([,v])=>v.thresholds).map(([k,v])=>[k,v.thresholds])):{}})+'\n'}}
