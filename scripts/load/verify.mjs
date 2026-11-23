// Integration check on the owned lab only. No tokens/passwords in reports.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync} from 'node:fs';
import {setTimeout as sleep} from 'node:timers/promises';
const fixture=JSON.parse(readFileSync('/data/accounts.json','utf8')),pair=fixture.pairs[0];
const base='http://nginx', backend='http://server:8080',report={at:new Date().toISOString(),checks:[]},sockets=[];
const pass=name=>report.checks.push(name);
async function request(method,path,token,body){const multipart=body instanceof FormData;const r=await fetch(base+path,{method,headers:{...(token?{Authorization:'Bearer '+token}:{}),...(body&&!multipart?{'Content-Type':'application/json'}:{})},body:body?(multipart?body:JSON.stringify(body)):undefined,signal:AbortSignal.timeout(10000)});assert.equal(r.status,200,method+' '+path.split('?')[0]);return r}
const wait=async fn=>{for(let i=0;i<50;i++){const result=await fn();if(result)return result;await sleep(200)}throw Error('Timed out waiting for lab condition')};
async function metrics(){const r=await fetch(backend+'/metrics');assert.equal(r.status,200);return r.text()}
function value(raw,name,filter=()=>true){return raw.split('\n').filter(s=>s.startsWith(name+'{')||s.startsWith(name+' ')).filter(filter).reduce((n,s)=>n+Number(s.slice(s.lastIndexOf(' ')+1)),0)}
async function peer(account){const ticket=await(await request('POST','/api/ws-ticket',account.token)).json(),ws=new WebSocket('ws://nginx/ws?ticket='+ticket.ticket),messages=[];sockets.push(ws);ws.onmessage=e=>{for(const line of String(e.data).split('\n')){try{messages.push(JSON.parse(line))}catch{}}};await new Promise((r,j)=>{const t=setTimeout(()=>j(Error('Handshake failed')),5000);ws.onopen=()=>{clearTimeout(t);r()};ws.onerror=()=>{clearTimeout(t);j(Error('Handshake failed'))}});return{ws,messages}}
try{
 const before=await metrics(),started=value(before,'process_start_time_seconds'),active=value(before,'websocket_connections_active');
 const a=await peer(pair.sender),b=await peer(pair.receiver),marker='verify-'+Date.now();
 assert.equal(value(await metrics(),'websocket_connections_active'),active+2);
 for(const chatID of [pair.privateChatID,pair.groupChatID]){const content=marker+'-'+chatID;a.ws.send(JSON.stringify({chat_id:chatID,content}));await wait(()=>b.messages.some(m=>m.content===content));const history=await(await request('GET',`/api/chats/${chatID}/messages?limit=50`,pair.receiver.token)).json();assert.ok(history.messages.some(m=>m.content===content))}
 pass('independent private/group WS delivery and persisted history');
 a.ws.send('invalid-json');a.ws.send(JSON.stringify({chat_id:pair.privateChatID,content:''}));
 try{
  await request('PUT',`/api/groups/${pair.groupChatID}/permissions`,pair.sender.token,{admins_only_write:true});
  b.ws.send(JSON.stringify({chat_id:pair.groupChatID,content:marker+'-forbidden'}));await wait(()=>b.messages.some(m=>m.type==='error'));
 }finally{await request('PUT',`/api/groups/${pair.groupChatID}/permissions`,pair.sender.token,{admins_only_write:false})}
 const file=Buffer.from('metrics-file-'+Date.now()),form=new FormData();form.set('chat_id',String(pair.privateChatID));form.set('file',new Blob([file],{type:'text/plain'}),'metrics.txt');
 const upload=await(await request('POST','/api/files/upload',pair.sender.token,form)).json();
 const path=upload.url||upload.file_url;assert.ok(path,'Upload response contains URL');
 const download=await request('GET',path,pair.sender.token);assert.deepEqual(Buffer.from(await download.arrayBuffer()),file);
 pass('authenticated file upload/download preserves bytes');
 for(const s of sockets)s.close();await wait(async()=>value(await metrics(),'websocket_connections_active')===active);
 const raw=await metrics();assert.equal(value(raw,'process_start_time_seconds'),started);
 for(const name of ['go_goroutines','process_cpu_seconds_total','http_requests_total','http_request_duration_seconds_count','go_sql_max_open_connections','grpc_server_handled_total','grpc_server_handling_seconds_count','websocket_handshake_seconds_count','websocket_frames_received_total','websocket_message_processing_seconds_count','zchat_call_reconcile_last_success_timestamp_seconds'])assert.ok(value(raw,name)>0,name+' exported and sampled');
 assert.equal(value(raw,'go_sql_max_open_connections'),25);
 for(const result of ['saved','invalid','forbidden'])assert.ok(value(raw,'websocket_message_results_total',line=>line.includes(`result="${result}"`))>0,'message outcome '+result);
 assert.ok(value(raw,'websocket_frames_received_total',line=>line.includes('kind="invalid_json"'))>0);
 for(const account of [pair.sender,pair.receiver]){assert.ok(!raw.includes(account.token));assert.ok(!raw.includes(account.email))}
 assert.ok(!raw.includes(fixture.password));pass('live HTTP/WS/gRPC/Go/DB metrics, bounded labels and no fixture credentials');
 report.processStartTime=started;
 if(!process.argv.includes('--without-prometheus')){
  const auth='Basic '+Buffer.from('admin:'+process.env.GRAFANA_ADMIN_PASSWORD).toString('base64');
  async function grafana(path){const r=await fetch('http://grafana:3000'+path,{headers:{Authorization:auth}});assert.equal(r.status,200);return r.json()}
  async function query(expr){const r=await fetch('http://prometheus:9090/api/v1/query?query='+encodeURIComponent(expr));const q=await r.json();assert.equal(q.status,'success',q.error||'PromQL');return q.data.result}
  const health=await grafana('/api/datasources/uid/zchat-prometheus/health');assert.equal(health.status,'OK');
  await wait(async()=>{const values=await query('up{job=~"zchat-backend|zchat-postgres|zchat-livekit|zchat-lab-resources"}');return values.length===4&&values.every(v=>v.value[1]==='1')});
  let expressions=0;report.emptyPanels=[];
  for(const uid of ['zchat-overview','zchat-calls','zchat-load']){
   const dashboard=await grafana('/api/dashboards/uid/'+uid);assert.ok(dashboard.dashboard.panels.length>5);
   for(const panel of dashboard.dashboard.panels)for(const target of panel.targets||[]){const result=await query(target.expr.replaceAll('$__rate_interval','30s').replaceAll('$__interval','5s'));expressions++;if(!result.length)report.emptyPanels.push(panel.title)}
  }
  report.promqlExpressions=expressions;pass('Grafana datasource, three provisioned dashboards and all PromQL expressions');
  // New label combinations are exported on use; allow a complete scrape after
  // this check generated traffic, including when Prometheus just restarted.
  for(const name of ['http_requests_total','grpc_server_handled_total','go_sql_max_open_connections','websocket_message_results_total','pg_stat_database_xact_commit','livekit_room_total'])await wait(async()=>(await query(name)).length>0);
  pass('Prometheus stores actual application/PostgreSQL/SFU samples');
 }
 report.status='PASS';
}catch(e){report.status='FAIL';report.error=e.message;process.exitCode=1}finally{
 for(const s of sockets)s.close();
 writeFileSync('/data/monitoring-check'+(process.argv.includes('--without-prometheus')?'-without-prometheus':'')+'.json',JSON.stringify(report,null,2)+'\n',{mode:0o600});console.log(JSON.stringify(report));
}
