// Short isolated SELECT workloads; use the same authenticated public API.
import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import exec from 'k6/execution';
const pairs=new SharedArray('profile pairs',()=>JSON.parse(open('/data/accounts.json')).pairs);
const profile=__ENV.LOAD_PROFILE;
if(!['profile-read-chats','profile-read-history'].includes(profile))throw Error('Unknown read profile');
export const options={
 systemTags:['status','method','name','scenario','check','expected_response','error_code','proto'],
 tags:{profile},summaryTrendStats:['avg','min','med','p(95)','p(99)','max'],
 scenarios:{read:{executor:'constant-arrival-rate',rate:100,timeUnit:'1s',duration:'45s',preAllocatedVUs:40,maxVUs:40,gracefulStop:'10s'}},
 thresholds:{http_req_failed:[{threshold:'rate<0.05',abortOnFail:true,delayAbortEval:'20s'}],dropped_iterations:['count==0']}
};
export default function(){
 const p=pairs[(exec.vu.idInTest-1)%pairs.length];
 const chats=profile==='profile-read-chats';
 const r=http.get('http://nginx'+(chats?'/api/chats':`/api/chats/${p.privateChatID}/messages?limit=20`),{
  headers:{Authorization:'Bearer '+p.sender.token},timeout:'8s',responseType:'none',tags:{name:chats?'GET /api/chats':'GET /api/chats/{id}/messages',phase:'load'}
 });check(r,{'read HTTP200':r=>r.status===200});
}
export function handleSummary(data){return{'/results/summary.json':JSON.stringify(data,null,2),'stdout':JSON.stringify({profile,thresholds:Object.fromEntries(Object.entries(data.metrics).filter(([,v])=>v.thresholds).map(([k,v])=>[k,v.thresholds]))})+'\n'}}
