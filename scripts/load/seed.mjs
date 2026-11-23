// Synthetic fixtures only. Invoked inside the lab network; never print tokens.
import { randomBytes } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
const base='http://nginx', count=Number(process.env.LOAD_PAIRS||'10');
if(!Number.isInteger(count)||count<1||count>500)throw Error('LOAD_PAIRS must be 1..500');
const stamp=randomBytes(5).toString('hex'),password=randomBytes(24).toString('hex'),pairs=[];
async function api(method,path,token,body){
 for(let attempt=0;attempt<15;attempt++){
  const r=await fetch(base+path,{method,headers:{...(token?{Authorization:'Bearer '+token}:{}),...(body?{'Content-Type':'application/json'}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(15000)});
  if(r.status===429){await sleep(1000);continue}
  if(!r.ok)throw Error(`${method} ${path.split('?')[0]}: HTTP${r.status}`);
  return r.json();
 }throw Error('Authentication throttle did not recover');
}
async function account(label){const email=`load_${stamp}_${label}@example.invalid`,username=`load_${stamp}_${label}`;const auth=await api('POST','/api/register',null,{email,username,password});const profile=await api('GET','/api/profile',auth.token);return{email,username,token:auth.token,id:Number(profile.id)}}
let group;
for(let i=0;i<count;i++){
 const sender=await account('s'+i),receiver=await account('r'+i);
 const request=await api('POST','/api/friends/request',sender.token,{to_user_id:receiver.id});
 const privateChat=await api('POST',`/api/friends/requests/${request.id}/accept`,receiver.token);
 if(i%5===0)group=(await api('POST','/api/groups',sender.token,{name:`Load ${stamp} ${i}`,is_public:true})).group;
 else await api('POST',`/api/groups/invite/${group.invite_code}/join`,sender.token);
 await api('POST',`/api/groups/invite/${group.invite_code}/join`,receiver.token);
 pairs.push({sender,receiver,privateChatID:Number(privateChat.id),groupChatID:Number(group.id)});
}
writeFileSync('/data/accounts.json',JSON.stringify({schema:1,password,createdAt:new Date().toISOString(),pairs},null,2)+'\n',{mode:0o600});
writeFileSync('/data/manual-accounts.txt',`Sender: ${pairs[0].sender.email}\nReceiver: ${pairs[0].receiver.email}\nPassword: ${password}\nServer: http://localhost:38080\n`,{mode:0o600});
console.log(`Prepared ${count} private pairs and ${Math.ceil(count/5)} groups; credentials saved only in private lab output.`);
