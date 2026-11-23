import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import { webcrypto } from 'node:crypto';
import ts from 'typescript';
const compile = name => ts.transpile(readFileSync(new URL('../web/src/chat/' + name, import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const archiveCode = compile('025-e2ee-archive.ts');
const sdkCode = readFileSync(new URL('../desktop/renderer/desktop-runtime.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../web/chat.html', import.meta.url), 'utf8');
const id = n => '00000000-0000-4000-8000-' + n.toString(16).padStart(12, '0');
const GEN = id(10), OLD = id(11), CHAT = id(20), PEER = id(21);
const generation = (generationId = GEN, ready = true) => ({ generationId, replacedByGenerationId: generationId === GEN ? null : GEN, historyReady: ready });
const conversation = (conversationId = CHAT) => ({ conversationId, peerUserId: PEER, state: 'active', closeReason: null, closedAtUnixMs: '0' });
const message = n => ({ messageId: id(1000 + n), conversationId: CHAT, cryptoEpoch: n % 2 ? '1' : '0', createdAtUnixMs: String(1000 + n), senderUserId: PEER, text: 'archive message ' + n, outgoing: false, delivery: null });
const flush = async () => { for (let i = 0; i < 4; i++) await new Promise(resolve => setImmediate(resolve)); };
const deferred = () => { let resolve, reject; const promise = new Promise((a,b) => { resolve=a;reject=b; }); return { promise,resolve,reject }; };
class Element {
  constructor(tag='div') { this.tagName=tag.toUpperCase(); }
  children=[];dataset={};style={};hidden=false;disabled=false;open=false;value='';checked=false;readOnly=false;scrollTop=0;clientHeight=100;listeners=new Map();attrs={};parentElement=null;className='';
  classList={ contains:()=>false,remove(){} };
  set textContent(v) {this.replaceChildren();this.text=String(v);} get textContent(){return this.text||this.children.map(v=>v.textContent).join('');}
  set innerHTML(v){throw Error('archive must not interpret HTML: '+v);}
  append(...items){for(const item of items){item.parentElement=this;this.children.push(item);}}
  appendChild(item){this.append(item);if(item.tagName==='SCRIPT') this.boot?.();return item;}
  prepend(item){item.parentElement=this;this.children.unshift(item);}
  replaceChildren(...items){for(const n of this.children)n.parentElement=null;this.children=[];this.text='';this.append(...items);}
  removeChild(item){this.children=this.children.filter(v=>v!==item);item.parentElement=null;}
  get lastElementChild(){return this.children.at(-1);}get scrollHeight(){return this.children.length*20;}
  get isConnected(){return this.parentElement !== null;}
  setAttribute(k,v){this.attrs[k]=v;}getAttribute(k){return this.attrs[k]??null;}
  querySelectorAll(selector){return this.children.filter(v=>selector==='button'&&v.tagName==='BUTTON');}
  contains(node){return node===this||this.children.some(v=>v.contains(node));}
  addEventListener(type,fn){const group=this.listeners.get(type)||[];group.push(fn);this.listeners.set(type,group);}
  focus(){this.focused=true;}
  showModal(){this.open=true;}close(){this.open=false;this.closeEvents=(this.closeEvents||0)+1;}
  async emit(type,event={}){for(const fn of this.listeners.get(type)||[]) fn(event);await flush();}
  async click(){if(!this.hidden&&!this.disabled)await this.emit('click');}
}
async function fixture(options={}){
  const nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element()]));
  const byId=name=>{assert.ok(nodes.has(name),'actual HTML missing '+name);return nodes.get(name);};
  for(const name of ['e2eeArchiveOpen','desktopE2eeArchiveOpen'])byId(name).tagName='BUTTON';
  const events=[],calls=[],writes=[],timers=new Map();let session='session',serial=0,cursorSerial=10000,active=0,maxActive=0;
  let state={context:2,phase:options.readonly?'device_unavailable':'active',runtimePhase:options.readonly?'ArchiveReadOnly':'Running'};
  const rows=options.messages||Array.from({length:20},(_,n)=>message(n));
  rows.sort((a,b)=>BigInt(a.cryptoEpoch)>BigInt(b.cryptoEpoch)?-1:BigInt(a.cryptoEpoch)<BigInt(b.cryptoEpoch)?1:Number(BigInt(b.createdAtUnixMs)-BigInt(a.createdAtUnixMs)));
  const generations=options.generations||[generation(),generation(OLD)];
  const conversations=options.conversations||[conversation()];
  const segments=[{cryptoEpoch:'1',sealed:false,createdAtUnixMs:'1001',sealedAtUnixMs:null},{cryptoEpoch:'0',sealed:true,createdAtUnixMs:'1000',sealedAtUnixMs:'1001'}];
  const positions=new Map();
  const readPage=(kind,target,cursor)=>{
    let all=kind==='archives'?generations:kind==='archive_dialogs'?conversations:kind==='segments'?segments:rows.filter(v=>target.cryptoEpoch===null||target.cryptoEpoch===v.cryptoEpoch);
    const offset=cursor?positions.get(cursor):0;assert.ok(Number.isInteger(offset),'fixture cursor was never issued');
    const items=all.slice(offset,offset+8);let nextCursor=null;
    if(offset+8<all.length){nextCursor=id(++cursorSerial);positions.set(nextCursor,offset+8);}
    return {context:state.context,page:{kind,...target,items,nextCursor}};
  };
  const read=async(kind,target,expected,cursor)=>{
    calls.push([kind,structuredClone(target),expected,cursor]);active++;maxActive=Math.max(active,maxActive);
    try{return options.read?await options.read(kind,target,expected,cursor,()=>readPage(kind,target,cursor)):readPage(kind,target,cursor);}finally{active--;}
  };
  const api={compiled:options.compiled!==false,
    status:async()=>{calls.push(['status']);return options.status?options.status(state):{...state};},
    listArchives:(ctx,cursor)=>read('archives',{},ctx,cursor),
    archiveConversations:(target,ctx,cursor)=>read('archive_dialogs',target,ctx,cursor),
    archiveSegments:(target,ctx,cursor)=>read('segments',target,ctx,cursor),
    archiveHistory:(target,ctx,cursor)=>read('archive_history',target,ctx,cursor),
    history:async chat=>({context:2,page:{kind:'history',items:[{...message(5),conversationId:chat}],nextCursor:null}}),
    recoveryStatus:async chat=>({context:2,recovery:{kind:'state',conversationId:chat,cryptoEpoch:'0',receiveOnly:false,sendingFrozen:false,proposals:[]}}),
    listConversations:async()=>({context:state.context,page:{kind:'dialogs',items:[],nextCursor:null}}),
  };
  for(const method of ['setupDevice','retry','replaceDevice','proposeRecovery','decideRecovery','sendText','messageStatuses','getConversation','endSession','downloadAttachment'])api[method]=()=>{calls.push(['forbidden',method]);throw Error('unexpected archive mutation or transport: '+method);};
  if(options.oldSDK)delete api.listArchives;
  const listen=(type,fn,capture=false)=>events.push({type,fn,capture});
  const data=new Map([['token',session],['zchat_desktop_server','http://127.0.0.1:18080']]);
  const storage={get length(){return data.size;},key:i=>[...data.keys()][i],getItem:k=>k==='token'?session:data.get(k)??null,setItem:(k,v)=>{writes.push([k,v]);if(k==='token')session=v;else data.set(k,v);},removeItem:k=>{if(k==='token')session=null;data.delete(k);},clear:()=>{session=null;data.clear();}};
  const doc={visibilityState:'visible',getElementById:name=>nodes.get(name)??null,addEventListener:listen,createElement:tag=>new Element(tag),querySelectorAll:()=>[],querySelector:()=>null,body:new Element('body'),currentScript:null};
  const ctx={TextEncoder,TextDecoder,AbortController,DOMException,URL,Request,Response,Headers,Event,EventTarget,MessageEvent,Blob,File,ArrayBuffer,Uint8Array,DataView,Element,
    structuredClone,crypto:webcrypto,performance:{now:()=>0},token:'session',authExpiredHandled:false,localStorage:storage,sessionStorage:{clear(){}},document:doc,byId,optionalById:byId,
    currentChatID:17,currentChatType:'private',chatViewVersion:4,chatHistoryRequests:new Map(),clearTypingDisplay(){},closeInfoPanel(){},
    messagesEl:byId('messages'),chatNameEl:byId('chatName'),chatStatusEl:byId('chatStatus'),messageInput:byId('messageInput'),
    location:new URL('http://tauri.localhost/chat.html'),navigator:{},
    setTimeout:(fn,delay)=>{timers.set(++serial,{fn,delay});return serial;},clearTimeout:n=>timers.delete(n),
    addEventListener:listen,apiFetch:async()=>{calls.push(['http']);return {ok:true,json:async()=>[]};},profilePeerUUID:()=>true,
  };
  ctx.window=ctx;
  ctx.zchatDesktop=options.browser?undefined:{e2ee:api};
  if(options.realSDK){
    let boot;const ready=new Promise(r=>{boot=r;});doc.body.boot=boot;
    ctx.fetch=async()=>{throw Error('unexpected HTTP in archive');};let nativeContext=1;
    ctx.__TAURI__={core:{Channel:class{},invoke:async(name,args)=>{
      calls.push(['invoke',name]);
      if(name==='get_config')return {serverUrl:'http://127.0.0.1:18080',epoch:7,e2eeCompiled:true,e2eeContext:1};
      if(name==='e2ee_session')return {context:++nativeContext,phase:'starting',replyLease:'1'};
      if(name==='e2ee_status')return {...state,context:nativeContext,replyLease:'2'};
      if(name==='e2ee_read'){const{kind,cursor,...target}=args.query;return {...await read(kind,target,args.context,cursor),replyLease:'3'};}
      if(['e2ee_ack','set_session','check_server'].includes(name))return;
      throw Error('unexpected native command '+name);
    }}};
    runInNewContext(sdkCode,ctx);await ready;assert.ok(ctx.zchatDesktop?.e2ee?.archiveHistory);
  }
  let code='';
  if(options.integrated)code+=['025-e2ee-view.ts','025-e2ee-composer.ts','025-e2ee-recovery.ts'].map(compile).join('\n');
  else ctx.e2eeHistoryView=null;
  if(options.discovery)code+=compile('025-e2ee-discovery.ts');
  code+=archiveCode;
  if(options.escape){Object.assign(ctx,{userProfileRequest:null,notifPanelOpen:false,infoPanelOpen:false});code+=compile('026-escape-key-close-all-modals.ts');}
  if(options.settings)code+=compile('033-e2ee-settings.ts');
  if(options.apiHelper){Object.assign(ctx,{stopChatConnection(){},SETTINGS_CACHE_KEY:'settings-only',NOTIFICATIONS_STORAGE_KEY:'notifications-only'});code+=compile('004-api-helper.ts');}
  code+='\nthis.archiveState=()=>e2eeArchiveView;this.archivePending=()=>e2eeArchiveBusy;';
  if(options.integrated)code+='this.currentState=()=>({view:e2eeHistoryView,drafts:e2eeDrafts});';
  runInNewContext(code,ctx);
  const emit=async(type,extra={})=>{const e={key:'',preventDefault(){this.prevented=true;},stopImmediatePropagation(){this.stopped=true;},...extra};for(const item of [...events.filter(v=>v.type===type&&v.capture),...events.filter(v=>v.type===type&&!v.capture)]){if(e.stopped)break;item.fn(e);}await flush();return e;};
  return {ctx,byId,calls,writes,timers,emit,state:()=>state,setState:value=>{state=value;},setToken:value=>{session=value;},maxActive:()=>maxActive,
    open:async()=>{await byId('e2eeArchiveOpen').click();await flush();},
    pick:async(n=0)=>{await byId('e2eeArchiveContent').children[n].click();await flush();},
    history:async()=>{await byId('e2eeArchiveOpen').click();await flush();await byId('e2eeArchiveContent').children[0].click();await flush();await byId('e2eeArchiveContent').children[0].click();await flush();},
  };
}
const kind=(f,name)=>f.calls.filter(v=>v[0]===name);

test('browser/default/old SDK hide both archive entrypoints and perform no reads',async()=>{
  for(const opts of [{browser:true},{compiled:false},{oldSDK:true}]){const f=await fixture(opts);assert.equal(f.byId('e2eeArchiveOpen').hidden,true);assert.equal(f.byId('desktopE2eeArchiveOpen').hidden,true);await f.ctx.openE2eeArchive();assert.equal(f.calls.length,0);assert.equal(f.timers.size,0);}
});
test('readonly revoked installation opens independently from Active sidebar and reads saved generations/dialogs/text',async()=>{
  const f=await fixture({readonly:true});await f.byId('desktopE2eeArchiveOpen').click();await flush();assert.equal(f.byId('e2eeArchiveDialog').open,true);assert.equal(f.ctx.archiveState().context,2);
  await f.pick(1);await f.pick();assert.equal(f.ctx.archiveState().stage,'history');
  assert.deepEqual(JSON.parse(JSON.stringify(kind(f,'archive_history')[0].slice(1))),[{generationId:OLD,conversationId:CHAT,cryptoEpoch:null},2,null]);
  assert.equal(f.byId('e2eeArchiveContent').children.length,8);assert.equal(f.writes.length,0);assert.equal(f.timers.size,0);assert.equal(kind(f,'forbidden').length,0);assert.equal(kind(f,'http').length,0);
});
test('viewer with actual SDK12 uses exact scope, real cursor provenance and no native mutation commands',async()=>{
  const f=await fixture({readonly:true,realSDK:true});await f.history();assert.equal(f.ctx.archiveState().count,8);
  await f.byId('e2eeArchiveMore').click();assert.equal(f.ctx.archiveState().count,16);
  assert.ok(f.calls.filter(v=>v[0]==='invoke').every(v=>['get_config','e2ee_session','e2ee_status','e2ee_read','e2ee_ack','set_session','check_server'].includes(v[1])));
});
test('metadata pages retain at most8rows and explicit refresh returns to the first page',async()=>{
  const f=await fixture({generations:Array.from({length:19},(_,n)=>generation(id(100+n)))});await f.open();assert.equal(f.byId('e2eeArchiveContent').children.length,8);
  await f.byId('e2eeArchiveMore').click();assert.equal(f.byId('e2eeArchiveContent').children.length,8);await f.byId('e2eeArchiveMore').click();assert.equal(f.byId('e2eeArchiveContent').children.length,3);
  assert.equal(f.byId('e2eeArchiveMore').hidden,true);await f.byId('e2eeArchiveRefresh').click();assert.equal(f.byId('e2eeArchiveContent').children.length,8);assert.equal(kind(f,'archives').at(-1)[3],null);
});
test('history window is bounded128 and prepending old pages preserves the scroll anchor after trimming newest rows',async()=>{
  const f=await fixture({messages:Array.from({length:152},(_,n)=>({...message(n),cryptoEpoch:'0'}))});await f.history();
  for(let i=0;i<15;i++)await f.byId('e2eeArchiveMore').click();const content=f.byId('e2eeArchiveContent');assert.equal(content.children.length,128);content.scrollTop=30;
  const kept=content.children[0];await f.byId('e2eeArchiveMore').click();assert.equal(content.children.length,128);assert.equal(f.ctx.archiveState().seen.size,128);assert.equal(content.children[8],kept);assert.equal(content.scrollTop,190);
  assert.match(f.byId('e2eeArchiveCount').textContent,/128/);assert.equal(kind(f,'archive_history').length,17);assert.equal(f.timers.size,0);
});
test('session picker filters the exact decimal epoch and All sessions explicitly restores null selection',async()=>{
  const f=await fixture();await f.history();await f.byId('e2eeArchiveSegments').click();assert.equal(f.ctx.archiveState().stage,'segments');await f.pick(1);
  assert.equal(kind(f,'archive_history').at(-1)[1].cryptoEpoch,'0');assert.ok(f.byId('e2eeArchiveContent').children.every(v=>v.children[2].textContent.includes('Сессия №0')));
  await f.byId('e2eeArchiveAll').click();assert.equal(kind(f,'archive_history').at(-1)[1].cryptoEpoch,null);
});
test('unprepared index shows unavailable history without setup/repair or a history request',async()=>{
  const f=await fixture({generations:[generation(GEN,false)]});await f.open();await f.pick();assert.match(f.byId('e2eeArchiveNotice').textContent,/Индекс.*не подготовлен/);
  assert.equal(f.byId('e2eeArchiveContent').children[0].disabled,true);await f.pick();assert.equal(kind(f,'archive_history').length,0);assert.equal(kind(f,'forbidden').length,0);
});
test('archive text, labels and old delivery status are literal and never offer a send/retry action',async()=>{
  const text='<img src=x onerror=alert(1)>\n<script>steal()</script>';
  const f=await fixture({messages:[{...message(1),text,outgoing:true,delivery:{storedOnServer:false,state:null,removal:null,sealedUnsent:false},cryptoEpoch:'9007199254740993',createdAtUnixMs:'9223372036854775807'}]});await f.history();
  const row=f.byId('e2eeArchiveContent').children[0];assert.equal(row.children[1].textContent,text);assert.match(row.children[2].textContent,/Сохранённый статус: Сервер не подтвердил отправку/);assert.match(row.children[2].textContent,/9007199254740993/);assert.match(row.children[2].textContent,/Время недоступно/);assert.equal(row.tagName,'ARTICLE');assert.equal(kind(f,'forbidden').length,0);
});
test('archived attachments show local availability without offering a network download', async () => {
  const attachment = { kind: 'photo', filename: 'archive.png', size: 10, caption: '', mimeType: 'image/png', photoMetadataStripped: true };
  const f = await fixture({ readonly: true, integrated: true, messages: [
    { ...message(0), attachment, cacheState: 'missing' }, { ...message(1), attachment, cacheState: 'saved' },
  ] });
  await f.history();
  const rows = f.byId('e2eeArchiveContent').children;
  assert.equal(rows.length, 2);
  assert.ok(rows.every(row => row.children.length === 3));
  assert.match(rows[0].children[2].textContent, /отсутствует на этом устройстве/);
  assert.match(rows[1].children[2].textContent, /Сохранено на этом устройстве/);
  assert.deepEqual(kind(f, 'forbidden'), []);
});
test('archived export carries its exact generation and closes with the archive', async () => {
  const attachment = { kind: 'file', filename: 'saved.txt', size: 4, caption: '', mimeType: 'application/octet-stream', photoMetadataStripped: null };
  const saved = { ...message(0), attachment, cacheState: 'saved' };
  const f = await fixture({ readonly: true, integrated: true, messages: [saved] });
  const gate = deferred();
  let scope, signal;
  f.ctx.zchatDesktop.e2ee.previewAttachment = () => assert.fail('file should only offer export');
  f.ctx.zchatDesktop.e2ee.exportAttachment = (target, context, name, active) => {
    scope = { ...target, context, name }; signal = active;
    return gate.promise;
  };
  await f.history();
  const [button, notice] = f.byId('e2eeArchiveContent').children[0].children[3].children;
  assert.equal(button.textContent, 'Сохранить копию…');
  await button.click();
  assert.deepEqual(scope, { generationId: GEN, conversationId: CHAT, messageId: saved.messageId, context: 2, name: 'saved.txt' });
  await f.byId('e2eeArchiveClose').click();
  assert.equal(signal.aborted, true);
  gate.resolve({ context: 2, target: scope, result: { kind: 'exported' } });
  await flush();
  assert.notEqual(notice.textContent, 'Копия сохранена.');
  assert.deepEqual(kind(f, 'forbidden'), []);
});
test('closing archive clears plaintext and late reads cannot reopen it or overwrite a new dialog',async()=>{
  const gate=deferred();const f=await fixture({read:(kind,_t,_c,_p,normal)=>kind==='archive_history'?gate.promise:normal()});
  await f.history();assert.equal(f.ctx.archivePending(),true);await f.byId('e2eeArchiveClose').click();assert.equal(f.ctx.archiveState(),null);assert.equal(f.byId('e2eeArchiveContent').children.length,0);
  await f.ctx.openE2eeArchive();assert.equal(kind(f,'status').length,1);gate.resolve({context:2,page:{kind:'archive_history',items:[message(1)],nextCursor:null}});await flush();
  assert.equal(f.byId('e2eeArchiveDialog').open,false);assert.equal(f.byId('e2eeArchiveContent').children.length,0);await f.open();assert.equal(kind(f,'status').length,2);
});
test('repeated clicks keep a single archive read in flight',async()=>{
  const gate=deferred();const f=await fixture({read:(kind,_t,_c,_p,normal)=>kind==='archives'?gate.promise:normal()});await f.open();
  for(let n=0;n<10;n++){await f.ctx.openE2eeArchive();await f.byId('e2eeArchiveRefresh').click();}
  assert.equal(kind(f,'archives').length,1);assert.equal(f.maxActive(),1);gate.resolve({context:2,page:{kind:'archives',items:[],nextCursor:null}});await flush();assert.equal(f.ctx.archivePending(),false);
});
test('observer context change clears plaintext and requires close/reopen instead of automatic rebinding',async()=>{
  const f=await fixture();await f.history();f.ctx.observeE2eeArchiveStatus({context:3,phase:'active',runtimePhase:'Running'});
  assert.equal(f.byId('e2eeArchiveContent').children.length,0);assert.equal(f.ctx.archiveState().context,2);assert.equal(f.ctx.archiveState().retired,true);assert.equal(f.byId('e2eeArchiveRefresh').disabled,true);assert.match(f.byId('e2eeArchiveNotice').textContent,/откройте.*заново/);
  f.setState({context:3,phase:'active',runtimePhase:'Running'});await f.byId('e2eeArchiveClose').click();await f.open();assert.equal(f.ctx.archiveState().context,3);
});
test('late status from a previous owner cannot initialize an archive after a newer context was observed',async()=>{
  const gate=deferred();const f=await fixture({status:()=>gate.promise});await f.open();f.ctx.observeE2eeArchiveStatus({context:3,phase:'active',runtimePhase:'Running'});gate.resolve({context:2,phase:'active',runtimePhase:'Running'});await flush();assert.equal(kind(f,'archives').length,0);assert.equal(f.ctx.archiveState().retired,true);
});
test('storage/page retirement closes and clears the archive without modifying account storage',async()=>{
  for(const action of ['storage','pagehide']){const f=await fixture();await f.history();if(action==='storage')f.setToken('another-account');await f.emit(action,{key:'token'});assert.equal(f.ctx.archiveState(),null);assert.equal(f.byId('e2eeArchiveContent').children.length,0);assert.equal(f.writes.length,0);}
});
test('hidden page drops a pending history result and does not refresh automatically on return',async()=>{
  const gate=deferred();const f=await fixture({read:(kind,_t,_c,_p,normal)=>kind==='archive_history'?gate.promise:normal()});await f.history();f.ctx.document.visibilityState='hidden';await f.emit('visibilitychange');gate.resolve({context:2,page:{kind:'archive_history',items:[message(1)],nextCursor:null}});await flush();
  assert.equal(f.byId('e2eeArchiveContent').children.length,0);f.ctx.document.visibilityState='visible';await f.emit('visibilitychange');assert.equal(kind(f,'archive_history').length,1);assert.match(f.byId('e2eeArchiveNotice').textContent,/приостановлено/);
});
test('expired cursor keeps a readonly fragment and requires explicit first-page refresh in the same context',async()=>{
  const f=await fixture({read:(kind,_t,_c,cursor,normal)=>{if(kind==='archive_history'&&cursor)throw new DOMException('expired','InvalidStateError');return normal();}});await f.history();await f.byId('e2eeArchiveMore').click();
  assert.equal(f.byId('e2eeArchiveContent').children.length,8);assert.equal(f.byId('e2eeArchiveMore').disabled,true);assert.match(f.byId('e2eeArchiveNotice').textContent,/устарела/);
  assert.equal(kind(f,'archive_history').length,2);await f.byId('e2eeArchiveRefresh').click();assert.equal(kind(f,'archive_history').at(-1)[3],null);assert.equal(kind(f,'status').length,1);
});
test('native stale is classified by a bounded readonly status observation, never by retry/setup',async()=>{
  const f=await fixture({read:(kind,_t,_c,cursor,normal)=>{if(kind==='archive_history'&&cursor)throw 'stale';return normal();}});await f.history();await f.byId('e2eeArchiveMore').click();assert.equal(kind(f,'status').length,2);assert.equal(f.ctx.archiveState().retired,false);assert.equal(f.byId('e2eeArchiveMore').disabled,true);assert.equal(kind(f,'forbidden').length,0);
});
test('native context mismatch or failed status clears the view without returning other-owner text',async()=>{
  for(const phase of ['context','failed']){
    const f=await fixture({read:(kind,_t,_c,_p,normal)=>{if(kind==='archive_history')throw 'stale';return normal();}});await f.open();await f.pick();f.setState(phase==='context'?{context:3,phase:'active',runtimePhase:'Running'}:{context:2,phase:'failed',runtimePhase:null});await f.pick();assert.equal(f.ctx.archiveState().retired,true);assert.equal(f.byId('e2eeArchiveContent').children.length,0);
  }
});
test('Escape closes only archive and preserves underlying settings/inline edit',async()=>{
  const f=await fixture({escape:true});f.byId('settingsModal').style.display='flex';let edits=0;f.ctx.document.querySelector=()=>({dispatchEvent(){edits++;}});await f.open();const event=await f.emit('keydown',{key:'Escape'});assert.equal(event.prevented,true);assert.equal(event.stopped,true);assert.equal(f.byId('e2eeArchiveDialog').open,false);assert.equal(f.byId('settingsModal').style.display,'flex');assert.equal(edits,0);
});
test('opening/reading/closing archive preserves actual active E2EE draft, uncertain UUID, recovery consent, history and scroll',async()=>{
  const f=await fixture({integrated:true});f.ctx.openE2eeConversation({context:2,conversation:conversation()},'Alice');await flush();f.byId('e2eeTextInput').value='keep uncertain text';f.ctx.updateE2eeDraft();
  const current=f.ctx.currentState();const draft=current.drafts.get('2:'+CHAT);draft.messageId=id(9900);draft.error='unknown commit';f.ctx.renderE2eeComposer();
  current.view.recovery.consent={sentinel:'keep consent'};f.byId('e2eeHistory').scrollTop=37;
  const before={view:current.view,draft,body:f.byId('e2eeTextInput').value,nodes:[...f.byId('e2eeHistory').children],consent:current.view.recovery.consent};
  await f.history();await f.byId('e2eeArchiveClose').click();assert.equal(f.ctx.currentState().view,before.view);assert.equal(f.ctx.currentState().drafts.get('2:'+CHAT),before.draft);assert.equal(f.byId('e2eeTextInput').value,before.body);assert.deepEqual(f.byId('e2eeHistory').children,before.nodes);assert.equal(f.byId('e2eeHistory').scrollTop,37);assert.equal(current.view.recovery.consent,before.consent);assert.equal(f.ctx.currentChatType,'e2ee');assert.equal(kind(f,'forbidden').length,0);
});
test('discovery reuses its existing status cycle to retire archive without a new timer',async()=>{
  const f=await fixture({discovery:true});f.ctx.startE2eeDiscovery();await flush();let task=[...f.timers.entries()][0];f.timers.delete(task[0]);task[1].fn();await flush();await f.history();assert.equal(f.timers.size,1);
  f.setState({context:3,phase:'device_unavailable',runtimePhase:'ArchiveReadOnly'});task=[...f.timers.entries()][0];f.timers.delete(task[0]);task[1].fn();await flush();assert.equal(f.ctx.archiveState().retired,true);assert.equal(f.timers.size,1);
});
test('settings status/action results retire an open archive through the actual settings renderer',async()=>{
  const f=await fixture({settings:true});await f.history();f.ctx.renderE2eeSettings({context:3,phase:'device_unavailable',runtimePhase:'ArchiveReadOnly'});
  assert.equal(f.ctx.archiveState().retired,true);assert.equal(f.byId('e2eeArchiveContent').children.length,0);assert.equal(f.timers.size,0);assert.equal(kind(f,'forbidden').length,0);
});
test('actual auth expiry clears archive immediately before asynchronous native logout completes',async()=>{
  const f=await fixture({integrated:true,apiHelper:true});await f.history();const gate=deferred();f.ctx.zchatDesktop.e2ee.endSession=()=>gate.promise;f.ctx.location={href:'http://tauri.localhost/chat.html'};
  f.ctx.handleAuthExpired();assert.equal(f.ctx.archiveState(),null);assert.equal(f.byId('e2eeArchiveDialog').open,false);assert.equal(f.byId('e2eeArchiveContent').children.length,0);assert.equal(f.ctx.localStorage.getItem('token'),'session');
  gate.resolve();await flush();assert.equal(f.ctx.localStorage.getItem('token'),null);assert.equal(f.ctx.location.href,'/login.html');
});
test('token changes fence an in-flight history reply even without a storage or status event',async()=>{
  const gate=deferred();const f=await fixture({read:(kind,_t,_c,_p,normal)=>kind==='archive_history'?gate.promise:normal()});await f.history();f.setToken('new-account');
  gate.resolve({context:2,page:{kind:'archive_history',items:[message(1)],nextCursor:null}});await flush();assert.equal(f.ctx.archiveState(),null);assert.equal(f.byId('e2eeArchiveContent').children.length,0);assert.equal(f.byId('e2eeArchiveDialog').open,false);
});
test('unreadable or failed initial status never reads history or exposes raw native errors',async()=>{
  for(const status of [()=>Promise.reject(Error('native sensitive detail')),()=>({context:2,phase:'failed',runtimePhase:null})]){
    const f=await fixture({status});await f.open();assert.equal(f.ctx.archiveState().retired,true);assert.equal(kind(f,'archives').length,0);assert.equal(kind(f,'forbidden').length,0);assert.doesNotMatch(f.byId('e2eeArchiveNotice').textContent,/sensitive/);assert.equal(f.timers.size,0);
  }
});
