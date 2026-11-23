import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';

const source = ts.transpile(readFileSync(new URL('../web/src/chat/040-image-upload.ts', import.meta.url), 'utf8'));
class Element extends EventTarget {
  value = ''; disabled = false; hidden = true; checked = false; textContent = ''; src = ''; open = false; style = {}; files = [];
  click() { this.dispatchEvent(new Event('click')); }
  showModal() { this.open = true; }
  close() { this.open = false; this.dispatchEvent(new Event('close')); }
  removeAttribute(name) { this[name] = ''; }
}
const tick = () => new Promise(resolve => setImmediate(resolve));
function fixture(desktop = true, attach = false, storage = new Map()) {
  const ids = ['imageSendDialog','imageSendMode','imageSendQuality','imageSendQualityValue','imageSendDimensions','imageSendStatus',
    'imageSendPreview','imageSendPrepare','imageSendSubmit','imageSendCancel','imageSendName','imageSendOriginalSize','imageSendRemember',
    'desktopImageSettings','imagePreferenceStatus','resetImagePreference'];
  const elements = Object.fromEntries(ids.map(id => [id,new Element()]));
  const processing = [], uploads = [], sent = [], blobs = [], revoked = [], inputs = [];
  let processImage = async file => ({ file, mime: file.type, width: 2, height: 2, changed: false, reason: 'original_smaller' });
  const ctx = { File, FormData, Response, Error, console,
    currentChatID: 7, accountID: 7, token: 'session-a', loggedToken: 'session-a', authExpiredHandled: false,
    accountStorageKey: base => base+':'+ctx.accountID,
    localStorage: { getItem: key => key === 'token' ? ctx.loggedToken : storage.get(key) || null,
      setItem: (key,value) => storage.set(key,value), removeItem: key => storage.delete(key) },
    URL: { createObjectURL: file => { const url='blob:preview-'+blobs.length;blobs.push({file,url});return url; }, revokeObjectURL: url => revoked.push(url) },
    document: { getElementById: id => elements[id], createElement: () => { const e=new Element();inputs.push(e);return e; }, body: { appendChild() {} } },
    optionalById: id => attach && id==='attachBtn' ? elements.attachBtn : null,
    alert: message => { throw Error('Unexpected alert: '+message); },
    sessionFetch: async (path, options) => { uploads.push({path,options});return new Response(JSON.stringify({url:'/uploads/media/prepared.jpg',name:'original.tiff',size:12})); },
    sendMediaMessage: (...args) => sent.push({kind:'image',args}), sendFileMessage: (...args) => sent.push({kind:'file',args}),
  };
  elements.attachBtn = new Element();
  ctx.window=ctx;
  if(desktop)ctx.zchatDesktop={prepareImage:async(...args)=>{processing.push(args);return processImage(...args)}};
  runInNewContext(source,ctx);
  return {ctx,elements,processing,uploads,sent,blobs,revoked,inputs,storage,setProcessor: fn=>{processImage=fn}};
}
function compressed() { return {file:new File(['smaller'],'photo.jpg',{type:'image/jpeg'}),mime:'image/jpeg',width:2,height:2,changed:true,reason:'compressed'}; }
const selectCompression=f=>{f.elements.imageSendMode.value='compress';f.elements.imageSendMode.dispatchEvent(new Event('change'))};

test('original selection never invokes compression and keeps the exact File', async()=>{
  const f=fixture(),file=new File(['original bytes'],'photo.png',{type:'image/png'});
  const choice=f.ctx.chooseImageToSend(file,()=>true);
  assert.equal(f.elements.imageSendDialog.open,true);
  f.elements.imageSendSubmit.click();
  const result=await choice;
  assert.equal(result.file,file);assert.equal(result.asFile,false);
  assert.equal(f.processing.length,0);assert.equal(f.uploads.length,0);assert.equal(f.blobs.length,0);
});

test('quality and size reach preparation; changing them invalidates the old result',async()=>{
  const f=fixture(),file=new File(['original image'],'photo.png',{type:'image/png'});f.setProcessor(async()=>compressed());
  const choice=f.ctx.chooseImageToSend(file,()=>true);selectCompression(f);
  f.elements.imageSendQuality.value='45';f.elements.imageSendDimensions.value='1600';
  f.elements.imageSendPrepare.click();await tick();
  assert.equal(f.processing[0][0],file);assert.deepEqual(f.processing[0].slice(1),[45,1600]);
  assert.equal(f.elements.imageSendSubmit.disabled,false);assert.equal(f.blobs.length,1);
  f.elements.imageSendQuality.value='90';f.elements.imageSendQuality.dispatchEvent(new Event('input'));
  assert.equal(f.elements.imageSendSubmit.disabled,true);assert.equal(f.revoked.length,1);
  assert.equal(f.uploads.length,0,'Preview must not send an upload');
  f.elements.imageSendCancel.click();assert.equal(await choice,null);
});

test('cancelled processing cannot overwrite a subsequent photo dialog or upload',async()=>{
  const f=fixture();let finish;f.setProcessor(()=>new Promise(resolve=>{finish=resolve}));
  const first=f.ctx.chooseImageToSend(new File(['first'],'first.png',{type:'image/png'}),()=>true);selectCompression(f);
  f.elements.imageSendRemember.checked=true;
  f.elements.imageSendPrepare.click();f.elements.imageSendCancel.click();assert.equal(await first,null);assert.equal(f.storage.size,0);
  const nextFile=new File(['next'],'next.png',{type:'image/png'}),next=f.ctx.chooseImageToSend(nextFile,()=>true);
  finish(compressed());await tick();
  assert.equal(f.elements.imageSendName.textContent,'next.png');assert.equal(f.blobs.length,0);
  f.elements.imageSendSubmit.click();assert.equal((await next).file,nextFile);assert.equal(f.uploads.length,0);
});

test('session/chat invalidation while processing cancels the result',async()=>{
  const f=fixture();let finish,current=true;f.setProcessor(()=>new Promise(resolve=>{finish=resolve}));
  const choice=f.ctx.chooseImageToSend(new File(['photo'],'photo.png',{type:'image/png'}),()=>current);selectCompression(f);
  f.elements.imageSendPrepare.click();current=false;finish(compressed());await tick();
  assert.equal(await choice,null);assert.equal(f.blobs.length,0);assert.equal(f.uploads.length,0);
});

test('original TIFF is sent as a file and preserves bytes through multipart',async()=>{
  const f=fixture(true,true),file=new File(['original tiff bytes'],'original.tiff',{type:'image/tiff'});
  f.inputs[0].files=[file];f.inputs[0].dispatchEvent(new Event('change'));await tick();
  assert.equal(f.elements.imageSendSubmit.textContent,'Отправить файл');
  f.elements.imageSendSubmit.click();await tick();
  assert.equal(f.uploads[0].path,'/api/files/upload');
  assert.equal(await f.uploads[0].options.body.get('file').text(),'original tiff bytes');
  assert.equal(f.sent[0].kind,'file');assert.equal(f.sent[0].args[1],7);assert.equal(f.processing.length,0);
});

test('browser upload keeps its existing media route even without a declared MIME',async()=>{
  const f=fixture(false,true),file=new File(['png bytes'],'photo.png');
  f.inputs[0].files=[file];f.inputs[0].dispatchEvent(new Event('change'));await tick();
  assert.equal(f.uploads[0].path,'/api/media/upload');assert.equal(f.sent[0].kind,'image');
  assert.equal(f.elements.imageSendDialog.open,false);
});

test('switching chats while a send dialog is open prevents upload to either chat',async()=>{
  const f=fixture(true,true),file=new File(['photo'],'photo.png',{type:'image/png'});
  f.inputs[0].files=[file];f.inputs[0].dispatchEvent(new Event('change'));await tick();
  f.ctx.currentChatID=8;f.elements.imageSendSubmit.click();await tick();
  assert.equal(f.uploads.length,0);assert.equal(f.sent.length,0);
});

test('remembered original survives reload, preserves bytes and is scoped to the account',async()=>{
  const f=fixture(),file=new File(['original bytes'],'photo.png',{type:'image/png'});
  const choice=f.ctx.chooseImageToSend(file,()=>true);
  f.elements.imageSendRemember.checked=true;f.elements.imageSendSubmit.click();await choice;
  const reloaded=fixture(true,false,f.storage);
  assert.equal((await reloaded.ctx.chooseImageToSend(file,()=>true)).file,file);
  assert.equal(reloaded.elements.imageSendDialog.open,false);assert.equal(reloaded.processing.length,0);
  reloaded.ctx.accountID=8;
  const other=reloaded.ctx.chooseImageToSend(file,()=>true);
  assert.equal(reloaded.elements.imageSendDialog.open,true);reloaded.elements.imageSendCancel.click();await other;
});

test('remembered compression automatically applies the saved quality and dimensions to a small photo',async()=>{
  const f=fixture(),file=new File(['original photo'],'photo.png',{type:'image/png'});f.setProcessor(async()=>compressed());
  const choice=f.ctx.chooseImageToSend(file,()=>true);selectCompression(f);
  f.elements.imageSendQuality.value='45';f.elements.imageSendDimensions.value='1600';f.elements.imageSendRemember.checked=true;
  f.elements.imageSendPrepare.click();await tick();f.elements.imageSendSubmit.click();await choice;
  const next=await f.ctx.chooseImageToSend(file,()=>true);
  assert.equal(next.file.type,'image/jpeg');assert.deepEqual(f.processing[1].slice(1),[45,1600]);
  assert.equal(f.elements.imageSendDialog.open,false);assert.equal(f.uploads.length,0);
});

test('photos above 5 MiB always ask again and prefill but do not overwrite the remembered choice',async()=>{
  const f=fixture(),pref=JSON.stringify({version:1,mode:'compress',quality:55,maxDimension:2560});f.storage.set('zchat_image_send_v1:7',pref);
  const file=new File([new Uint8Array(5*1024*1024+1)],'large.png',{type:'image/png'});
  const choice=f.ctx.chooseImageToSend(file,()=>true);
  assert.equal(f.elements.imageSendDialog.open,true);assert.equal(f.elements.imageSendQuality.value,'55');assert.equal(f.processing.length,0);
  f.elements.imageSendMode.value='original';f.elements.imageSendMode.dispatchEvent(new Event('change'));
  f.elements.imageSendSubmit.click();assert.equal((await choice).file,file);
  assert.equal(f.storage.get('zchat_image_send_v1:7'),pref);
});

test('failed automatic compression asks for an explicit choice instead of silently uploading an original',async()=>{
  const f=fixture();f.storage.set('zchat_image_send_v1:7',JSON.stringify({version:1,mode:'compress',quality:80,maxDimension:0}));
  f.setProcessor(async()=>{throw Error('Codec rejected the image')});
  const file=new File(['damaged'],'photo.png',{type:'image/png'}),choice=f.ctx.chooseImageToSend(file,()=>true);await tick();
  assert.equal(f.elements.imageSendDialog.open,true);assert.equal(f.elements.imageSendSubmit.disabled,true);
  assert.match(f.elements.imageSendStatus.textContent,/Codec rejected/);assert.equal(f.uploads.length,0);
  f.elements.imageSendCancel.click();assert.equal(await choice,null);
});

test('invalid stored preferences are ignored and resetting restores the per-photo question',async()=>{
  const f=fixture(),file=new File(['photo'],'photo.png',{type:'image/png'});
  for(const pref of ['invalid json',JSON.stringify({version:1,mode:'compress',quality:0,maxDimension:0})]){
    f.storage.set('zchat_image_send_v1:7',pref);const choice=f.ctx.chooseImageToSend(file,()=>true);
    assert.equal(f.elements.imageSendMode.value,'original');assert.equal(f.processing.length,0);f.elements.imageSendCancel.click();await choice;
  }
  f.storage.set('zchat_image_send_v1:7',JSON.stringify({version:1,mode:'original',quality:80,maxDimension:0}));
  f.elements.resetImagePreference.click();assert.equal(f.storage.has('zchat_image_send_v1:7'),false);
  assert.match(f.elements.imagePreferenceStatus.textContent,/Спрашивать перед каждым/);
  const choice=f.ctx.chooseImageToSend(file,()=>true);assert.equal(f.elements.imageSendDialog.open,true);f.elements.imageSendCancel.click();await choice;
});

test('automatic compression discards a result after the session changes',async()=>{
  const f=fixture();f.storage.set('zchat_image_send_v1:7',JSON.stringify({version:1,mode:'compress',quality:80,maxDimension:0}));
  let finish,current=true;f.setProcessor(()=>new Promise(resolve=>{finish=resolve}));
  const choice=f.ctx.chooseImageToSend(new File(['photo'],'photo.png',{type:'image/png'}),()=>current);
  current=false;finish(compressed());assert.equal(await choice,null);assert.equal(f.uploads.length,0);assert.equal(f.elements.imageSendDialog.open,false);
});
