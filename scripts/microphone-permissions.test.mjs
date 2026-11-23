import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import {runInNewContext} from 'node:vm';
import ts from 'typescript';
const source=ts.createSourceFile('settings.ts',readFileSync(new URL('../web/src/chat/033-settings-modal.ts',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true);
const declaration=source.statements.find(s=>ts.isFunctionDeclaration(s)&&s.name?.text==='resetDesktopMicrophonePermission');
const code=ts.transpile(declaration.getText(source));
function fixture({state='idle',recording=false,desktop=true,failure}={}){
 const button={disabled:false},feedback=[],calls=[];let reloads=0;
 const ctx={window:{zchatDesktop:desktop?{resetMicrophonePermission:async()=>{calls.push('reset');if(failure)throw failure}}:undefined},
  optionalById:()=>button,hideSettingsFeedback:()=>{},showSettingsFeedback:(...args)=>feedback.push(args),
  callState:state,document:{getElementById:()=>({classList:{contains:()=>recording}})},location:{reload:()=>reloads++}};
 runInNewContext(code,ctx);return{ctx,button,feedback,calls,get reloads(){return reloads}};
}
test('reset waits for native completion, prevents double click and reloads only after success',async()=>{
 const f=fixture();let finish;f.ctx.window.zchatDesktop.resetMicrophonePermission=()=>new Promise(r=>{finish=r});
 const pending=f.ctx.resetDesktopMicrophonePermission();assert.equal(f.button.disabled,true);assert.equal(f.reloads,0);
 await f.ctx.resetDesktopMicrophonePermission();finish();await pending;assert.equal(f.reloads,1);
});
test('native reset failure keeps the document and permits retry',async()=>{
 const f=fixture({failure:'Runtime unavailable'});await f.ctx.resetDesktopMicrophonePermission();
 assert.equal(f.reloads,0);assert.equal(f.button.disabled,false);assert.match(f.feedback[0][2],/Runtime unavailable/);
});
test('capture and an incoming/ongoing/reconnecting call prevent a reset/reload',async()=>{
 for(const state of ['incoming','calling','connecting','connected','reconnecting']){const f=fixture({state});await f.ctx.resetDesktopMicrophonePermission();assert.equal(f.calls.length,0);assert.equal(f.reloads,0);assert.match(f.feedback[0][2],/Завершите/)}
 const f=fixture({recording:true});await f.ctx.resetDesktopMicrophonePermission();assert.equal(f.calls.length,0);assert.equal(f.reloads,0);
});
test('the browser does not perform native permission reset',async()=>{
 const f=fixture({desktop:false});await f.ctx.resetDesktopMicrophonePermission();assert.equal(f.reloads,0);assert.equal(f.button.disabled,false);
});
