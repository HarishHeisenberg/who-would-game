// Who Would?! online server: serves the game page + a tiny realtime document store over WebSocket.
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const docs=new Map(), rooms=new Map(), subs=new Set(), conns=new Map();   // docs: path -> data ; rooms: CODE -> last activity
const page=()=>fs.readFileSync(path.join(__dirname,'index.html'));
const srv=http.createServer((q,r)=>{
  if(q.url==='/health'){r.end('ok');return}
  r.writeHead(200,{'content-type':'text/html; charset=utf-8'});r.end(page());
});
const wss=new WebSocketServer({server:srv,maxPayload:4096});
const snapDoc=p=>docs.has(p)?{exists:true,data:docs.get(p)}:{exists:false};
const snapCol=p=>({docs:[...docs].filter(([k])=>k.startsWith(p+'/')&&!k.slice(p.length+1).includes('/')).map(([,d])=>({data:d}))});
const push=s=>s.ws.readyState===1&&s.ws.send(JSON.stringify({sub:s.id,snap:s.kind==='doc'?snapDoc(s.path):snapCol(s.path)}));
const okPath=p=>typeof p==='string'&&/^rooms\/[A-Z0-9]{5}(\/[a-z](\/[a-z0-9]{1,16})?)?$/.test(p);
const notify=p=>{const parent=p.split('/').slice(0,-1).join('/');subs.forEach(s=>{if(s.path===p||s.path===parent)push(s)})};
function removePlayer(code,id){   // a player left: remove their entry + vote, hand over host/asker if needed
  const pp=`rooms/${code}/p/${id}`,vp=`rooms/${code}/v/${id}`;docs.delete(pp);docs.delete(vp);notify(pp);notify(vp);
  const rest=[...docs].filter(([k])=>k.startsWith(`rooms/${code}/p/`)).map(([,d])=>d).sort((a,b)=>a.t-b.t);
  const rp=`rooms/${code}`,r=docs.get(rp);if(!rest.length||!r)return;const u={};
  if(r.host===id)u.host=rest[0].id;if(r.asker===id)u.asker=rest[0].id;
  if(Object.keys(u).length){docs.set(rp,{...r,...u});notify(rp)}
}
wss.on('connection',ws=>{
  ws.alive=true;ws.on('pong',()=>ws.alive=true);
  ws.on('message',m=>{
    let x;try{x=JSON.parse(m)}catch{return}
    if(x.op==='join'){if(/^[A-Z0-9]{5}$/.test(x.code)&&/^[a-z0-9]{1,16}$/.test(x.id)){ws.who={code:x.code,id:x.id};const c=conns.get(x.code+'/'+x.id);if(c&&c.timer)clearTimeout(c.timer);conns.set(x.code+'/'+x.id,{ws})}return}
    if(!okPath(x.path))return;
    const code=x.path.split('/')[1];
    if(x.op==='get')ws.send(JSON.stringify({id:x.id,snap:snapDoc(x.path)}));
    else if(x.op==='sub'){subs.add({ws,id:x.id,path:x.path,kind:x.kind});push({ws,id:x.id,path:x.path,kind:x.kind})}
    else if((x.op==='set'||x.op==='update')&&x.data&&typeof x.data==='object'){
      if(rooms.size>5000&&!rooms.has(code))return;
      docs.set(x.path,x.op==='set'?x.data:{...docs.get(x.path),...x.data});rooms.set(code,Date.now());
      const parent=x.path.split('/').slice(0,-1).join('/');
      subs.forEach(s=>{if(s.path===x.path||s.path===parent)push(s)});
    }
  });
  ws.on('close',()=>{
    subs.forEach(s=>s.ws===ws&&subs.delete(s));if(!ws.who)return;
    const {code,id}=ws.who,k=code+'/'+id,c=conns.get(k);if(!c||c.ws!==ws)return;
    c.timer=setTimeout(()=>{if(conns.get(k)!==c)return;conns.delete(k);removePlayer(code,id)},8000);  // 8s grace for network blips
  });
});
setInterval(()=>{wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()})},30000);       // keep connections alive
setInterval(()=>{const old=Date.now()-12*3600e3;rooms.forEach((t,c)=>{if(t<old){rooms.delete(c);[...docs.keys()].forEach(k=>k.startsWith('rooms/'+c)&&docs.delete(k))}})},600000); // delete rooms idle 12h
srv.listen(process.env.PORT||3000,()=>console.log('Running on port '+(process.env.PORT||3000)));
