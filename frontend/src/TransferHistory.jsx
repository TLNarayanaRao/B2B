import React, { useEffect, useState } from 'react';
export default function TransferHistory(){
 const [items,setItems]=useState([]),[error,setError]=useState('');
 async function load(){try{const r=await fetch('/api/transfers');if(!r.ok)throw new Error('Unable to load transfers');setItems(await r.json());}catch(e){setError(e.message);}}
 useEffect(()=>{load();},[]);
 return <section className="table-panel transfer-history"><div className="table-heading"><div><h2>Protocol transfer history</h2><p>Connection checks, browsing, sends, and received documents.</p></div><button className="secondary" onClick={load}>Refresh</button></div>{error&&<div className="error">{error}</div>}{items.length?items.map(item=><details className="run" key={item.id}><summary><span className={item.status==='success'?'configured':'failure'}>{item.status}</span><strong>{item.operation} · {item.path||'Endpoint root'}</strong><span>{new Date(item.created).toLocaleString()}</span></summary><pre>{JSON.stringify(item.detail,null,2)}</pre>{['receive','inbound'].includes(item.operation)&&item.status==='success'&&<a className="secondary" href={`/api/transfers/${item.id}/download`}>Download received document</a>}</details>):<div className="empty"><strong>No protocol activity yet</strong><p>Open a connection and run an action.</p></div>}</section>;
}
