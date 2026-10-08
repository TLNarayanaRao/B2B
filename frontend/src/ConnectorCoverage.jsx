import React,{useEffect,useState} from 'react';
export default function ConnectorCoverage(){
 const [catalog,setCatalog]=useState(null),[error,setError]=useState('');
 useEffect(()=>{fetch('/api/connector-catalog').then(async response=>{if(!response.ok)throw new Error('Connector reference unavailable');setCatalog(await response.json());}).catch(e=>setError(e.message));},[]);
 if(error)return <p className="error">{error}</p>;
 if(!catalog)return null;
 return <section className="table-panel"><div className="table-heading"><div><h2>Relay connector reference</h2><p>{catalog.scope}</p></div><span className="subtle">RELAY DOCUMENTATION</span></div>{catalog.connectors.map(c=><details className="run" key={c.id}><summary><strong>{c.name}</strong><span>{c.role}</span></summary><p>{c.implemented}</p><p><strong>Verification:</strong> {c.verification}</p><p><strong>Current limitations:</strong> {c.gaps}</p></details>)}</section>;
}
