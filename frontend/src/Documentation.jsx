import React,{useEffect,useState} from 'react';
import {BookOpen,Download} from 'lucide-react';

function GuideBody({content}){
 const blocks=[],lines=content.split(/\r?\n/),ticks=String.fromCharCode(96).repeat(3);
 const startsBlock=line=>/^(#|~~~|\||- |\d+\. )/.test(line)||line.startsWith(ticks);
 for(let i=0;i<lines.length;){
   const line=lines[i];
   if(!line.trim()){i++;continue;}
   if(line.startsWith('~~~')||line.startsWith(ticks)){
     const fence=line.slice(0,3),code=[];i++;
     while(i<lines.length&&!lines[i].startsWith(fence)){code.push(lines[i++]);}
     i++;blocks.push(<pre key={blocks.length}><code>{code.join('\n')}</code></pre>);continue;
   }
   const heading=line.match(/^(#{1,3}) (.+)$/);
   if(heading){const Tag=heading[1].length===1?'h2':heading[1].length===2?'h3':'h4';blocks.push(<Tag key={blocks.length}>{heading[2]}</Tag>);i++;continue;}
   if(line.startsWith('|')){
     const rows=[];
     while(i<lines.length&&lines[i].startsWith('|')){const cells=lines[i++].split('|').slice(1,-1).map(c=>c.trim());if(!cells.every(c=>/^[-: ]+$/.test(c)))rows.push(cells);}
     blocks.push(<div className="table-scroll" key={blocks.length}><table><thead><tr>{rows[0]?.map((c,j)=><th key={j}>{c}</th>)}</tr></thead><tbody>{rows.slice(1).map((row,j)=><tr key={j}>{row.map((c,k)=><td key={k}>{c}</td>)}</tr>)}</tbody></table></div>);continue;
   }
   if(/^(- |\d+\. )/.test(line)){
     const ordered=/^\d/.test(line),items=[];
     while(i<lines.length&&/^(- |\d+\. )/.test(lines[i]))items.push(lines[i++].replace(/^(- |\d+\. )/,''));
     const Tag=ordered?'ol':'ul';blocks.push(<Tag key={blocks.length}>{items.map((item,j)=><li key={j}>{item}</li>)}</Tag>);continue;
   }
   const paragraph=[line];i++;
   while(i<lines.length&&lines[i].trim()&&!startsBlock(lines[i]))paragraph.push(lines[i++]);
   blocks.push(<p key={blocks.length}>{paragraph.join(' ')}</p>);
 }
 return blocks;
}

export default function Documentation(){
 const [guides,setGuides]=useState([]),[selected,setSelected]=useState('user-guide'),[guide,setGuide]=useState(null),[error,setError]=useState('');
 useEffect(()=>{let active=true;fetch('/api/documentation').then(async r=>{if(!r.ok)throw new Error('Documentation unavailable');const data=await r.json();if(active)setGuides(data);}).catch(e=>{if(active)setError(e.message);});return()=>{active=false;};},[]);
 useEffect(()=>{let active=true;setGuide(null);setError('');fetch('/api/documentation/'+selected).then(async r=>{if(!r.ok)throw new Error('Guide unavailable');const data=await r.json();if(active)setGuide(data);}).catch(e=>{if(active)setError(e.message);});return()=>{active=false;};},[selected]);
 function download(){const url=URL.createObjectURL(new Blob([guide.content],{type:'text/markdown;charset=utf-8'}));const link=document.createElement('a');link.href=url;link.download='relay-'+guide.slug+'.md';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
 return <><div className="section-heading"><div><h2><BookOpen size={22}/> Relay documentation</h2><p>Set up connections, exchange documents, and manage delivery.</p></div>{guide&&<button className="secondary" onClick={download}><Download size={16}/>Download guide</button>}</div><div className="documentation-layout"><nav className="documentation-nav" aria-label="Documentation guides">{guides.map(item=><button key={item.slug} aria-current={selected===item.slug?'page':undefined} className={selected===item.slug?'active':''} onClick={()=>setSelected(item.slug)}>{item.title}</button>)}</nav><article className="table-panel documentation-body">{error?<p role="alert" className="error">{error}</p>:guide?<GuideBody content={guide.content}/>:<p role="status">Loading guide...</p>}</article></div></>;
}
