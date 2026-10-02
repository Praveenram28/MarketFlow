import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Search, Bell, Home, BarChart3, Layers3, Bitcoin, Star, Clock3, X, Trash2, BrainCircuit, RefreshCw, ChevronRight, TrendingUp, TrendingDown, Sparkles, Activity, Menu, ArrowUpRight, ArrowDownRight } from 'lucide-react';
import { ResponsiveContainer, LineChart, Line, Tooltip, CartesianGrid, XAxis, YAxis, AreaChart, Area } from 'recharts';
import './styles.css';

const API='https://marketflow-api-bva0.onrender.com';
const money=(n,d=2)=>n==null?'—':Number(n).toLocaleString('en-IN',{minimumFractionDigits:d,maximumFractionDigits:d});
const pct=n=>n==null?'—':`${Number(n)>=0?'+':''}${Number(n).toFixed(2)}%`;
const nav=[['Home',Home],['Stocks',BarChart3],['Indices',Layers3],['Crypto',Bitcoin],['Watchlist',Star],['Alerts',Clock3]];

function MiniChart({positive=true}){const data=Array.from({length:18},(_,i)=>50+Math.sin(i/2.1)*3+(positive?i*.55:-i*.3)); const min=Math.min(...data),max=Math.max(...data); const path=data.map((v,i)=>{const x=i/(data.length-1)*100,y=92-(v-min)/Math.max(.01,max-min)*82;return `${i?'L':'M'} ${x} ${y}`}).join(' ');return <svg viewBox="0 0 100 100" className={positive?'mini up':'mini down'}><path d={path}/></svg>}
function FullChart({data=[]}){return <div className="chart"><ResponsiveContainer width="100%" height="100%"><AreaChart data={data}><defs><linearGradient id="area" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#7c5cff" stopOpacity=".35"/><stop offset="100%" stopColor="#7c5cff" stopOpacity="0"/></linearGradient></defs><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e9e7f4"/><XAxis dataKey="label" hide/><YAxis domain={['auto','auto']} tickFormatter={v=>`₹${money(v,0)}`} width={78}/><Tooltip formatter={v=>[`₹${money(v)}`,'Price']}/><Area type="monotone" dataKey="p" stroke="#6d4aff" fill="url(#area)" strokeWidth={3}/></AreaChart></ResponsiveContainer></div>}

function App(){
 const [active,setActive]=useState('Home'),[market,setMarket]=useState([]),[query,setQuery]=useState(''),[results,setResults]=useState([]),[searchOpen,setSearchOpen]=useState(false),[watchlist,setWatchlist]=useState(()=>JSON.parse(localStorage.getItem('mf-watchlist')||'[]')),[alerts,setAlerts]=useState([]),[selected,setSelected]=useState(null),[history,setHistory]=useState([]),[ai,setAi]=useState(null),[toast,setToast]=useState(''),[loading,setLoading]=useState(false),[page,setPage]=useState(1),[mobile,setMobile]=useState(false),[connected,setConnected]=useState(false),[totalStocks,setTotalStocks]=useState(0);
 const [nseMarketStatus,setNseMarketStatus]=useState('OPEN');
 const [nseMarketReason,setNseMarketReason]=useState('');
 useEffect(()=>localStorage.setItem('mf-watchlist',JSON.stringify(watchlist)),[watchlist]);
 useEffect(()=>{fetch(`${API}/api/market`).then(r=>r.json()).then(setMarket).catch(()=>{});fetch(`${API}/api/alerts`).then(r=>r.json()).then(setAlerts).catch(()=>{});const WS = new WebSocket('wss://marketflow-api-bva0.onrender.com/ws/market');ws.onopen=()=>setConnected(true);ws.onclose=()=>setConnected(false);ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.type==='snapshot'){setMarket(m.items||[]);setConnected(true)}if(m.type==='ticker')setMarket(prev=>{const i=prev.findIndex(x=>x.key===m.item.key);return i<0?[...prev,m.item]:prev.map((x,j)=>j===i?{...x,...m.item}:x)});if(m.type==='alert'){setToast(`🔔 ${m.item.name} crossed ${m.item.condition==='above'?'above':'below'} ₹${money(m.item.price)}`);fetch(`${API}/api/alerts`).then(r=>r.json()).then(setAlerts);setTimeout(()=>setToast(''),5000)}};return()=>ws.close()},[]);
 useEffect(()=>{const q=query.trim();if(!q){setResults([]);return}let stop=false;const t=setTimeout(()=>fetch(`${API}/api/search?q=${encodeURIComponent(q)}`).then(r=>r.json()).then(x=>!stop&&setResults(x||[])).catch(()=>!stop&&setResults([])),220);return()=>{stop=true;clearTimeout(t)}},[query]);
 useEffect(()=>{
  fetch(`${API}/api/health`)
    .then(r=>r.json())
    .then(x=>{
      setTotalStocks(x.stockKeys||0);
      setNseMarketStatus(x.nseMarketStatus||'OPEN');
      setNseMarketReason(x.nseMarketReason||'');
    })
    .catch(()=>{})
},[connected]);
 const stocks=useMemo(()=>market.filter(x=>x.type==='stock'),[market]);
 const liveStocks=useMemo(()=>stocks.filter(x=>x.price!=null),[stocks]);
 const indices=market.filter(x=>x.type==='index'); const crypto=market.filter(x=>x.type==='crypto');
 const gainers=[...liveStocks].sort((a,b)=>b.changePct-a.changePct); const losers=[...liveStocks].sort((a,b)=>a.changePct-b.changePct); const trending=[...liveStocks].sort((a,b)=>b.changePct-a.changePct); const watch=market.filter(x=>watchlist.includes(x.symbol));
 const pageSize=40,totalPages=Math.max(1,Math.ceil(Math.max(totalStocks,stocks.length)/pageSize));
 const sortedStocks=[...stocks].sort((a,b)=>{
  const aLive=a.price!=null?1:0;
  const bLive=b.price!=null?1:0;
  if(bLive!==aLive) return bLive-aLive;
  return Number(b.changePct||0)-Number(a.changePct||0);
});

const shownStocks=sortedStocks.slice((page-1)*pageSize,page*pageSize);
 function go(name){setActive(name);setMobile(false);if(name==='Stocks')setPage(1)}
 function toggleWatch(sym){setWatchlist(v=>v.includes(sym)?v.filter(x=>x!==sym):[...v,sym])}
 async function openAsset(x){setSearchOpen(false);setQuery('');let a=x;try{const r=await fetch(`${API}/api/subscribe`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({key:x.key})});if(r.ok)a={...x,...await r.json()}}catch{} setSelected(a);setAi(null);setLoading(true);const [h,aa]=await Promise.all([fetch(`${API}/api/history/${encodeURIComponent(a.key)}`).then(r=>r.json()).catch(()=>[]),fetch(`${API}/api/ai/${encodeURIComponent(a.key)}`).then(r=>r.json()).catch(()=>null)]);setHistory((h||[]).map(v=>({label:new Date(v.t).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}),p:Number(v.p)})));setAi(aa);setLoading(false)}
 async function createAlert(){if(!selected||!selected.price)return;const target=Number(document.getElementById('alertTarget')?.value);const condition=document.getElementById('alertCondition')?.value||'above';if(!target)return;const r=await fetch(`${API}/api/alerts`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:selected.symbol,condition,target})});if(r.ok){const created=await r.json();setAlerts(v=>[created,...v]);setToast(`Alert created for ${selected.symbol}`);setTimeout(()=>setToast(''),2500)}}
 async function delAlert(id){await fetch(`${API}/api/alerts/${id}`,{method:'DELETE'});setAlerts(v=>v.filter(a=>a.id!==id))}
 return <div className="app">
  <header className="topbar"><button className="mobile-menu" onClick={()=>setMobile(!mobile)}><Menu/></button><div className="brand"><span className="logo">M</span><div><b>MarketFlow</b><small>LIVE MARKET INTELLIGENCE</small></div></div><div className="searchbox"><Search size={18}/><input value={query} onFocus={()=>setSearchOpen(true)} onChange={e=>setQuery(e.target.value)} placeholder="Search any NSE stock, index or crypto..."/>{query&&<X className="click" size={17} onClick={()=>{setQuery('');setResults([])}}/>}{searchOpen&&query&&<div className="searchmenu">{results.length?results.slice(0,10).map(r=><button key={r.key} onClick={()=>openAsset(r)}><div><strong>{r.symbol}</strong><span>{r.name} · {r.type==='stock'?'NSE':'Index'}</span></div><b>{r.price!=null?`₹${money(r.price)}`:'LIVE'}</b></button>):<div className="searching"><RefreshCw className="spin" size={16}/>Searching the full NSE stock universe…</div>}</div>}</div><div className="header-right"><span className={connected?'live':'offline'}><i/> {connected?'LIVE':'CONNECTING'}</span><button className="icon-btn" onClick={()=>go('Alerts')}><Bell size={20}/>{alerts.filter(a=>!a.triggered).length>0&&<em>{alerts.filter(a=>!a.triggered).length}</em>}</button><div className="avatar">PR</div></div></header>
  <aside className={`sidebar ${mobile?'show':''}`}><div className="nav-title">MARKETS</div>{nav.map(([n,I])=><button key={n} className={active===n?'nav-active':''} onClick={()=>go(n)}><I size={19}/><span>{n}</span>{n==='Alerts'&&alerts.filter(a=>!a.triggered).length>0&&<em>{alerts.filter(a=>!a.triggered).length}</em>}</button>)}<div className="connection"><div className="connection-head"><span>●</span><b>Market connection</b></div><p>NSE MCP: {totalStocks?'NSE catalog loaded':'waiting'}</p><p>Crypto: connected</p><strong>{totalStocks.toLocaleString('en-IN')} NSE stocks</strong></div></aside>
  <main>
   {active==='Alerts'?<Alerts alerts={alerts} del={delAlert} open={s=>{const x=market.find(m=>m.symbol===s);if(x)openAsset(x)}}/>:<>
   <section className="hero">
  <div>
    <span className="eyebrow">
      <Sparkles size={14} /> REAL-TIME MARKET
    </span>
    <div className="hero-content">
  <h1>Markets at a glance.</h1>

  <p>
    Live prices, trending movements, intelligent charts and automatic alerts —
    all in one colorful workspace.
  </p>

  <div className="hero-stats">
    <div>
      <span>NSE stocks</span>
      <strong>{totalStocks.toLocaleString('en-IN')}</strong>
    </div>

    <div>
      <span>Live quotes</span>
      <strong>{liveStocks.length}</strong>
    </div>

    <div>
  <span>NSE Market</span>

  <strong className={nseMarketStatus === 'CLOSED' ? 'downtext' : 'uptext'}>
    {nseMarketStatus === 'CLOSED' ? 'CLOSED' : 'LIVE'}
  </strong>

  <small>
    {nseMarketStatus === 'CLOSED'
      ? `NSE • ${nseMarketReason}`
      : 'NSE • Open'}
  </small>
</div>
  </div>
</div>
{active === 'Home' && (
  <div className="home-panels">

    <Panel
      title="Trending now"
      sub="Highest live percentage gains"
      icon={<TrendingUp size={20} />}
      action={() => go('Stocks')}
      actionText="View all"
    >
      <div className="asset-list">
        {gainers.slice(0, 5).map((x, i) => (
          <AssetRow
            key={x.key}
            x={x}
            rank={i + 1}
            watch={watchlist.includes(x.symbol)}
            toggle={() => toggleWatch(x.symbol)}
            open={() => openAsset(x)}
          />
        ))}
      </div>

      {!gainers.length && (
        <Empty text="Waiting for NSE market data..." />
      )}
    </Panel>

    <Panel
      title="Market movers"
      sub="Largest live declines"
      icon={<TrendingDown size={20} />}
      action={() => go('Stocks')}
      actionText="View all"
    >
      <div className="asset-list">
        {losers.slice(0, 5).map((x, i) => (
          <AssetRow
            key={x.key}
            x={x}
            rank={i + 1}
            watch={watchlist.includes(x.symbol)}
            toggle={() => toggleWatch(x.symbol)}
            open={() => openAsset(x)}
          />
        ))}
      </div>

      {!losers.length && (
        <Empty text="Waiting for NSE market data..." />
      )}
    </Panel>

  </div>
)}

    {active === 'Home' && (
      <div className="index-strip">
        {indices.map((x) => (
          <div className="index-card" key={x.key}>
            <span>{x.symbol}</span>
            <strong>₹{Number(x.price || 0).toLocaleString('en-IN')}</strong>
            <span className={Number(x.changePct || 0) >= 0 ? 'up' : 'down'}>
              {Number(x.changePct || 0) >= 0 ? '+' : ''}
              {Number(x.changePct || 0).toFixed(2)}%
            </span>
          </div>
        ))}
      </div>
    )}

    {active === 'Stocks' && (
      <Panel
        title="All NSE stocks"
        sub={`${totalStocks.toLocaleString('en-IN')} instruments`}
      >
        <div className="grid-cards">
          {shownStocks.length ? (
  shownStocks.map((x) => (
              <MarketCard
                key={x.key}
                x={x}
                watch={watchlist.includes(x.symbol)}
                toggle={() => toggleWatch(x.symbol)}
                open={() => openAsset(x)}
              />
            ))
          ) : (
            <Empty text="Waiting for NSE stock data..." />
          )}
        </div>
      </Panel>
    )}

    {active === 'Indices' && (
      <Panel
        title="Indian indices"
        sub="Live benchmark prices"
        icon={<Layers3 size={20} />}
      >
        <div className="grid-cards">
          {indices.length ? (
            indices.map((x) => (
              <MarketCard
                key={x.key}
                x={x}
                watch={watchlist.includes(x.symbol)}
                toggle={() => toggleWatch(x.symbol)}
                open={() => openAsset(x)}
              />
            ))
          ) : (
            <Empty text="Waiting for index data..." />
          )}
        </div>
      </Panel>
    )}

    {active === 'Crypto' && (
      <Panel
        title="Crypto — live"
        sub="Converted to INR using live FX"
        icon={<Bitcoin size={20} />}
      >
        <div className="grid-cards">
          {crypto.length ? (
            crypto.map((x) => (
              <MarketCard
                key={x.key}
                x={x}
                watch={watchlist.includes(x.symbol)}
                toggle={() => toggleWatch(x.symbol)}
                open={() => openAsset(x)}
              />
            ))
          ) : (
            <Empty text="Waiting for crypto data..." />
          )}
        </div>
      </Panel>
    )}

    {active === 'Watchlist' && (
      <Panel
        title="Your watchlist"
        sub="Every saved instrument"
        icon={<Star size={20} />}
      >
        <div className="grid-cards">
          {watch.length ? (
            watch.map((x) => (
              <MarketCard
                key={x.key}
                x={x}
                watch={true}
                toggle={() => toggleWatch(x.symbol)}
                open={() => openAsset(x)}
              />
            ))
          ) : (
            <Empty text="Your watchlist is empty. Search a stock and press the star." />
          )}
        </div>
      </Panel>
    )}

    {active === 'Alerts' && (
  <Alerts
    alerts={alerts}
    del={delAlert}
    open={(symbol) => {
      const x = market.find((m) => m.symbol === symbol);
      if (x) openAsset(x);
    }}
  />
)}
  </div>
</section>
   </>}
  </main>
  {selected&&<div className="overlay" onClick={()=>setSelected(null)}><div className="modal" onClick={e=>e.stopPropagation()}><button className="modal-close" onClick={()=>setSelected(null)}><X/></button><div className="modal-head"><div><span className="eyebrow">{selected.type.toUpperCase()} · {selected.symbol}</span><h2>{selected.name}</h2><p>{selected.source||'Market data'}</p></div><button className={`star-large ${watchlist.includes(selected.symbol)?'chosen':''}`} onClick={()=>toggleWatch(selected.symbol)}><Star fill={watchlist.includes(selected.symbol)?'currentColor':'none'}/></button></div><div className="big-price"><strong>{selected.price!=null?`₹${money(selected.price)}`:'Waiting for live price'}</strong><span className={selected.changePct>=0?'uptext':'downtext'}>{selected.price!=null?`${selected.change>=0?'+':''}₹${money(selected.change)}  (${pct(selected.changePct)})`:'Subscribe to receive live quote'}</span></div><FullChart data={history}/><div className="analysis-grid"><div className="alert-card"><h3>🔔 Price alert</h3><p>Get an automatic notification when the live price crosses your target.</p><div className="alert-form"><select id="alertCondition"><option value="above">Crosses above</option><option value="below">Falls below</option></select><input id="alertTarget" type="number" placeholder={selected.price?Math.round(selected.price):'Target price'}/><button onClick={createAlert}>Create alert</button></div></div><div className="ai-card"><div className="ai-head"><BrainCircuit/><h3>AI Market Analysis</h3>{loading&&<RefreshCw className="spin" size={17}/>}</div>{ai?<><div className="ai-row"><span className={`signal ${ai.signal?.toLowerCase()}`}>{ai.signal}</span><b>{ai.score}/100</b><span>{ai.confidence}% confidence</span></div><p>{ai.summary}</p><ul>{ai.factors?.map((f,i)=><li key={i}>{f}</li>)}</ul></>:<p>Analyzing recent live and historical movement…</p>}</div></div></div></div>}
  {toast&&<div className="toast">{toast}</div>}
 </div>
}
function Panel({title,sub,icon,action,actionText,children}){return <section className="panel"><div className="panel-title"><div className="panel-icon">{icon}</div><div><h2>{title}</h2><p>{sub}</p></div>{action&&<button className="panel-action" onClick={action}>{actionText}<ChevronRight size={16}/></button>}</div>{children}</section>}
function AssetRow({x,rank,watch,toggle,open,table=false}){return <div className={`asset-row ${table?'table-row':''}`} onClick={open}><div className="rank">{rank||<span className="dot"/>}</div><button className={`star-mini ${watch?'chosen':''}`} onClick={e=>{e.stopPropagation();toggle()}}><Star size={15} fill={watch?'currentColor':'none'}/></button><div className="asset-avatar">{x.symbol?.slice(0,2)}</div><div className="asset-name"><b>{x.symbol}</b><span>{x.name}</span></div><div className="asset-price">{x.price!=null?`₹${money(x.price)}`:<small>Waiting…</small>}</div><div className={x.changePct>=0?'uptext':'downtext'}>{x.price!=null?pct(x.changePct):'—'}</div><div className="row-chart"><MiniChart positive={x.changePct>=0}/></div><ChevronRight className="row-arrow" size={17}/></div>}
function MarketCard({x,watch,toggle,open}){return <div className="market-card" onClick={open}><div className="card-top"><div className="asset-avatar big">{x.symbol?.slice(0,2)}</div><button className={`star-mini ${watch?'chosen':''}`} onClick={e=>{e.stopPropagation();toggle()}}><Star size={16} fill={watch?'currentColor':'none'}/></button></div><b>{x.symbol}</b><span>{x.name}</span><strong>{x.price!=null?`₹${money(x.price)}`:'Waiting for live price'}</strong><em className={x.changePct>=0?'uptext':'downtext'}>{x.price!=null?pct(x.changePct):'LIVE'}</em></div>}
function Pagination({page,pages,setPage}){if(pages<=1)return null;return <div className="pagination"><button disabled={page<=1} onClick={()=>setPage(Math.max(1,page-1))}>← Previous</button><b>Page {page} / {pages}</b><button disabled={page>=pages} onClick={()=>setPage(Math.min(pages,page+1))}>Next →</button></div>}
function Empty({text}){return <div className="empty"><Sparkles size={25}/><p>{text}</p></div>}
function Alerts({alerts,del,open}){return <section className="panel"><div className="panel-title"><div className="panel-icon"><Bell/></div><div><h2>Price alerts</h2><p>Automatic notifications from live market ticks.</p></div></div>{alerts.length?alerts.map(a=><div className="alert-row" key={a.id}><div><b>{a.symbol}</b><span>{a.condition==='above'?'Crosses above':'Falls below'} ₹{money(a.target)}</span></div><strong className={a.triggered?'muted':'uptext'}>{a.triggered?'Triggered':'Watching'}</strong><button onClick={()=>open(a.symbol)}>Open</button><button className="delete" onClick={()=>del(a.id)}><Trash2 size={16}/></button></div>):<Empty text="No alerts yet. Open any live instrument to create one."/>}</section>}
createRoot(document.getElementById('root')).render(<App/>);
