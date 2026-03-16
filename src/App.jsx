import { useState, useEffect, useCallback } from "react";

/* ── Supabase config ── */
const SB_URL = "https://dwkvdelvnmvniewakzoq.supabase.co";
const SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR3a3ZkZWx2bm12bmlld2Frem9xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1NDAxMTcsImV4cCI6MjA4OTExNjExN30.4B9V-_uKHM8UOf4MxZ_g5tg1kcarPqZ0SoZifEu6O4A";

const sbH = () => ({ apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, "Content-Type": "application/json", Prefer: "return=representation" });

async function sbGet(table, params) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${params || ""}`, { headers: sbH() });
  if (!r.ok) { console.error("GET", table, r.status, await r.text()); return []; }
  return r.json();
}

async function sbCount(table, params) {
  const h = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}`, Prefer: "count=exact" };
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${params || "select=id&head=true"}`, { method: "HEAD", headers: h });
  const range = r.headers.get("content-range");
  if (range) { const p = range.split("/"); return parseInt(p[1]) || 0; }
  return 0;
}

async function sbInsert(table, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, { method: "POST", headers: sbH(), body: JSON.stringify(data) });
  if (!r.ok) { console.error("INSERT", table, r.status, await r.text()); return null; }
  const result = await r.json();
  return result[0] || result;
}

async function sbUpdate(table, id, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?id=eq.${id}`, { method: "PATCH", headers: sbH(), body: JSON.stringify(data) });
  if (!r.ok) { console.error("UPDATE", table, r.status, await r.text()); return null; }
  const result = await r.json();
  return result[0] || result;
}

/* ── In-memory cache (2 min TTL) ── */
const _cache = {};
function cacheGet(key) { const c = _cache[key]; if (c && Date.now() - c.ts < 120000) return c.data; return null; }
function cacheSet(key, data) { _cache[key] = { data, ts: Date.now() }; }
function cacheClear(prefix) { Object.keys(_cache).forEach(k => { if (!prefix || k.startsWith(prefix)) delete _cache[k]; }); }
async function sbGetCached(table, params) {
  const key = `${table}?${params || ""}`;
  const cached = cacheGet(key);
  if (cached) return cached;
  const result = await sbGet(table, params);
  cacheSet(key, result);
  return result;
}

/* ── Constants ── */
const STAGES = ["Not Yet Contacted","Discovered","Qualified","Visited","Estimated","Presented","Revised","Prepare To Close","Sold","Lost"];
const ACTIVE_STAGES = STAGES.filter(s => s !== "Sold" && s !== "Lost");
const PTYPES = ["Bathroom Remodel","Kitchen Remodel","Bathroom / Kitchen","Basement Finish","Room Addition","Whole House Renovation","Exterior Finish","Interior Finish","Porch Conversion","Attic Conversion","Decks","Flooring","PGT","26 Entries","Other"];
const LOCS = ["Marion Co - NE","Marion Co - SE","Marion Co - NW","Marion Co - SW","Carmel","Fishers","Hamilton Co","Johnson Co","Hendricks Co","Hancock Co","Boone Co","Other County / Out of State","Unknown"];
const LSOURCES = ["Referral","Referral - Client","Referral - BAGI","Referral - Designer","Referral - Employees","Referral - Franklin Window & Door","Referral - Geoff","Referral - Home Builder","Referral - Other Remodeler","Referral--Realtor","Referral--Vendor","Repeat","Repeat Client","Previous Lead","Website / Internet","Internet","Google","Houzz","Drive By / Lives in Area","Drive By - Lives In Area","Trade Show","Home Show - Spring","Home Show - Fall","Home-A-Rama","Angie's List / Angi","BAGI","Yard Sign","Newspaper","Thumbtack","Groupon","Social Media","Other","Unknown"];
const TEAM = ["Geoff Horen","Kalee Dunham","Brittney Schebler","Rebecca Rhea","Leesa","Lanie"];
const ATYPES = ["Note","Call","Email","Site Visit","Meeting"];
const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const YEARS_RANGES = ["0-2","2-3","4-5","5-10","10+","Forever"];
const BUYING_BEHAVIOR = [
  {v:"1",l:"1 - Basic / Functional"},
  {v:"1.5",l:"1.5 - Between Basic & Nice"},
  {v:"2",l:"2 - Nice, Not Crazy"},
  {v:"2.5",l:"2.5 - Between Nice & Luxury"},
  {v:"3",l:"3 - Do It Once, Do It Right"}
];
const DEATH_STAGES = ["Discovery","Qualification","First Visit","Presentation","Revision"];
const DEATH_REASONS = {
  "Discovery": ["Unresponsive To","We Declined","Can't Meet Client Timeline"],
  "Qualification": ["Cancel Prior","Unresponsive To","Not Enough Budget","Information Only","We Declined","Can't Meet Client Timeline"],
  "First Visit": ["Cancel Prior","No Show","Unresponsive To","We Declined","Not Enough Budget","Information Only","Other Intentions"],
  "Presentation": ["Cancel Prior","No Show","Unresponsive To","Not Enough Budget","Other Intentions","Competition","We Declined"],
  "Revision": ["Unresponsive To","Not Enough Budget","Other Intentions","Competition","We Declined"]
};
const LIST_COLS = "id,job_name,stage,project_type,job_location,lead_source,lead_date,sale_amount,contact_id,buying_behavior";
const PIPE_COLS = "id,job_name,stage,estimate_amount,lead_date,contact_id,sale_amount,date_sold,buying_behavior";
const DASH_COLS = "id,job_name,stage,project_type,job_location,lead_source,lead_date,sale_amount,estimate_amount,date_sold,date_lost,stage_lost,lost_reason,contact_id,salesperson,buying_behavior,years_in_home,staying_years";
const HOME_VALUE_RANGES = [{l:"Under $200K",min:0,max:200000},{l:"$200K-$400K",min:200000,max:400000},{l:"$400K-$700K",min:400000,max:700000},{l:"$700K+",min:700000,max:999999999}];
const PRICE_BUCKETS = [{l:"Under $4K",min:0,max:4000},{l:"$4K-$10K",min:4000,max:10000},{l:"$10K-$20K",min:10000,max:20000},{l:"$20K-$75K",min:20000,max:75000},{l:"$75K+",min:75000,max:999999999}];

/* ── Auto-Location Detection ── */
const ZIP_MAP = {
  // Marion Co - NE
  "46202":"Marion Co - NE","46205":"Marion Co - NE","46218":"Marion Co - NE","46219":"Marion Co - NE",
  "46220":"Marion Co - NE","46226":"Marion Co - NE","46229":"Marion Co - NE","46235":"Marion Co - NE",
  "46236":"Marion Co - NE","46256":"Marion Co - NE",
  // Marion Co - SE
  "46201":"Marion Co - SE","46203":"Marion Co - SE","46217":"Marion Co - SE","46225":"Marion Co - SE",
  "46227":"Marion Co - SE","46237":"Marion Co - SE","46239":"Marion Co - SE","46259":"Marion Co - SE",
  "46107":"Marion Co - SE",
  // Marion Co - NW
  "46208":"Marion Co - NW","46222":"Marion Co - NW","46224":"Marion Co - NW","46228":"Marion Co - NW",
  "46234":"Marion Co - NW","46240":"Marion Co - NW","46254":"Marion Co - NW","46260":"Marion Co - NW",
  "46268":"Marion Co - NW","46278":"Marion Co - NW",
  // Marion Co - SW
  "46183":"Marion Co - SW","46221":"Marion Co - SW","46241":"Marion Co - SW","46231":"Marion Co - SW",
  // Carmel
  "46032":"Carmel","46033":"Carmel","46082":"Carmel","46280":"Carmel","46290":"Carmel",
  // Fishers
  "46037":"Fishers","46038":"Fishers","46040":"Fishers","46055":"Fishers","46085":"Fishers",
  // Hamilton Co (other)
  "46060":"Hamilton Co","46061":"Hamilton Co","46062":"Hamilton Co","46074":"Hamilton Co",
  "46030":"Hamilton Co","46031":"Hamilton Co","46034":"Hamilton Co","46064":"Hamilton Co",
  // Johnson Co
  "46142":"Johnson Co","46143":"Johnson Co","46131":"Johnson Co","46184":"Johnson Co",
  "46124":"Johnson Co","46162":"Johnson Co","46181":"Johnson Co",
  // Hendricks Co
  "46112":"Hendricks Co","46113":"Hendricks Co","46118":"Hendricks Co","46122":"Hendricks Co",
  "46123":"Hendricks Co","46149":"Hendricks Co","46158":"Hendricks Co","46167":"Hendricks Co",
  "46168":"Hendricks Co",
  // Hancock Co
  "46140":"Hancock Co","46163":"Hancock Co","46130":"Hancock Co","46117":"Hancock Co",
  // Boone Co
  "46034":"Boone Co","46052":"Boone Co","46065":"Boone Co","46069":"Boone Co",
  "46071":"Boone Co","46075":"Boone Co","46077":"Boone Co",
};

const CITY_MAP = {
  "carmel":"Carmel","fishers":"Fishers","noblesville":"Hamilton Co","westfield":"Hamilton Co",
  "cicero":"Hamilton Co","arcadia":"Hamilton Co","atlanta":"Hamilton Co","sheridan":"Hamilton Co",
  "greenwood":"Johnson Co","franklin":"Johnson Co","whiteland":"Johnson Co","bargersville":"Johnson Co",
  "edinburgh":"Johnson Co","trafalgar":"Johnson Co","avon":"Hendricks Co","brownsburg":"Hendricks Co",
  "plainfield":"Hendricks Co","danville":"Hendricks Co","pittsboro":"Hendricks Co",
  "lizton":"Hendricks Co","north salem":"Hendricks Co",
  "mccordsville":"Hancock Co","greenfield":"Hancock Co","new palestine":"Hancock Co","fortville":"Hancock Co",
  "zionsville":"Boone Co","whitestown":"Boone Co","lebanon":"Boone Co","thorntown":"Boone Co",
  "beech grove":"Marion Co - SE","southport":"Marion Co - SE","lawrence":"Marion Co - NE",
  "speedway":"Marion Co - NW","cumberland":"Marion Co - SE",
};

function detectLocation(address, city, zip) {
  // Priority 1: Zip code
  if (zip) {
    const z = zip.toString().trim().slice(0,5);
    if (ZIP_MAP[z]) return ZIP_MAP[z];
  }
  // Priority 2: City name
  if (city) {
    const c = city.toString().trim().toLowerCase();
    if (CITY_MAP[c]) return CITY_MAP[c];
    if (c === "indianapolis" || c === "indpls" || c === "indy") {
      // Fall through to address parsing for Indy
    } else {
      return null; // Unknown city, don't guess
    }
  }
  // Priority 3: Address directional parsing (for Indianapolis)
  if (address) {
    const a = address.toString().trim().toUpperCase();
    // Look for N/S/E/W at start of address or after house number
    const match = a.match(/^\d+\s+(N\.?|S\.?|E\.?|W\.?|NORTH|SOUTH|EAST|WEST|NE|NW|SE|SW)\b/);
    if (match) {
      const dir = match[1].replace(/\./g,"").trim();
      // N = north of Washington (could be NE or NW)
      // S = south of Washington (could be SE or SW)
      // E = east of Meridian
      // W = west of Meridian
      if (dir === "N" || dir === "NORTH") {
        // Check if address contains a street that's east or west of Meridian
        if (a.includes("MERIDIAN")) return "Marion Co - NE"; // on Meridian, default NE
        // Look for E/W further in the address
        const secondary = a.match(/\b(E\.?|W\.?|EAST|WEST)\b/);
        if (secondary) {
          const s = secondary[1].replace(/\./g,"");
          return s === "E" || s === "EAST" ? "Marion Co - NE" : "Marion Co - NW";
        }
        return "Marion Co - NE"; // Default north to NE
      }
      if (dir === "S" || dir === "SOUTH") {
        const secondary = a.match(/\b(E\.?|W\.?|EAST|WEST)\b/);
        if (secondary) {
          const s = secondary[1].replace(/\./g,"");
          return s === "E" || s === "EAST" ? "Marion Co - SE" : "Marion Co - SW";
        }
        return "Marion Co - SE"; // Default south to SE
      }
      if (dir === "E" || dir === "EAST") return "Marion Co - NE"; // East of Meridian, default NE
      if (dir === "W" || dir === "WEST") return "Marion Co - NW"; // West of Meridian, default NW
      if (dir === "NE") return "Marion Co - NE";
      if (dir === "NW") return "Marion Co - NW";
      if (dir === "SE") return "Marion Co - SE";
      if (dir === "SW") return "Marion Co - SW";
    }
  }
  return null;
}

/* ── Utils ── */
const fmtC = n => (!n && n !== 0) ? "" : new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",minimumFractionDigits:0,maximumFractionDigits:0}).format(n);
const fmtD = d => { if (!d) return ""; try { return new Date(d+"T00:00:00").toLocaleDateString("en-US",{month:"short",day:"numeric",year:"numeric"}); } catch { return d; } };
const fmtTS = ts => { if (!ts) return ""; try { const d = new Date(ts); return d.toLocaleDateString("en-US",{month:"short",day:"numeric",year:"numeric"})+" · "+d.toLocaleTimeString("en-US",{hour:"numeric",minute:"2-digit"}); } catch { return ts; } };
const daysSince = d => { if (!d) return null; return Math.floor((Date.now() - new Date(d+"T00:00:00").getTime()) / 86400000); };
const todayStr = () => new Date().toISOString().slice(0,10);

/* ── Styles ── */
const SCOLORS = {"Not Yet Contacted":{bg:"#F1EFE8",fg:"#444441"},"Discovered":{bg:"#E6F1FB",fg:"#0C447C"},"Qualified":{bg:"#EEEDFE",fg:"#3C3489"},"Visited":{bg:"#E1F5EE",fg:"#085041"},"Estimated":{bg:"#FAEEDA",fg:"#633806"},"Presented":{bg:"#FAECE7",fg:"#712B13"},"Revised":{bg:"#FBEAF0",fg:"#72243E"},"Prepare To Close":{bg:"#EAF3DE",fg:"#27500A"},"Sold":{bg:"#EAF3DE",fg:"#173404"},"Lost":{bg:"#FCEBEB",fg:"#791F1F"}};
const inpS = {width:"100%",padding:"8px 10px",borderRadius:8,border:"1px solid #d0cec7",fontSize:14,background:"#fff",color:"#1a1a1a",boxSizing:"border-box",outline:"none"};
const filtS = {padding:"6px 10px",borderRadius:8,border:"1px solid #d0cec7",fontSize:13,background:"#fff",color:"#1a1a1a"};
const btnP = {padding:"8px 24px",borderRadius:8,border:"none",background:"#185FA5",color:"#fff",cursor:"pointer",fontSize:14,fontWeight:600};
const btnSec = {padding:"8px 16px",borderRadius:8,border:"1px solid #d0cec7",background:"#fff",cursor:"pointer",fontSize:13,color:"#1a1a1a",fontWeight:500};
const cardShadow = "0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)";

/* ── Shared Components ── */
const Badge = ({ stage }) => { const c = SCOLORS[stage] || {bg:"#F1EFE8",fg:"#444441"}; return <span style={{display:"inline-block",padding:"2px 10px",borderRadius:6,fontSize:12,fontWeight:600,background:c.bg,color:c.fg,whiteSpace:"nowrap"}}>{stage}</span>; };
const Field = ({ label, half, children }) => <div style={{flex:half?"1 1 45%":"1 1 100%",minWidth:half?180:0}}><label style={{display:"block",fontSize:12,color:"#6b6960",marginBottom:4,fontWeight:600,letterSpacing:"0.02em"}}>{label}</label>{children}</div>;
const SH = ({ children }) => <div style={{fontSize:12,fontWeight:700,color:"#6b6960",textTransform:"uppercase",letterSpacing:"0.06em",borderBottom:"1px solid #e8e6df",paddingBottom:6,marginTop:20,marginBottom:12}}>{children}</div>;
const Pill = ({ children, active, onClick }) => <button onClick={onClick} style={{padding:"6px 16px",border:"none",background:"none",cursor:"pointer",fontSize:14,fontWeight:active?600:400,color:active?"#1a1a1a":"#8a8780",borderBottom:active?"2.5px solid #185FA5":"2.5px solid transparent",marginBottom:-1,transition:"all 0.15s"}}>{children}</button>;
const TopTab = ({ children, active, onClick }) => <button onClick={onClick} style={{padding:"8px 20px",border:"none",background:active?"#185FA5":"transparent",color:active?"#fff":"#6b6960",cursor:"pointer",fontSize:13,fontWeight:600,borderRadius:8,transition:"all 0.15s"}}>{children}</button>;

const Modal = ({ title, onClose, children, width }) => (
  <div style={{position:"fixed",inset:0,zIndex:1000,display:"flex",alignItems:"center",justifyContent:"center",background:"rgba(0,0,0,0.4)",backdropFilter:"blur(4px)"}} onClick={onClose}>
    <div onClick={e => e.stopPropagation()} style={{background:"#fff",borderRadius:14,width:width||560,maxWidth:"94vw",maxHeight:"90vh",overflow:"auto",boxShadow:"0 25px 50px rgba(0,0,0,0.15)"}}>
      <div style={{padding:"20px 24px 0",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
        <h2 style={{margin:0,fontSize:18,fontWeight:600}}>{title}</h2>
        <button onClick={onClose} style={{background:"none",border:"none",cursor:"pointer",fontSize:20,color:"#8a8780",padding:4,lineHeight:1}}>×</button>
      </div>
      <div style={{padding:"16px 24px 24px"}}>{children}</div>
    </div>
  </div>
);

/* ── Skeleton Loaders ── */
const Skel = ({ w, h, mb }) => <div style={{height:h||14,width:w||"100%",borderRadius:4,background:"#eeece6",animation:"pulse 1.5s ease-in-out infinite",marginBottom:mb||0}} />;
const SkeletonRow = ({ cols }) => <tr><td colSpan={cols} style={{padding:0}}><div style={{display:"flex",gap:16,padding:"12px 8px"}}>{Array.from({length:cols}).map((_,i)=><Skel key={i} w={i===0?"40%":"20%"} />)}</div></td></tr>;
const SkeletonCard = () => <div style={{background:"#fff",borderRadius:10,padding:"10px 12px",border:"1px solid #f0eeea"}}><Skel w="70%" mb={6} /><Skel w="50%" mb={8} h={11} /><Skel w="40%" h={12} /></div>;
const SkeletonMetric = () => <div style={{background:"#f7f6f3",borderRadius:10,padding:"14px 18px",flex:"1 1 130px"}}><Skel w="60%" h={10} mb={8} /><Skel w="45%" h={20} /></div>;

/* ── Activity Log ── */
function ActivityLog({ activities, projectId, contactId, onAdd }) {
  const [text, setText] = useState("");
  const [author, setAuthor] = useState(TEAM[0]);
  const [atype, setAtype] = useState("Note");
  const [showAll, setShowAll] = useState(false);
  const [saving, setSaving] = useState(false);
  const icons = {"Note":"📝","Call":"📞","Email":"✉️","Site Visit":"🏠","Meeting":"🤝"};

  const doAdd = () => {
    if (!text.trim() || saving) return;
    setSaving(true);
    sbInsert("activity_log", { project_id: projectId||null, contact_id: contactId||null, activity_type: atype, activity_text: text.trim(), author, source: "manual", activity_date: new Date().toISOString() })
    .then(result => { setSaving(false); if (result) { setText(""); if (onAdd) onAdd(result); } });
  };

  const sorted = (activities||[]).slice().sort((a,b) => new Date(b.activity_date||0) - new Date(a.activity_date||0));
  const visible = showAll ? sorted : sorted.slice(0,5);

  return (<div>
    <SH>{`Activity log (${sorted.length})`}</SH>
    <div style={{background:"#f7f6f3",borderRadius:10,padding:14}}>
      <div style={{display:"flex",gap:8,marginBottom:8,flexWrap:"wrap"}}>
        <select style={{...inpS,width:"auto",fontSize:13,padding:"6px 10px"}} value={atype} onChange={e => setAtype(e.target.value)}>{ATYPES.map(t => <option key={t}>{t}</option>)}</select>
        <select style={{...inpS,width:"auto",fontSize:13,padding:"6px 10px"}} value={author} onChange={e => setAuthor(e.target.value)}>{TEAM.map(t => <option key={t}>{t}</option>)}</select>
      </div>
      <textarea style={{...inpS,minHeight:50,resize:"vertical",fontSize:13}} value={text} onChange={e => setText(e.target.value)} placeholder="Add a note, log a call, record a conversation..." />
      <div style={{display:"flex",justifyContent:"flex-end",marginTop:8}}>
        <button onClick={doAdd} disabled={!text.trim()||saving} style={{padding:"6px 18px",borderRadius:8,border:"none",background:text.trim()?"#185FA5":"#e8e6df",color:text.trim()?"#fff":"#b0ada6",cursor:text.trim()?"pointer":"default",fontSize:13,fontWeight:600}}>{saving?"Saving...":"Add entry"}</button>
      </div>
    </div>
    {visible.map(entry => (
      <div key={entry.id} style={{padding:"12px 0",borderBottom:"1px solid #f0eeea"}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4,flexWrap:"wrap",gap:4}}>
          <div style={{display:"flex",alignItems:"center",gap:8}}>
            <span style={{fontSize:14}}>{icons[entry.activity_type]||"📝"}</span>
            <span style={{fontSize:12,fontWeight:600}}>{entry.author||""}</span>
            {entry.activity_type!=="Note"&&<span style={{fontSize:11,padding:"1px 8px",borderRadius:4,background:"#f0eeea",color:"#6b6960"}}>{entry.activity_type}</span>}
            {entry.source&&entry.source!=="manual"&&<span style={{fontSize:10,padding:"1px 6px",borderRadius:4,background:"#e8e6df",color:"#8a8780"}}>{entry.source==="marketsharp_note"?"MS":"MS-Act"}</span>}
          </div>
          <span style={{fontSize:12,color:"#8a8780"}}>{fmtTS(entry.activity_date)}</span>
        </div>
        <div style={{fontSize:14,color:"#1a1a1a",lineHeight:1.5,paddingLeft:26,whiteSpace:"pre-wrap"}}>{entry.activity_text}</div>
      </div>
    ))}
    {sorted.length>5&&<button onClick={()=>setShowAll(!showAll)} style={{background:"none",border:"none",cursor:"pointer",color:"#185FA5",fontSize:13,padding:"8px 0",fontWeight:600}}>{showAll?"Show recent only":`Show all ${sorted.length} entries`}</button>}
    {sorted.length===0&&<div style={{fontSize:13,color:"#8a8780",padding:"12px 0"}}>No activity yet.</div>}
  </div>);
}

/* ── Project Detail ── */
function ProjectDetail({ project, onBack, onSaved }) {
  const [contact, setContact] = useState(null);
  const [activities, setActivities] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [form, setForm] = useState({...project});
  const set = k => e => setForm(f => ({...f, [k]: e.target.value}));

  useEffect(() => {
    setLoading(true);
    const promises = [];
    if (project.contact_id) promises.push(sbGetCached("contacts",`id=eq.${project.contact_id}`).then(r => { if(r[0]) setContact(r[0]); }));
    promises.push(sbGet("activity_log",`order=activity_date.desc&or=(project_id.eq.${project.id},contact_id.eq.${project.contact_id||"null"})&limit=200`).then(r => setActivities(r||[])));
    Promise.all(promises).then(() => setLoading(false));
  }, [project.id]);

  const handleSave = () => {
    setSaving(true); cacheClear("projects");
    sbUpdate("projects", project.id, {
      stage:form.stage, project_type:form.project_type, lead_source:form.lead_source, salesperson:form.salesperson,
      estimate_amount:form.estimate_amount?parseFloat(form.estimate_amount):null, sale_amount:form.sale_amount?parseFloat(form.sale_amount):null,
      confidence:form.confidence?parseInt(form.confidence):null, date_sold:form.date_sold||null, date_lost:form.date_lost||null,
      stage_lost:form.stage_lost||null, lost_reason:form.lost_reason||null, job_name:form.job_name,
      years_in_home:form.years_in_home||null, staying_years:form.staying_years||null, buying_behavior:form.buying_behavior||null
    }).then(() => { setSaving(false); if(onSaved) onSaved(); });
  };

  if (loading) return <div style={{padding:20,color:"#8a8780"}}>Loading project...</div>;

  return (<div>
    <button onClick={onBack} style={{background:"none",border:"none",cursor:"pointer",fontSize:14,color:"#185FA5",fontWeight:600,padding:"0 0 16px"}}>← Back</button>
    <div style={{background:"#fff",borderRadius:14,border:"1px solid #e8e6df",padding:"20px 24px",boxShadow:cardShadow}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",flexWrap:"wrap",gap:12,marginBottom:4}}>
        <h2 style={{margin:0,fontSize:20,fontWeight:600}}>{project.job_name||"Project"}</h2><Badge stage={project.stage}/>
      </div>
      <div style={{fontSize:13,color:"#6b6960",marginBottom:16}}>{project.job_location&&<span>{project.job_location}</span>}{project.lead_date&&<span>{` · Lead: ${fmtD(project.lead_date)}`}</span>}</div>

      {contact&&<><SH>Client</SH><div style={{display:"flex",flexWrap:"wrap",gap:20,fontSize:14}}>
        <div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Name</span><br/><span style={{fontWeight:500}}>{`${contact.first_name||""} ${contact.last_name||""}`}</span></div>
        {contact.phone_home&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Home</span><br/>{contact.phone_home}</div>}
        {contact.phone_cell&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Cell</span><br/>{contact.phone_cell}</div>}
        {contact.email&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Email</span><br/>{contact.email}</div>}
        {contact.address&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Address</span><br/>{`${contact.address}, ${contact.city||""} ${contact.state||""} ${contact.zip||""}`}</div>}
        {contact.home_value&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Home value</span><br/>{fmtC(contact.home_value)}</div>}
      </div>{(contact.spouse_name||contact.spouse_email||contact.spouse_phone)&&<div style={{marginTop:12}}><div style={{fontSize:12,fontWeight:600,color:"#8a8780",marginBottom:4}}>Spouse / Partner</div><div style={{display:"flex",flexWrap:"wrap",gap:20,fontSize:14}}>
        {contact.spouse_name&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Name</span><br/>{contact.spouse_name}</div>}
        {contact.spouse_email&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Email</span><br/>{contact.spouse_email}</div>}
        {contact.spouse_phone&&<div><span style={{color:"#8a8780",fontSize:12,fontWeight:600}}>Phone</span><br/>{contact.spouse_phone}</div>}
      </div></div>}</>}

      <SH>Project details</SH>
      <div style={{display:"flex",flexWrap:"wrap",gap:12}}>
        <Field label="Job name" half><input style={inpS} value={form.job_name||""} onChange={set("job_name")}/></Field>
        <Field label="Stage" half><select style={inpS} value={form.stage||""} onChange={set("stage")}>{STAGES.map(s=><option key={s}>{s}</option>)}</select></Field>
        <Field label="Project type" half><select style={inpS} value={form.project_type||""} onChange={set("project_type")}><option value="">Select...</option>{PTYPES.map(t=><option key={t}>{t}</option>)}</select></Field>
        <Field label="Lead source" half><select style={inpS} value={form.lead_source||""} onChange={set("lead_source")}><option value="">Select...</option>{LSOURCES.map(s=><option key={s}>{s}</option>)}</select></Field>
        <Field label="Salesperson" half><select style={inpS} value={form.salesperson||""} onChange={set("salesperson")}><option value="">Select...</option>{TEAM.map(s=><option key={s}>{s}</option>)}</select></Field>
        <Field label="Estimate $" half><input style={inpS} value={form.estimate_amount||""} onChange={set("estimate_amount")} placeholder="$0"/></Field>
        <Field label="Confidence %" half><input style={inpS} value={form.confidence||""} onChange={set("confidence")} placeholder="0-100"/></Field>
        <Field label="Buying behavior" half><select style={inpS} value={form.buying_behavior||""} onChange={set("buying_behavior")}><option value="">Select...</option>{BUYING_BEHAVIOR.map(b=><option key={b.v} value={b.v}>{b.l}</option>)}</select></Field>
      </div>
      <SH>Client situation</SH>
      <div style={{display:"flex",flexWrap:"wrap",gap:12}}>
        <Field label="Years in home" half><select style={inpS} value={form.years_in_home||""} onChange={set("years_in_home")}><option value="">Select...</option>{YEARS_RANGES.map(r=><option key={r}>{r}</option>)}</select></Field>
        <Field label="Staying how many years" half><select style={inpS} value={form.staying_years||""} onChange={set("staying_years")}><option value="">Select...</option>{YEARS_RANGES.map(r=><option key={r}>{r}</option>)}</select></Field>
      </div>
      {(form.stage==="Sold"||form.sale_amount)&&<><SH>Sale details</SH><div style={{display:"flex",flexWrap:"wrap",gap:12}}>
        <Field label="Sale amount" half><input style={inpS} value={form.sale_amount||""} onChange={set("sale_amount")} placeholder="$0"/></Field>
        <Field label="Date sold" half><input type="date" style={inpS} value={form.date_sold||""} onChange={set("date_sold")}/></Field>
      </div></>}
      {form.stage==="Lost"&&<><SH>Why was this lead lost?</SH><div style={{display:"flex",flexWrap:"wrap",gap:12}}>
        <Field label="Date lost" half><input type="date" style={inpS} value={form.date_lost||""} onChange={set("date_lost")}/></Field>
        <Field label="Stage lost at" half><select style={inpS} value={form.stage_lost||""} onChange={e => { setForm(f => ({...f, stage_lost: e.target.value, lost_reason: ""})); }}><option value="">Select stage...</option>{DEATH_STAGES.map(s=><option key={s}>{s}</option>)}</select></Field>
        <Field label="Reason" half><select style={inpS} value={form.lost_reason||""} onChange={set("lost_reason")}><option value="">Select reason...</option>{(DEATH_REASONS[form.stage_lost]||[]).map(r=><option key={r}>{r}</option>)}</select></Field>
      </div></>}
      <div style={{display:"flex",justifyContent:"flex-end",marginTop:20}}><button onClick={handleSave} disabled={saving} style={btnP}>{saving?"Saving...":"Save changes"}</button></div>
      <ActivityLog activities={activities} projectId={project.id} contactId={project.contact_id} onAdd={e => setActivities(p => [e,...p])}/>
    </div>
  </div>);
}

/* ── Contact Detail ── */
function ContactDetail({ contact, onBack, onSaved, onOpenProject }) {
  const [form, setForm] = useState({...contact});
  const [projects, setProjects] = useState([]);
  const [activities, setActivities] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const set = k => e => setForm(f => ({...f, [k]: e.target.value}));
  const setWithLoc = k => e => {
    const val = e.target.value;
    setForm(f => { const u = {...f, [k]: val}; const d = detectLocation(u.address, u.city, u.zip); if (d) u.location = d; return u; });
  };

  useEffect(() => {
    setLoading(true);
    Promise.all([
      sbGet("projects",`contact_id=eq.${contact.id}&select=${LIST_COLS}&order=lead_date.desc.nullslast`),
      sbGet("activity_log",`contact_id=eq.${contact.id}&order=activity_date.desc&limit=100`)
    ]).then(([p,a]) => { setProjects(p||[]); setActivities(a||[]); setLoading(false); });
  }, [contact.id]);

  const handleSave = () => {
    setSaving(true); cacheClear("contacts");
    sbUpdate("contacts", contact.id, { first_name:form.first_name, last_name:form.last_name, email:form.email, phone_home:form.phone_home, phone_cell:form.phone_cell, address:form.address, city:form.city, state:form.state, zip:form.zip, location:form.location, home_value:form.home_value?parseFloat(form.home_value):null, spouse_name:form.spouse_name||null, spouse_email:form.spouse_email||null, spouse_phone:form.spouse_phone||null })
    .then(() => { setSaving(false); if(onSaved) onSaved(); });
  };

  if (loading) return <div style={{padding:20,color:"#8a8780"}}>Loading contact...</div>;

  return (<div>
    <button onClick={onBack} style={{background:"none",border:"none",cursor:"pointer",fontSize:14,color:"#185FA5",fontWeight:600,padding:"0 0 16px"}}>← Back</button>
    <div style={{background:"#fff",borderRadius:14,border:"1px solid #e8e6df",padding:"20px 24px",boxShadow:cardShadow}}>
      <h2 style={{margin:"0 0 16px",fontSize:20,fontWeight:600}}>{`${contact.first_name||""} ${contact.last_name||""}`}</h2>
      <SH>Contact info</SH>
      <div style={{display:"flex",flexWrap:"wrap",gap:12}}>
        <Field label="First name" half><input style={inpS} value={form.first_name||""} onChange={set("first_name")}/></Field>
        <Field label="Last name" half><input style={inpS} value={form.last_name||""} onChange={set("last_name")}/></Field>
        <Field label="Email" half><input style={inpS} value={form.email||""} onChange={set("email")}/></Field>
        <Field label="Cell phone" half><input style={inpS} value={form.phone_cell||""} onChange={set("phone_cell")}/></Field>
        <Field label="Home phone" half><input style={inpS} value={form.phone_home||""} onChange={set("phone_home")}/></Field>
        <Field label="Address" half><input style={inpS} value={form.address||""} onChange={setWithLoc("address")}/></Field>
        <Field label="City" half><input style={inpS} value={form.city||""} onChange={setWithLoc("city")}/></Field>
        <Field label="State" half><input style={inpS} value={form.state||""} onChange={set("state")}/></Field>
        <Field label="Zip" half><input style={inpS} value={form.zip||""} onChange={setWithLoc("zip")}/></Field>
        <Field label="Location" half><select style={inpS} value={form.location||""} onChange={set("location")}><option value="">Auto-detect or select...</option>{LOCS.map(l=><option key={l}>{l}</option>)}</select></Field>
        <Field label="Home value" half><input style={inpS} value={form.home_value||""} onChange={set("home_value")} placeholder="$0"/></Field>
      </div>
      <SH>Spouse / Partner</SH>
      <div style={{display:"flex",flexWrap:"wrap",gap:12}}>
        <Field label="Name" half><input style={inpS} value={form.spouse_name||""} onChange={set("spouse_name")}/></Field>
        <Field label="Email" half><input style={inpS} value={form.spouse_email||""} onChange={set("spouse_email")}/></Field>
        <Field label="Phone" half><input style={inpS} value={form.spouse_phone||""} onChange={set("spouse_phone")}/></Field>
      </div>
      <div style={{display:"flex",justifyContent:"flex-end",marginTop:16}}><button onClick={handleSave} disabled={saving} style={btnP}>{saving?"Saving...":"Save changes"}</button></div>
      <SH>{`Projects (${projects.length})`}</SH>
      {projects.length===0&&<div style={{fontSize:13,color:"#8a8780",padding:"8px 0"}}>No projects linked.</div>}
      {projects.map(p => (
        <div key={p.id} onClick={() => onOpenProject && onOpenProject(p)} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"10px 0",borderBottom:"1px solid #f0eeea",cursor:"pointer"}}>
          <div><div style={{fontWeight:500,fontSize:14}}>{p.job_name||"—"}</div><div style={{fontSize:12,color:"#8a8780"}}>{p.project_type||""}{p.lead_date?` · ${fmtD(p.lead_date)}`:""}</div></div>
          <div style={{display:"flex",alignItems:"center",gap:12}}>{p.sale_amount&&<span style={{fontWeight:600,fontSize:14}}>{fmtC(p.sale_amount)}</span>}<Badge stage={p.stage}/></div>
        </div>
      ))}
      <ActivityLog activities={activities} contactId={contact.id} onAdd={e => setActivities(p => [e,...p])}/>
    </div>
  </div>);
}

/* ── Pipeline View ── */
function PipelineView({ onOpenProject }) {
  const [data, setData] = useState(null);
  const [contacts, setContacts] = useState({});
  const [loading, setLoading] = useState(true);
  const [contactsLoading, setContactsLoading] = useState(false);
  const [soldRecent, setSoldRecent] = useState([]);

  useEffect(() => {
    sbGetCached("projects",`select=${PIPE_COLS}&stage=neq.Lost&order=lead_date.desc.nullslast&limit=2000`).then(all => {
      const grouped = {}; const recentSold = [];
      STAGES.forEach(s => { grouped[s] = []; });
      (all||[]).forEach(p => { if(p.stage==="Sold") recentSold.push(p); else if(grouped[p.stage]) grouped[p.stage].push(p); });
      setSoldRecent(recentSold.sort((a,b)=>((b.date_sold||"")>(a.date_sold||"")?1:-1)).slice(0,10)); setData(grouped); setLoading(false);
      // Phase 2: contact names
      setContactsLoading(true);
      const cids = {}; (all||[]).forEach(p => { if(p.contact_id) cids[p.contact_id]=true; });
      const idList = Object.keys(cids);
      if (idList.length > 0) {
        const batches = []; for(let i=0;i<idList.length;i+=200) batches.push(idList.slice(i,i+200));
        Promise.all(batches.map(b => sbGetCached("contacts",`id=in.(${b.join(",")})&select=id,first_name,last_name`))).then(results => {
          const cmap = {}; results.forEach(b => (b||[]).forEach(c => { cmap[c.id]=c; }));
          setContacts(cmap); setContactsLoading(false);
        });
      } else setContactsLoading(false);
    });
  }, []);

  if (loading) return (<div>
    <div style={{display:"flex",gap:12,marginBottom:20,flexWrap:"wrap"}}><SkeletonMetric/><SkeletonMetric/><SkeletonMetric/></div>
    <div style={{display:"flex",gap:10,overflowX:"auto"}}>{ACTIVE_STAGES.map(s=>{const sc=SCOLORS[s];return<div key={s} style={{flex:"1 1 0",minWidth:210,maxWidth:280}}><div style={{background:sc.bg,borderRadius:10,padding:"10px 12px",marginBottom:8}}><span style={{fontSize:13,fontWeight:700,color:sc.fg}}>{s}</span></div><div style={{display:"flex",flexDirection:"column",gap:6}}><SkeletonCard/><SkeletonCard/></div></div>;})}</div>
  </div>);

  let totalActive=0, totalEstimate=0;
  ACTIVE_STAGES.forEach(s => { const items=data[s]||[]; totalActive+=items.length; items.forEach(p => { totalEstimate+=parseFloat(p.estimate_amount)||0; }); });

  return (<div>
    <div style={{display:"flex",gap:12,marginBottom:20,flexWrap:"wrap"}}>
      <div style={{background:"#f7f6f3",borderRadius:10,padding:"12px 18px",flex:"1 1 140px"}}><div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em"}}>Active leads</div><div style={{fontSize:22,fontWeight:600}}>{totalActive}</div></div>
      <div style={{background:"#f7f6f3",borderRadius:10,padding:"12px 18px",flex:"1 1 140px"}}><div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em"}}>Pipeline value</div><div style={{fontSize:22,fontWeight:600}}>{fmtC(totalEstimate)}</div></div>
      <div style={{background:"#EAF3DE",borderRadius:10,padding:"12px 18px",flex:"1 1 140px"}}><div style={{fontSize:11,fontWeight:700,color:"#27500A",textTransform:"uppercase",letterSpacing:"0.06em"}}>Recent wins</div><div style={{fontSize:22,fontWeight:600,color:"#173404"}}>{soldRecent.length}</div></div>
    </div>
    <div style={{overflowX:"auto",paddingBottom:16}}>
      <div style={{display:"flex",gap:10,minWidth:ACTIVE_STAGES.length*220}}>
        {ACTIVE_STAGES.map(stage => {
          const items=data[stage]||[]; const sc=SCOLORS[stage]; let stageEst=0; items.forEach(p=>{stageEst+=parseFloat(p.estimate_amount)||0;});
          return (<div key={stage} style={{flex:"1 1 0",minWidth:210,maxWidth:280}}>
            <div style={{background:sc.bg,borderRadius:10,padding:"10px 12px",marginBottom:8}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}><span style={{fontSize:13,fontWeight:700,color:sc.fg}}>{stage}</span><span style={{fontSize:12,fontWeight:700,color:sc.fg,background:"rgba(255,255,255,0.5)",borderRadius:6,padding:"1px 8px"}}>{items.length}</span></div>
              {stageEst>0&&<div style={{fontSize:11,color:sc.fg,marginTop:2,opacity:0.8}}>{fmtC(stageEst)}</div>}
            </div>
            <div style={{display:"flex",flexDirection:"column",gap:6,maxHeight:500,overflowY:"auto"}}>
              {items.length===0&&<div style={{fontSize:12,color:"#b0ada6",textAlign:"center",padding:16}}>Empty</div>}
              {items.map(p => {
                const c=contacts[p.contact_id]; const cName=c?`${c.first_name||""} ${c.last_name||""}`:""; const days=daysSince(p.lead_date);
                return (<div key={p.id} onClick={()=>onOpenProject(p)} style={{background:"#fff",borderRadius:10,padding:"10px 12px",border:"1px solid #e8e6df",cursor:"pointer",boxShadow:"0 1px 2px rgba(0,0,0,0.03)",transition:"box-shadow 0.15s"}} onMouseEnter={e=>{e.currentTarget.style.boxShadow="0 3px 8px rgba(0,0,0,0.08)";}} onMouseLeave={e=>{e.currentTarget.style.boxShadow="0 1px 2px rgba(0,0,0,0.03)";}}>
                  <div style={{fontWeight:600,fontSize:13,marginBottom:2,lineHeight:1.3}}>{p.job_name||"—"}</div>
                  {cName.trim()?<div style={{fontSize:12,color:"#6b6960",marginBottom:4}}>{cName}</div>:contactsLoading?<Skel w="50%" h={11} mb={4}/>:null}
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                    <span style={{fontSize:12,color:p.estimate_amount?"#1a1a1a":"#b0ada6",fontWeight:p.estimate_amount?600:400}}>{p.estimate_amount?fmtC(p.estimate_amount):"No est."}</span>
                    <div style={{display:"flex",alignItems:"center",gap:6}}>
                      {p.buying_behavior&&<span style={{fontSize:10,padding:"1px 6px",borderRadius:4,background:"#EEEDFE",color:"#3C3489",fontWeight:600}}>B{p.buying_behavior}</span>}
                      {days!==null&&<span style={{fontSize:11,color:days>90?"#791F1F":days>30?"#633806":"#8a8780"}}>{days}d</span>}
                    </div>
                  </div>
                </div>);
              })}
            </div>
          </div>);
        })}
      </div>
    </div>
    {soldRecent.length>0&&<div style={{marginTop:16}}>
      <div style={{fontSize:13,fontWeight:700,color:"#6b6960",marginBottom:8,textTransform:"uppercase",letterSpacing:"0.06em"}}>Recent wins</div>
      <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>{soldRecent.map(p=><div key={p.id} onClick={()=>onOpenProject(p)} style={{background:"#EAF3DE",borderRadius:10,padding:"8px 14px",cursor:"pointer",border:"1px solid #d4e8c2"}}><div style={{fontWeight:600,fontSize:13,color:"#173404"}}>{p.job_name||"—"}</div><div style={{fontSize:12,color:"#27500A"}}>{p.sale_amount?fmtC(p.sale_amount):""}{p.date_sold?` · ${fmtD(p.date_sold)}`:""}</div></div>)}</div>
    </div>}
  </div>);
}

/* ── HBars (clickable) ── */
const HBars = ({ entries, color, dc, fmt, onClick }) => {
  if (!entries||!entries.length) return <div style={{fontSize:13,color:"#8a8780"}}>No data</div>;
  const mx = Math.max(...entries.map(e=>e[1]),1);
  return <div>{entries.slice(0,10).map(([name,count])=><div key={name} onClick={()=>onClick&&onClick(name)} style={{display:"flex",alignItems:"center",gap:8,marginBottom:6,cursor:onClick?"pointer":"default"}} onMouseEnter={e=>{if(onClick)e.currentTarget.style.opacity="0.7";}} onMouseLeave={e=>{e.currentTarget.style.opacity="1";}}>
    <span style={{fontSize:12,minWidth:140,color:"#6b6960",textAlign:"right",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{name}</span>
    <div style={{flex:1,height:18,background:"#f0eeea",borderRadius:4,overflow:"hidden"}}><div style={{width:`${Math.max((count/mx)*100,5)}%`,height:"100%",background:color,borderRadius:4,display:"flex",alignItems:"center",justifyContent:"flex-end",paddingRight:6,fontSize:11,fontWeight:600,color:dc}}>{fmt?fmt(count):count}</div></div>
  </div>)}</div>;
};

/* ── Drill-Down Panel ── */
function DrillDown({ title, projects, contacts, onClose, onOpenProject }) {
  if (!projects) return null;
  return (<div style={{position:"fixed",right:0,top:0,bottom:0,width:560,maxWidth:"100vw",background:"#fff",boxShadow:"-4px 0 24px rgba(0,0,0,0.12)",zIndex:999,overflow:"auto",padding:"20px 24px"}}>
    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:16}}>
      <h3 style={{margin:0,fontSize:16,fontWeight:600}}>{title}</h3>
      <button onClick={onClose} style={{background:"none",border:"none",cursor:"pointer",fontSize:20,color:"#8a8780"}}>×</button>
    </div>
    <div style={{fontSize:13,color:"#8a8780",marginBottom:12}}>{projects.length} projects · {fmtC(projects.reduce((s,p)=>s+(parseFloat(p.sale_amount)||0),0))} revenue</div>
    {projects.map(p => {
      const c = contacts[p.contact_id]; const cName = c ? `${c.first_name||""} ${c.last_name||""}`.trim() : "";
      return (<div key={p.id} onClick={()=>onOpenProject&&onOpenProject(p)} style={{padding:"10px 0",borderBottom:"1px solid #f0eeea",cursor:"pointer"}} onMouseEnter={e=>{e.currentTarget.style.background="#f7f6f3";}} onMouseLeave={e=>{e.currentTarget.style.background="transparent";}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
          <div><div style={{fontWeight:500,fontSize:14}}>{cName||p.job_name||"—"}</div><div style={{fontSize:12,color:"#8a8780"}}>{p.project_type||""}{p.lead_date?` · ${fmtD(p.lead_date)}`:""}</div></div>
          <div style={{display:"flex",alignItems:"center",gap:8}}>
            {p.sale_amount&&<span style={{fontWeight:600,fontSize:13}}>{fmtC(p.sale_amount)}</span>}
            <Badge stage={p.stage}/>
          </div>
        </div>
      </div>);
    })}
  </div>);
}

/* ── Funnel Bar ── */
function FunnelBar({ label, count, total, pct, color, onClick }) {
  const w = total > 0 ? Math.max((count/total)*100,8) : 0;
  return (<div onClick={onClick} style={{display:"flex",alignItems:"center",gap:12,marginBottom:8,cursor:onClick?"pointer":"default"}} onMouseEnter={e=>{if(onClick)e.currentTarget.style.opacity="0.8";}} onMouseLeave={e=>{e.currentTarget.style.opacity="1";}}>
    <span style={{fontSize:12,minWidth:100,color:"#6b6960",textAlign:"right",fontWeight:500}}>{label}</span>
    <div style={{flex:1,height:28,background:"#f0eeea",borderRadius:6,overflow:"hidden",position:"relative"}}>
      <div style={{width:`${w}%`,height:"100%",background:color,borderRadius:6,display:"flex",alignItems:"center",paddingLeft:10,fontSize:12,fontWeight:600,color:"#fff",minWidth:50}}>{count.toLocaleString()}</div>
    </div>
    {pct !== undefined && <span style={{fontSize:12,color:"#6b6960",minWidth:45,textAlign:"right",fontWeight:600}}>{pct}%</span>}
  </div>);
}

/* ── Dashboard (redesigned) ── */
function Dashboard({ onOpenProject }) {
  const curYear = new Date().getFullYear();
  const yearOptions = []; for(let y=curYear;y>=2000;y--) yearOptions.push(y);

  const [year, setYear] = useState(curYear);
  const [compare, setCompare] = useState(false);
  const [data, setData] = useState(null);
  const [compData, setCompData] = useState(null);
  const [contacts, setContacts] = useState({});
  const [loading, setLoading] = useState(true);
  const [drill, setDrill] = useState(null); // {title, projects}

  function loadYear(y) {
    return sbGet("projects", `select=${DASH_COLS}&lead_date=gte.${y}-01-01&lead_date=lte.${y}-12-31&limit=50000`);
  }

  function loadContacts(projects) {
    const cids = {}; (projects||[]).forEach(p=>{if(p.contact_id)cids[p.contact_id]=true;});
    const idList = Object.keys(cids);
    if (!idList.length) return;
    const batches=[]; for(let i=0;i<idList.length;i+=200) batches.push(idList.slice(i,i+200));
    Promise.all(batches.map(b=>sbGetCached("contacts",`id=in.(${b.join(",")})&select=id,first_name,last_name,home_value`))).then(results=>{
      const cmap={}; results.forEach(b=>(b||[]).forEach(c=>{cmap[c.id]=c;}));
      setContacts(prev=>({...prev,...cmap}));
    });
  }

  function compute(projects) {
    const all = projects||[];
    const sold = all.filter(p=>p.stage==="Sold");
    const lost = all.filter(p=>p.stage==="Lost");
    const active = all.filter(p=>p.stage!=="Sold"&&p.stage!=="Lost");
    const rev = sold.reduce((s,p)=>s+(parseFloat(p.sale_amount)||0),0);
    const estTotal = all.reduce((s,p)=>s+(parseFloat(p.estimate_amount)||0),0);
    const estSold = sold.reduce((s,p)=>s+(parseFloat(p.estimate_amount)||0),0);

    // Death analysis
    const deathByStage={}, deathByReason={};
    lost.forEach(p=>{
      if(p.stage_lost){deathByStage[p.stage_lost]=(deathByStage[p.stage_lost]||0)+1;}
      if(p.lost_reason){deathByReason[p.lost_reason]=(deathByReason[p.lost_reason]||0)+1;}
    });

    // Funnel: count how many made it to or past each stage
    const stageOrder = ["Not Yet Contacted","Discovered","Qualified","Visited","Estimated","Presented","Revised","Prepare To Close","Sold"];
    const stageIdx = {}; stageOrder.forEach((s,i)=>{stageIdx[s]=i;});
    // For lost leads, use stage_lost to determine how far they got
    function projectMaxStage(p) {
      if(p.stage==="Lost"&&p.stage_lost) {
        const mapping={"Discovery":1,"Qualification":2,"First Visit":3,"Presentation":5,"Revision":6};
        return mapping[p.stage_lost]||0;
      }
      return stageIdx[p.stage]||0;
    }
    const funnelCounts = stageOrder.map((_,i)=>all.filter(p=>projectMaxStage(p)>=i).length);

    // Breakdowns
    function breakdown(arr, key) {
      const m={}; arr.forEach(p=>{const v=p[key]||"Unknown"; if(!m[v])m[v]={leads:0,sold:0,rev:0}; m[v].leads++;});
      sold.forEach(p=>{const v=p[key]||"Unknown"; if(m[v]){m[v].sold++;m[v].rev+=(parseFloat(p.sale_amount)||0);}});
      return Object.entries(m).sort((a,b)=>b[1].sold-a[1].sold);
    }

    // Price distribution
    const priceDist = PRICE_BUCKETS.map(b=>{
      const matching = sold.filter(p=>{const a=parseFloat(p.sale_amount)||0; return a>=b.min&&a<b.max;});
      return {label:b.l,count:matching.length,rev:matching.reduce((s,p)=>s+(parseFloat(p.sale_amount)||0),0)};
    });

    return {
      total:all.length, sold:sold.length, lost:lost.length, active:active.length,
      rev, estTotal, estSold,
      avgSale:sold.length>0?Math.round(rev/sold.length):0,
      closeRate:all.length>0?Math.round((sold.length/all.length)*100):0,
      captureRate:estTotal>0?Math.round((rev/estTotal)*100):0,
      deathByStage:Object.entries(deathByStage).sort((a,b)=>b[1]-a[1]),
      deathByReason:Object.entries(deathByReason).sort((a,b)=>b[1]-a[1]),
      funnel:stageOrder.map((s,i)=>({stage:s,count:funnelCounts[i]})),
      byType:breakdown(all,"project_type"), bySource:breakdown(all,"lead_source"), byLoc:breakdown(all,"job_location"),
      priceDist, all, sold, lost, active
    };
  }

  useEffect(()=>{
    setLoading(true); setCompData(null);
    loadYear(year).then(projects=>{
      setData(compute(projects)); loadContacts(projects); setLoading(false);
      if(compare) loadYear(year-1).then(cp=>{setCompData(compute(cp));});
    });
  },[year,compare]);

  function openDrill(title,projects){setDrill({title,projects});}

  if(loading) return <div style={{padding:40,textAlign:"center",color:"#8a8780"}}>Loading dashboard...</div>;
  if(!data) return null;

  const ms={background:"#f7f6f3",borderRadius:10,padding:"14px 18px",flex:"1 1 130px"};
  const compStyle={fontSize:11,marginTop:2};

  function metricCard(label,value,compValue) {
    return <div style={ms}>
      <div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>{label}</div>
      <div style={{fontSize:22,fontWeight:600}}>{value}</div>
      {compare&&compData&&compValue!==undefined&&<div style={{...compStyle,color:compValue>value?"#791F1F":"#173404"}}>{year-1}: {compValue}</div>}
    </div>;
  }

  const funnelColors = ["#6b6960","#0C447C","#3C3489","#085041","#633806","#712B13","#72243E","#27500A","#173404"];

  return (<div style={{display:"flex",flexDirection:"column",gap:24}}>
    {/* Year controls */}
    <div style={{display:"flex",gap:12,alignItems:"center",flexWrap:"wrap"}}>
      <select style={{...filtS,fontWeight:600}} value={year} onChange={e=>setYear(parseInt(e.target.value))}>
        {yearOptions.map(y=><option key={y} value={y}>{y}</option>)}
      </select>
      <label style={{display:"flex",alignItems:"center",gap:6,fontSize:13,color:"#6b6960",cursor:"pointer"}}>
        <input type="checkbox" checked={compare} onChange={e=>setCompare(e.target.checked)} />
        Compare to {year-1}
      </label>
    </div>

    {/* Metrics row */}
    <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
      {metricCard("Total leads",data.total.toLocaleString(),compData?.total.toLocaleString())}
      {metricCard("Sold",data.sold.toLocaleString(),compData?.sold.toLocaleString())}
      {metricCard("Lost",data.lost.toLocaleString(),compData?.lost.toLocaleString())}
      {metricCard("Revenue",fmtC(data.rev),compData?fmtC(compData.rev):undefined)}
      {metricCard("Avg sale",fmtC(data.avgSale),compData?fmtC(compData.avgSale):undefined)}
      {metricCard("Close rate",data.closeRate+"%",compData?compData.closeRate+"%":undefined)}
      {metricCard("Est $ capture",data.captureRate+"%",compData?compData.captureRate+"%":undefined)}
    </div>

    {/* Funnel */}
    <div>
      <div style={{fontSize:14,fontWeight:600,marginBottom:12,color:"#6b6960"}}>Sales funnel — {year}</div>
      {data.funnel.map((f,i)=>{
        const pct = i>0&&data.funnel[0].count>0?Math.round((f.count/data.funnel[0].count)*100):i===0?100:0;
        return <FunnelBar key={f.stage} label={f.stage} count={f.count} total={data.funnel[0].count} pct={pct} color={funnelColors[i]||"#6b6960"} onClick={()=>{
          const stageOrder=["Not Yet Contacted","Discovered","Qualified","Visited","Estimated","Presented","Revised","Prepare To Close","Sold"];
          const mapping={"Discovery":1,"Qualification":2,"First Visit":3,"Presentation":5,"Revision":6};
          function maxStage(p){if(p.stage==="Lost"&&p.stage_lost){return mapping[p.stage_lost]||0;}return stageOrder.indexOf(p.stage)||0;}
          openDrill(`${f.stage} (${year})`,data.all.filter(p=>maxStage(p)>=i));
        }}/>;
      })}
      {compare&&compData&&<div style={{marginTop:12,padding:12,background:"#f7f6f3",borderRadius:8,fontSize:12,color:"#6b6960"}}>
        <strong>{year-1}:</strong> {compData.funnel.map(f=>`${f.stage}: ${f.count}`).join(" → ")}
      </div>}
    </div>

    {/* Death analysis */}
    <div style={{display:"flex",gap:24,flexWrap:"wrap"}}>
      <div style={{flex:"1 1 280px"}}>
        <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>Deaths by stage</div>
        <HBars entries={data.deathByStage} color="#F09595" dc="#501313" onClick={stage=>openDrill(`Lost at ${stage} (${year})`,data.lost.filter(p=>p.stage_lost===stage))}/>
      </div>
      <div style={{flex:"1 1 280px"}}>
        <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>Deaths by reason</div>
        <HBars entries={data.deathByReason} color="#F5C4C4" dc="#501313" onClick={reason=>openDrill(`Lost: ${reason} (${year})`,data.lost.filter(p=>p.lost_reason===reason))}/>
      </div>
    </div>

    {/* Breakdowns */}
    <div>
      <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>By project type — leads / sold / revenue</div>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:13}}>
          <thead><tr style={{borderBottom:"1px solid #e8e6df"}}>
            <th style={{padding:"6px 8px",textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>Type</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Leads</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Sold</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Revenue</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Close %</th>
          </tr></thead>
          <tbody>{data.byType.filter(([,v])=>v.leads>0).map(([name,v])=><tr key={name} style={{borderBottom:"1px solid #f0eeea",cursor:"pointer"}} onClick={()=>openDrill(`${name} (${year})`,data.all.filter(p=>p.project_type===name))} onMouseEnter={e=>{e.currentTarget.style.background="#f7f6f3";}} onMouseLeave={e=>{e.currentTarget.style.background="transparent";}}>
            <td style={{padding:"8px",fontWeight:500}}>{name}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{v.leads}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{v.sold}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{fmtC(v.rev)}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{v.leads>0?Math.round((v.sold/v.leads)*100)+"%":"—"}</td>
          </tr>)}</tbody>
        </table>
      </div>
    </div>

    <div style={{display:"flex",gap:24,flexWrap:"wrap"}}>
      <div style={{flex:"1 1 320px"}}>
        <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>By lead source (sold)</div>
        <HBars entries={data.bySource.filter(([,v])=>v.sold>0).map(([n,v])=>[n,v.sold])} color="#85B7EB" dc="#042C53" onClick={src=>openDrill(`${src} (${year})`,data.all.filter(p=>p.lead_source===src))}/>
      </div>
      <div style={{flex:"1 1 320px"}}>
        <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>By location (sold)</div>
        <HBars entries={data.byLoc.filter(([,v])=>v.sold>0).map(([n,v])=>[n,v.sold])} color="#9FE1CB" dc="#04342C" onClick={loc=>openDrill(`${loc} (${year})`,data.all.filter(p=>p.job_location===loc))}/>
      </div>
    </div>

    {/* Price distribution */}
    <div>
      <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>Projects sold by price range</div>
      <HBars entries={data.priceDist.filter(b=>b.count>0).map(b=>[b.label,b.count])} color="#CECBF6" dc="#3C3489" onClick={label=>{
        const bucket=PRICE_BUCKETS.find(b=>b.l===label);
        if(bucket) openDrill(`Sold ${label} (${year})`,data.sold.filter(p=>{const a=parseFloat(p.sale_amount)||0;return a>=bucket.min&&a<bucket.max;}));
      }}/>
    </div>

    {/* Drill-down panel */}
    {drill&&<DrillDown title={drill.title} projects={drill.projects} contacts={contacts} onClose={()=>setDrill(null)} onOpenProject={p=>{setDrill(null);if(onOpenProject)onOpenProject(p);}}/>}
  </div>);
}

/* ── Custom Reports ── */
function Reports({ onOpenProject }) {
  const [filters, setFilters] = useState({project_type:"",job_location:"",stage:"",lead_source:"",buying_behavior:"",years_in_home:"",staying_years:"",salesperson:"",homeValueRange:"",dateFrom:"",dateTo:""});
  const [groupBy, setGroupBy] = useState("");
  const [results, setResults] = useState(null);
  const [contacts, setContacts] = useState({});
  const [loading, setLoading] = useState(false);

  const setF = (k,v) => setFilters(f=>({...f,[k]:v}));

  function runReport() {
    setLoading(true);
    let params = `select=${DASH_COLS}&order=lead_date.desc.nullslast&limit=10000`;
    if(filters.project_type) params+=`&project_type=eq.${encodeURIComponent(filters.project_type)}`;
    if(filters.job_location) params+=`&job_location=eq.${encodeURIComponent(filters.job_location)}`;
    if(filters.stage) {
      if(filters.stage==="Active") params+=`&stage=neq.Sold&stage=neq.Lost`;
      else params+=`&stage=eq.${encodeURIComponent(filters.stage)}`;
    }
    if(filters.lead_source) params+=`&lead_source=eq.${encodeURIComponent(filters.lead_source)}`;
    if(filters.buying_behavior) params+=`&buying_behavior=eq.${encodeURIComponent(filters.buying_behavior)}`;
    if(filters.years_in_home) params+=`&years_in_home=eq.${encodeURIComponent(filters.years_in_home)}`;
    if(filters.staying_years) params+=`&staying_years=eq.${encodeURIComponent(filters.staying_years)}`;
    if(filters.salesperson) params+=`&salesperson=eq.${encodeURIComponent(filters.salesperson)}`;
    if(filters.dateFrom) params+=`&lead_date=gte.${filters.dateFrom}`;
    if(filters.dateTo) params+=`&lead_date=lte.${filters.dateTo}`;

    sbGet("projects",params).then(projects=>{
      // Load contacts for home value filtering and display
      const cids={}; (projects||[]).forEach(p=>{if(p.contact_id)cids[p.contact_id]=true;});
      const idList=Object.keys(cids);
      const contactPromise = idList.length>0 ?
        Promise.all(Array.from({length:Math.ceil(idList.length/200)},(_,i)=>idList.slice(i*200,(i+1)*200)).map(b=>sbGetCached("contacts",`id=in.(${b.join(",")})&select=id,first_name,last_name,home_value`))).then(r=>{const m={};r.forEach(b=>(b||[]).forEach(c=>{m[c.id]=c;}));return m;}) :
        Promise.resolve({});

      contactPromise.then(cmap=>{
        setContacts(prev=>({...prev,...cmap}));
        let filtered = projects||[];

        // Home value filter (requires contact data)
        if(filters.homeValueRange) {
          const range=HOME_VALUE_RANGES.find(r=>r.l===filters.homeValueRange);
          if(range) filtered=filtered.filter(p=>{const c=cmap[p.contact_id];const hv=c?parseFloat(c.home_value)||0:0;return hv>=range.min&&hv<range.max;});
        }

        // Compute summary
        const sold=filtered.filter(p=>p.stage==="Sold");
        const rev=sold.reduce((s,p)=>s+(parseFloat(p.sale_amount)||0),0);
        const est=filtered.reduce((s,p)=>s+(parseFloat(p.estimate_amount)||0),0);

        // Group by
        let grouped = null;
        if(groupBy) {
          const gmap={};
          filtered.forEach(p=>{
            let key;
            if(groupBy==="homeValue") {
              const c=cmap[p.contact_id]; const hv=c?parseFloat(c.home_value)||0:0;
              const range=HOME_VALUE_RANGES.find(r=>hv>=r.min&&hv<r.max);
              key=range?range.l:"Unknown";
            } else { key=p[groupBy]||"Unknown"; }
            if(!gmap[key])gmap[key]={leads:0,sold:0,rev:0,projects:[]};
            gmap[key].leads++;gmap[key].projects.push(p);
            if(p.stage==="Sold"){gmap[key].sold++;gmap[key].rev+=(parseFloat(p.sale_amount)||0);}
          });
          grouped=Object.entries(gmap).sort((a,b)=>b[1].sold-a[1].sold);
        }

        setResults({filtered,sold:sold.length,rev,est,avgSale:sold.length>0?Math.round(rev/sold.length):0,captureRate:est>0?Math.round((rev/est)*100):0,grouped});
        setLoading(false);
      });
    });
  }

  const selS = {...filtS,minWidth:130};

  return (<div style={{display:"flex",flexDirection:"column",gap:20}}>
    <div style={{fontSize:14,fontWeight:600,color:"#6b6960"}}>Build a custom report — select filters and click Run</div>

    <div style={{display:"flex",gap:8,flexWrap:"wrap",alignItems:"flex-end"}}>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>PROJECT TYPE</div><select style={selS} value={filters.project_type} onChange={e=>setF("project_type",e.target.value)}><option value="">All</option>{PTYPES.map(t=><option key={t}>{t}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>LOCATION</div><select style={selS} value={filters.job_location} onChange={e=>setF("job_location",e.target.value)}><option value="">All</option>{LOCS.map(l=><option key={l}>{l}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>STAGE</div><select style={selS} value={filters.stage} onChange={e=>setF("stage",e.target.value)}><option value="">All</option><option value="Active">Active (not sold/lost)</option>{STAGES.map(s=><option key={s}>{s}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>LEAD SOURCE</div><select style={selS} value={filters.lead_source} onChange={e=>setF("lead_source",e.target.value)}><option value="">All</option>{LSOURCES.map(s=><option key={s}>{s}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>BUYING BEHAVIOR</div><select style={selS} value={filters.buying_behavior} onChange={e=>setF("buying_behavior",e.target.value)}><option value="">All</option>{BUYING_BEHAVIOR.map(b=><option key={b.v} value={b.v}>{b.l}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>HOME VALUE</div><select style={selS} value={filters.homeValueRange} onChange={e=>setF("homeValueRange",e.target.value)}><option value="">All</option>{HOME_VALUE_RANGES.map(r=><option key={r.l}>{r.l}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>YEARS IN HOME</div><select style={selS} value={filters.years_in_home} onChange={e=>setF("years_in_home",e.target.value)}><option value="">All</option>{YEARS_RANGES.map(r=><option key={r}>{r}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>STAYING YEARS</div><select style={selS} value={filters.staying_years} onChange={e=>setF("staying_years",e.target.value)}><option value="">All</option>{YEARS_RANGES.map(r=><option key={r}>{r}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>SALESPERSON</div><select style={selS} value={filters.salesperson} onChange={e=>setF("salesperson",e.target.value)}><option value="">All</option>{TEAM.map(t=><option key={t}>{t}</option>)}</select></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>FROM</div><input type="date" style={selS} value={filters.dateFrom} onChange={e=>setF("dateFrom",e.target.value)}/></div>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>TO</div><input type="date" style={selS} value={filters.dateTo} onChange={e=>setF("dateTo",e.target.value)}/></div>
    </div>

    <div style={{display:"flex",gap:12,alignItems:"center",flexWrap:"wrap"}}>
      <div><div style={{fontSize:11,fontWeight:700,color:"#8a8780",marginBottom:4}}>GROUP BY</div><select style={selS} value={groupBy} onChange={e=>setGroupBy(e.target.value)}><option value="">None</option><option value="project_type">Project type</option><option value="job_location">Location</option><option value="lead_source">Lead source</option><option value="buying_behavior">Buying behavior</option><option value="years_in_home">Years in home</option><option value="staying_years">Staying years</option><option value="salesperson">Salesperson</option><option value="homeValue">Home value range</option></select></div>
      <button onClick={runReport} style={{...btnP,marginTop:16}}>{loading?"Running...":"Run report"}</button>
      <button onClick={()=>{setFilters({project_type:"",job_location:"",stage:"",lead_source:"",buying_behavior:"",years_in_home:"",staying_years:"",salesperson:"",homeValueRange:"",dateFrom:"",dateTo:""});setGroupBy("");setResults(null);}} style={{...btnSec,marginTop:16}}>Clear</button>
    </div>

    {results&&<div>
      {/* Summary cards */}
      <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:20}}>
        <div style={{background:"#f7f6f3",borderRadius:10,padding:"12px 18px",flex:"1 1 120px"}}><div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>Matching</div><div style={{fontSize:22,fontWeight:600}}>{results.filtered.length}</div></div>
        <div style={{background:"#f7f6f3",borderRadius:10,padding:"12px 18px",flex:"1 1 120px"}}><div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>Sold</div><div style={{fontSize:22,fontWeight:600}}>{results.sold}</div></div>
        <div style={{background:"#f7f6f3",borderRadius:10,padding:"12px 18px",flex:"1 1 120px"}}><div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>Revenue</div><div style={{fontSize:22,fontWeight:600}}>{fmtC(results.rev)}</div></div>
        <div style={{background:"#f7f6f3",borderRadius:10,padding:"12px 18px",flex:"1 1 120px"}}><div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>Avg sale</div><div style={{fontSize:22,fontWeight:600}}>{fmtC(results.avgSale)}</div></div>
        <div style={{background:"#f7f6f3",borderRadius:10,padding:"12px 18px",flex:"1 1 120px"}}><div style={{fontSize:11,fontWeight:700,color:"#8a8780",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>Est $ capture</div><div style={{fontSize:22,fontWeight:600}}>{results.captureRate}%</div></div>
      </div>

      {/* Grouped results */}
      {results.grouped&&<div style={{marginBottom:20}}>
        <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>Results grouped by {groupBy==="homeValue"?"home value":groupBy.replace(/_/g," ")}</div>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:13}}>
          <thead><tr style={{borderBottom:"1px solid #e8e6df"}}>
            <th style={{padding:"6px 8px",textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>Group</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Leads</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Sold</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Revenue</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Close %</th>
          </tr></thead>
          <tbody>{results.grouped.map(([name,v])=><tr key={name} style={{borderBottom:"1px solid #f0eeea"}}>
            <td style={{padding:"8px",fontWeight:500}}>{name}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{v.leads}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{v.sold}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{fmtC(v.rev)}</td>
            <td style={{padding:"8px",textAlign:"right"}}>{v.leads>0?Math.round((v.sold/v.leads)*100)+"%":"—"}</td>
          </tr>)}</tbody>
        </table>
      </div>}

      {/* Project list */}
      <div style={{fontSize:14,fontWeight:600,marginBottom:10,color:"#6b6960"}}>Projects ({results.filtered.length})</div>
      <div style={{maxHeight:500,overflowY:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:13}}>
          <thead><tr style={{borderBottom:"1px solid #e8e6df",position:"sticky",top:0,background:"#fff"}}>
            <th style={{padding:"6px 8px",textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>Client</th>
            <th style={{padding:"6px 8px",textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>Type</th>
            <th style={{padding:"6px 8px",textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>Stage</th>
            <th style={{padding:"6px 8px",textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>Location</th>
            <th style={{padding:"6px 8px",textAlign:"right",color:"#8a8780",fontSize:12,fontWeight:700}}>Sale $</th>
            <th style={{padding:"6px 8px",textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>Date</th>
          </tr></thead>
          <tbody>{results.filtered.slice(0,200).map(p=>{
            const c=contacts[p.contact_id]; const cName=c?`${c.first_name||""} ${c.last_name||""}`.trim():"";
            return <tr key={p.id} onClick={()=>onOpenProject&&onOpenProject(p)} style={{borderBottom:"1px solid #f0eeea",cursor:"pointer"}} onMouseEnter={e=>{e.currentTarget.style.background="#f7f6f3";}} onMouseLeave={e=>{e.currentTarget.style.background="transparent";}}>
              <td style={{padding:"8px",fontWeight:500}}>{cName||p.job_name||"—"}</td>
              <td style={{padding:"8px",color:"#6b6960"}}>{p.project_type||"—"}</td>
              <td style={{padding:"8px"}}><Badge stage={p.stage}/></td>
              <td style={{padding:"8px",color:"#6b6960"}}>{p.job_location||"—"}</td>
              <td style={{padding:"8px",textAlign:"right",fontWeight:p.sale_amount?600:400}}>{p.sale_amount?fmtC(p.sale_amount):"—"}</td>
              <td style={{padding:"8px",color:"#6b6960"}}>{fmtD(p.lead_date)}</td>
            </tr>;
          })}</tbody>
        </table>
        {results.filtered.length>200&&<div style={{padding:12,textAlign:"center",color:"#8a8780",fontSize:12}}>Showing first 200 of {results.filtered.length}</div>}
      </div>
    </div>}
  </div>);
}

/* ── New Project Modal ── */
function NewProjectModal({ onClose, onCreated }) {
  const [mode, setMode] = useState("new");
  const [cSearch, setCSearch] = useState("");
  const [cResults, setCResults] = useState([]);
  const [searching, setSearching] = useState(false);
  const [selContact, setSelContact] = useState(null);
  const [saving, setSaving] = useState(false);
  const [cf, setCf] = useState({first_name:"",last_name:"",email:"",phone_cell:"",phone_home:"",address:"",city:"",state:"IN",zip:"",location:"",spouse_name:"",spouse_email:"",spouse_phone:""});
  const [pf, setPf] = useState({job_name:"",stage:"Not Yet Contacted",project_type:"",lead_source:"",salesperson:TEAM[0],lead_date:todayStr(),estimate_amount:"",confidence:"",buying_behavior:"",years_in_home:"",staying_years:""});
  const setC = k => e => setCf(f=>({...f,[k]:e.target.value}));
  const setCWithLoc = k => e => { const val=e.target.value; setCf(f=>{ const u={...f,[k]:val}; const d=detectLocation(u.address,u.city,u.zip); if(d) u.location=d; return u; }); };
  const setP = k => e => setPf(f=>({...f,[k]:e.target.value}));

  const searchC = () => { if(!cSearch.trim())return; setSearching(true); const enc=encodeURIComponent(cSearch.trim()); sbGet("contacts",`or=(first_name.ilike.*${enc}*,last_name.ilike.*${enc}*,email.ilike.*${enc}*,phone_cell.ilike.*${enc}*,phone_home.ilike.*${enc}*)&limit=10&order=last_name.asc`).then(r=>{setCResults(r||[]);setSearching(false);}); };

  const handleSave = () => { setSaving(true); cacheClear();
    if(mode==="existing"&&selContact){ const jn=pf.job_name||(selContact.last_name||"")+" Residence"; sbInsert("projects",{...pf,job_name:jn,contact_id:selContact.id,estimate_amount:pf.estimate_amount?parseFloat(pf.estimate_amount):null,confidence:pf.confidence?parseInt(pf.confidence):null}).then(r=>{setSaving(false);if(r&&onCreated)onCreated(r);}); }
    else { sbInsert("contacts",cf).then(contact=>{ if(!contact){setSaving(false);return;} const jn=pf.job_name||(cf.last_name||"New")+" Residence"; sbInsert("projects",{...pf,job_name:jn,contact_id:contact.id,estimate_amount:pf.estimate_amount?parseFloat(pf.estimate_amount):null,confidence:pf.confidence?parseInt(pf.confidence):null,job_location:cf.location||null}).then(r=>{setSaving(false);if(r&&onCreated)onCreated(r);}); }); }
  };

  const canSave = mode==="existing"?!!selContact:(cf.first_name||cf.last_name);
  return (<Modal title="New Project" onClose={onClose} width={620}>
    <div style={{display:"flex",gap:4,marginBottom:16}}>
      <button onClick={()=>{setMode("new");setSelContact(null);}} style={{...btnSec,background:mode==="new"?"#185FA5":"#fff",color:mode==="new"?"#fff":"#1a1a1a",fontWeight:600}}>New client</button>
      <button onClick={()=>setMode("existing")} style={{...btnSec,background:mode==="existing"?"#185FA5":"#fff",color:mode==="existing"?"#fff":"#1a1a1a",fontWeight:600}}>Existing client</button>
    </div>
    {mode==="existing"&&<div style={{marginBottom:16}}>
      <div style={{display:"flex",gap:8,marginBottom:8}}><input style={{...inpS,flex:1}} placeholder="Search by name, email, or phone..." value={cSearch} onChange={e=>setCSearch(e.target.value)} onKeyDown={e=>{if(e.key==="Enter")searchC();}}/><button onClick={searchC} style={btnSec}>{searching?"...":"Search"}</button></div>
      {selContact&&<div style={{background:"#E6F1FB",borderRadius:8,padding:"8px 12px",display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}><span style={{fontWeight:600,fontSize:14}}>{`${selContact.first_name||""} ${selContact.last_name||""}`}{selContact.email?` · ${selContact.email}`:""}</span><button onClick={()=>setSelContact(null)} style={{background:"none",border:"none",cursor:"pointer",color:"#791F1F",fontWeight:600,fontSize:13}}>✕</button></div>}
      {!selContact&&cResults.length>0&&<div style={{border:"1px solid #e8e6df",borderRadius:8,maxHeight:180,overflowY:"auto"}}>{cResults.map(c=><div key={c.id} onClick={()=>{setSelContact(c);setCResults([]);}} style={{padding:"8px 12px",cursor:"pointer",borderBottom:"1px solid #f0eeea",fontSize:13}} onMouseEnter={e=>{e.currentTarget.style.background="#f7f6f3";}} onMouseLeave={e=>{e.currentTarget.style.background="transparent";}}><span style={{fontWeight:500}}>{`${c.first_name||""} ${c.last_name||""}`}</span><span style={{color:"#8a8780",marginLeft:8}}>{c.email||""}{c.phone_cell?` · ${c.phone_cell}`:""}</span></div>)}</div>}
    </div>}
    {mode==="new"&&<div style={{marginBottom:16}}>
      <div style={{fontSize:13,fontWeight:700,color:"#6b6960",marginBottom:8,textTransform:"uppercase",letterSpacing:"0.06em"}}>Client</div>
      <div style={{display:"flex",flexWrap:"wrap",gap:10}}>
        <Field label="First name" half><input style={inpS} value={cf.first_name} onChange={setC("first_name")}/></Field>
        <Field label="Last name" half><input style={inpS} value={cf.last_name} onChange={setC("last_name")}/></Field>
        <Field label="Email" half><input style={inpS} value={cf.email} onChange={setC("email")}/></Field>
        <Field label="Cell phone" half><input style={inpS} value={cf.phone_cell} onChange={setC("phone_cell")}/></Field>
        <Field label="Address" half><input style={inpS} value={cf.address} onChange={setCWithLoc("address")}/></Field>
        <Field label="City" half><input style={inpS} value={cf.city} onChange={setCWithLoc("city")}/></Field>
        <Field label="Zip" half><input style={inpS} value={cf.zip} onChange={setCWithLoc("zip")}/></Field>
        <Field label="Location" half><select style={inpS} value={cf.location} onChange={setC("location")}><option value="">Auto-detect or select...</option>{LOCS.map(l=><option key={l}>{l}</option>)}</select></Field>
        <Field label="Spouse/partner name" half><input style={inpS} value={cf.spouse_name} onChange={setC("spouse_name")}/></Field>
        <Field label="Spouse email" half><input style={inpS} value={cf.spouse_email} onChange={setC("spouse_email")}/></Field>
        <Field label="Spouse phone" half><input style={inpS} value={cf.spouse_phone} onChange={setC("spouse_phone")}/></Field>
      </div>
    </div>}
    <div style={{fontSize:13,fontWeight:700,color:"#6b6960",marginBottom:8,textTransform:"uppercase",letterSpacing:"0.06em"}}>Project</div>
    <div style={{display:"flex",flexWrap:"wrap",gap:10}}>
      <Field label="Job name" half><input style={inpS} value={pf.job_name} onChange={setP("job_name")} placeholder="Auto: [Last Name] Residence"/></Field>
      <Field label="Stage" half><select style={inpS} value={pf.stage} onChange={setP("stage")}>{STAGES.map(s=><option key={s}>{s}</option>)}</select></Field>
      <Field label="Project type" half><select style={inpS} value={pf.project_type} onChange={setP("project_type")}><option value="">Select...</option>{PTYPES.map(t=><option key={t}>{t}</option>)}</select></Field>
      <Field label="Lead source" half><select style={inpS} value={pf.lead_source} onChange={setP("lead_source")}><option value="">Select...</option>{LSOURCES.map(s=><option key={s}>{s}</option>)}</select></Field>
      <Field label="Lead date" half><input type="date" style={inpS} value={pf.lead_date} onChange={setP("lead_date")}/></Field>
      <Field label="Salesperson" half><select style={inpS} value={pf.salesperson} onChange={setP("salesperson")}>{TEAM.map(t=><option key={t}>{t}</option>)}</select></Field>
      <Field label="Estimate $" half><input style={inpS} value={pf.estimate_amount} onChange={setP("estimate_amount")} placeholder="$0"/></Field>
      <Field label="Buying behavior" half><select style={inpS} value={pf.buying_behavior} onChange={setP("buying_behavior")}><option value="">Select...</option>{BUYING_BEHAVIOR.map(b=><option key={b.v} value={b.v}>{b.l}</option>)}</select></Field>
      <Field label="Years in home" half><select style={inpS} value={pf.years_in_home} onChange={setP("years_in_home")}><option value="">Select...</option>{YEARS_RANGES.map(r=><option key={r}>{r}</option>)}</select></Field>
      <Field label="Staying how many years" half><select style={inpS} value={pf.staying_years} onChange={setP("staying_years")}><option value="">Select...</option>{YEARS_RANGES.map(r=><option key={r}>{r}</option>)}</select></Field>
    </div>
    <div style={{display:"flex",justifyContent:"flex-end",gap:8,marginTop:20}}><button onClick={onClose} style={btnSec}>Cancel</button><button onClick={handleSave} disabled={!canSave||saving} style={{...btnP,opacity:canSave?1:0.5}}>{saving?"Creating...":"Create project"}</button></div>
  </Modal>);
}

/* ── New Contact Modal ── */
function NewContactModal({ onClose, onCreated }) {
  const [f, setF] = useState({first_name:"",last_name:"",email:"",phone_cell:"",phone_home:"",address:"",city:"",state:"IN",zip:"",location:"",home_value:"",spouse_name:"",spouse_email:"",spouse_phone:""});
  const [saving, setSaving] = useState(false);
  const set = k => e => setF(p=>({...p,[k]:e.target.value}));
  const setWithLoc = k => e => { const val=e.target.value; setF(p=>{ const u={...p,[k]:val}; const d=detectLocation(u.address,u.city,u.zip); if(d) u.location=d; return u; }); };
  const handleSave = () => { if(!f.first_name&&!f.last_name)return; setSaving(true); cacheClear("contacts"); sbInsert("contacts",{...f,home_value:f.home_value?parseFloat(f.home_value):null}).then(r=>{setSaving(false);if(r&&onCreated)onCreated(r);}); };
  return (<Modal title="New Contact" onClose={onClose} width={560}>
    <div style={{display:"flex",flexWrap:"wrap",gap:10}}>
      <Field label="First name" half><input style={inpS} value={f.first_name} onChange={set("first_name")}/></Field>
      <Field label="Last name" half><input style={inpS} value={f.last_name} onChange={set("last_name")}/></Field>
      <Field label="Email" half><input style={inpS} value={f.email} onChange={set("email")}/></Field>
      <Field label="Cell phone" half><input style={inpS} value={f.phone_cell} onChange={set("phone_cell")}/></Field>
      <Field label="Home phone" half><input style={inpS} value={f.phone_home} onChange={set("phone_home")}/></Field>
      <Field label="Address" half><input style={inpS} value={f.address} onChange={setWithLoc("address")}/></Field>
      <Field label="City" half><input style={inpS} value={f.city} onChange={setWithLoc("city")}/></Field>
      <Field label="State" half><input style={inpS} value={f.state} onChange={set("state")}/></Field>
      <Field label="Zip" half><input style={inpS} value={f.zip} onChange={setWithLoc("zip")}/></Field>
      <Field label="Location" half><select style={inpS} value={f.location} onChange={set("location")}><option value="">Auto-detect or select...</option>{LOCS.map(l=><option key={l}>{l}</option>)}</select></Field>
      <Field label="Home value" half><input style={inpS} value={f.home_value} onChange={set("home_value")} placeholder="$0"/></Field>
    </div>
    <div style={{fontSize:13,fontWeight:700,color:"#6b6960",marginBottom:8,marginTop:16,textTransform:"uppercase",letterSpacing:"0.06em"}}>Spouse / Partner</div>
    <div style={{display:"flex",flexWrap:"wrap",gap:10}}>
      <Field label="Name" half><input style={inpS} value={f.spouse_name} onChange={set("spouse_name")}/></Field>
      <Field label="Email" half><input style={inpS} value={f.spouse_email} onChange={set("spouse_email")}/></Field>
      <Field label="Phone" half><input style={inpS} value={f.spouse_phone} onChange={set("spouse_phone")}/></Field>
    </div>
    <div style={{display:"flex",justifyContent:"flex-end",gap:8,marginTop:20}}><button onClick={onClose} style={btnSec}>Cancel</button><button onClick={handleSave} disabled={(!f.first_name&&!f.last_name)||saving} style={{...btnP,opacity:(f.first_name||f.last_name)?1:0.5}}>{saving?"Creating...":"Create contact"}</button></div>
  </Modal>);
}

/* ── Contacts List ── */
function ContactsList({ onOpenContact }) {
  const [contacts, setContacts] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const PAGE = 50;

  const load = useCallback(() => {
    setLoading(true);
    let params = `select=id,first_name,last_name,email,phone_cell,phone_home,location,city,home_value&order=last_name.asc.nullslast,first_name.asc.nullslast&offset=${page*PAGE}&limit=${PAGE}`;
    let countP = "select=id&head=true";
    if (search) { const enc=encodeURIComponent(search.trim()); const f=`&or=(first_name.ilike.*${enc}*,last_name.ilike.*${enc}*,email.ilike.*${enc}*,phone_cell.ilike.*${enc}*,phone_home.ilike.*${enc}*,address.ilike.*${enc}*)`; params+=f; countP+=f; }
    Promise.all([sbGet("contacts",params),sbCount("contacts",countP)]).then(([r,c])=>{setContacts(r||[]);setTotal(c);setLoading(false);});
  }, [page, search]);

  useEffect(() => { load(); }, [load]);

  const doSearch = () => { setPage(0); };
  const totalPages = Math.ceil(total/PAGE);

  return (<div>
    <div style={{display:"flex",gap:8,marginBottom:16,flexWrap:"wrap",alignItems:"center"}}>
      <input placeholder="Search name, email, phone, address..." value={search} onChange={e=>setSearch(e.target.value)} onKeyDown={e=>{if(e.key==="Enter")doSearch();}} style={{...filtS,flex:"1 1 200px"}}/>
      <button onClick={doSearch} style={{...filtS,cursor:"pointer",fontWeight:600}}>Search</button>
      {search&&<button onClick={()=>{setSearch("");setPage(0);}} style={{padding:"6px 12px",borderRadius:8,border:"none",background:"#FCEBEB",color:"#791F1F",cursor:"pointer",fontSize:12,fontWeight:600}}>Clear</button>}
    </div>
    {loading?<table style={{width:"100%",borderCollapse:"collapse"}}><thead><tr style={{borderBottom:"1px solid #e8e6df"}}>{["Name","Email","Phone","Location","Home value"].map(h=><th key={h} style={{padding:8,textAlign:"left",color:"#8a8780",fontSize:12,fontWeight:700}}>{h}</th>)}</tr></thead><tbody>{Array.from({length:8}).map((_,i)=><SkeletonRow key={i} cols={5}/>)}</tbody></table>:
    <div style={{overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:13}}>
        <thead><tr style={{borderBottom:"1px solid #e8e6df",textAlign:"left"}}>{["Name","Email","Phone","Location","Home value"].map(h=><th key={h} style={{padding:8,fontWeight:700,color:"#8a8780",fontSize:12}}>{h}</th>)}</tr></thead>
        <tbody>
          {contacts.length===0&&<tr><td colSpan={5} style={{padding:40,textAlign:"center",color:"#b0ada6"}}>{search?"No contacts match.":"No contacts found."}</td></tr>}
          {contacts.map(c=><tr key={c.id} onClick={()=>onOpenContact(c)} style={{borderBottom:"1px solid #f0eeea",cursor:"pointer"}} onMouseEnter={e=>{e.currentTarget.style.background="#f7f6f3";}} onMouseLeave={e=>{e.currentTarget.style.background="transparent";}}>
            <td style={{padding:"10px 8px",fontWeight:500}}>{`${c.first_name||""} ${c.last_name||""}`}</td>
            <td style={{padding:"10px 8px",color:"#6b6960"}}>{c.email||"—"}</td>
            <td style={{padding:"10px 8px",color:"#6b6960"}}>{c.phone_cell||c.phone_home||"—"}</td>
            <td style={{padding:"10px 8px",color:"#6b6960"}}>{c.location||c.city||"—"}</td>
            <td style={{padding:"10px 8px",color:c.home_value?"#1a1a1a":"#b0ada6"}}>{c.home_value?fmtC(c.home_value):"—"}</td>
          </tr>)}
        </tbody>
      </table>
      {totalPages>1&&<div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:12,fontSize:13}}>
        <span style={{color:"#8a8780"}}>{`Page ${page+1} of ${totalPages} (${total.toLocaleString()} contacts)`}</span>
        <div style={{display:"flex",gap:8}}>
          <button onClick={()=>setPage(Math.max(0,page-1))} disabled={page===0} style={{...filtS,cursor:page>0?"pointer":"default",opacity:page>0?1:0.4}}>Previous</button>
          <button onClick={()=>setPage(Math.min(totalPages-1,page+1))} disabled={page>=totalPages-1} style={{...filtS,cursor:page<totalPages-1?"pointer":"default",opacity:page<totalPages-1?1:0.4}}>Next</button>
        </div>
      </div>}
    </div>}
  </div>);
}

/* ══════════════════════════════════════════
   MAIN APP
   ══════════════════════════════════════════ */
export default function App() {
  const [section, setSection] = useState("projects");
  const [projectView, setProjectView] = useState("list");
  const [detailView, setDetailView] = useState(null);
  const [showNewProject, setShowNewProject] = useState(false);
  const [showNewContact, setShowNewContact] = useState(false);

  const [projects, setProjects] = useState([]);
  const [projectContacts, setProjectContacts] = useState({});
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [fStage, setFStage] = useState("");
  const [fType, setFType] = useState("");
  const [fLoc, setFLoc] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [sortCol, setSortCol] = useState("lead_date");
  const [sortDir, setSortDir] = useState("desc");
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 50;

  const loadContactNames = (projectList) => {
    const cids = {};
    (projectList||[]).forEach(p => { if(p.contact_id) cids[p.contact_id]=true; });
    const idList = Object.keys(cids);
    if (idList.length === 0) return;
    const batches = []; for(let i=0;i<idList.length;i+=200) batches.push(idList.slice(i,i+200));
    Promise.all(batches.map(b => sbGetCached("contacts",`id=in.(${b.join(",")})&select=id,first_name,last_name`))).then(results => {
      const cmap = {};
      results.forEach(b => (b||[]).forEach(c => { cmap[c.id] = c; }));
      setProjectContacts(prev => ({...prev, ...cmap}));
    });
  };

  const loadProjects = useCallback(() => {
    setLoading(true);
    let params = `select=${LIST_COLS}&order=${sortCol}.${sortDir}.nullslast&offset=${page*PAGE_SIZE}&limit=${PAGE_SIZE}`;
    let countP = "select=id&head=true";

    if (fStage) { params+=`&stage=eq.${encodeURIComponent(fStage)}`; countP+=`&stage=eq.${encodeURIComponent(fStage)}`; }
    if (fType) { params+=`&project_type=eq.${encodeURIComponent(fType)}`; countP+=`&project_type=eq.${encodeURIComponent(fType)}`; }
    if (fLoc) { params+=`&job_location=eq.${encodeURIComponent(fLoc)}`; countP+=`&job_location=eq.${encodeURIComponent(fLoc)}`; }
    if (dateFrom) { params+=`&lead_date=gte.${dateFrom}`; countP+=`&lead_date=gte.${dateFrom}`; }
    if (dateTo) { params+=`&lead_date=lte.${dateTo}`; countP+=`&lead_date=lte.${dateTo}`; }

    if (search && search.trim()) {
      const enc = encodeURIComponent(search.trim());
      sbGet("contacts",`or=(first_name.ilike.*${enc}*,last_name.ilike.*${enc}*,phone_cell.ilike.*${enc}*,phone_home.ilike.*${enc}*,email.ilike.*${enc}*,address.ilike.*${enc}*)&select=id&limit=200`)
      .then(contactResults => {
        const cids = (contactResults||[]).map(c=>c.id);
        let orParts = `job_name.ilike.*${enc}*,lead_source.ilike.*${enc}*`;
        if (cids.length>0) orParts+=`,contact_id.in.(${cids.join(",")})`;
        const sf = `&or=(${orParts})`;
        Promise.all([sbGet("projects",params+sf),sbCount("projects",countP+sf)]).then(([r,c])=>{setProjects(r||[]);setTotalCount(c);setLoading(false);loadContactNames(r);});
      });
    } else {
      Promise.all([sbGet("projects",params),sbCount("projects",countP)]).then(([r,c])=>{setProjects(r||[]);setTotalCount(c);setLoading(false);loadContactNames(r);});
    }
  }, [page, sortCol, sortDir, fStage, fType, fLoc, dateFrom, dateTo, search]);

  useEffect(() => { loadProjects(); }, [loadProjects]);

  const doSearch = () => { setPage(0); };
  const toggleSort = col => { const dbCol = col === "client" ? "job_name" : col; if(sortCol===dbCol) setSortDir(d=>d==="asc"?"desc":"asc"); else { setSortCol(dbCol); setSortDir("desc"); } setPage(0); };
  const hasF = search||fStage||fType||fLoc||dateFrom||dateTo;
  const clearFilters = () => { setSearch(""); setFStage(""); setFType(""); setFLoc(""); setDateFrom(""); setDateTo(""); setPage(0); };
  const totalPages = Math.ceil(totalCount/PAGE_SIZE);
  const openProject = p => setDetailView({type:"project",data:p});
  const openContact = c => setDetailView({type:"contact",data:c});

  if (detailView) {
    if (detailView.type==="project") return <div style={{maxWidth:800,margin:"0 auto",padding:"16px 0"}}><ProjectDetail project={detailView.data} onBack={()=>setDetailView(null)} onSaved={()=>{cacheClear("projects");loadProjects();}}/></div>;
    if (detailView.type==="contact") return <div style={{maxWidth:800,margin:"0 auto",padding:"16px 0"}}><ContactDetail contact={detailView.data} onBack={()=>setDetailView(null)} onSaved={()=>{}} onOpenProject={p=>setDetailView({type:"project",data:p})}/></div>;
  }

  const tableCols = [{k:"client",l:"Client"},{k:"stage",l:"Stage"},{k:"project_type",l:"Type"},{k:"job_location",l:"Location"},{k:"lead_source",l:"Source"},{k:"lead_date",l:"Lead date"},{k:"sale_amount",l:"Sale $"}];

  return (<div style={{maxWidth:1200,margin:"0 auto"}}>
    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:12,marginBottom:16}}>
      <div style={{display:"flex",alignItems:"center",gap:16}}>
        <h1 style={{margin:0,fontSize:22,fontWeight:700,letterSpacing:"-0.03em",color:"#1a1a1a"}}>TLG<span style={{color:"#185FA5",marginLeft:4}}>CRM</span></h1>
        <div style={{display:"flex",gap:4,background:"#f0eeea",borderRadius:10,padding:3}}>
          <TopTab active={section==="projects"} onClick={()=>{setSection("projects");setProjectView("list");}}>Projects</TopTab>
          <TopTab active={section==="contacts"} onClick={()=>setSection("contacts")}>Contacts</TopTab>
        </div>
      </div>
      <div style={{display:"flex",gap:8}}>
        <button onClick={()=>setShowNewProject(true)} style={{...btnP,fontSize:13,padding:"7px 16px"}}>+ Project</button>
        <button onClick={()=>setShowNewContact(true)} style={{...btnSec,fontSize:13,padding:"7px 16px"}}>+ Contact</button>
      </div>
    </div>

    {showNewProject&&<NewProjectModal onClose={()=>setShowNewProject(false)} onCreated={p=>{setShowNewProject(false);cacheClear();openProject(p);}}/>}
    {showNewContact&&<NewContactModal onClose={()=>setShowNewContact(false)} onCreated={c=>{setShowNewContact(false);cacheClear();openContact(c);}}/>}

    {section==="contacts"&&<ContactsList onOpenContact={openContact}/>}

    {section==="projects"&&<div>
      <div style={{display:"flex",gap:0,marginBottom:16,borderBottom:"1px solid #e8e6df"}}>
        <Pill active={projectView==="list"} onClick={()=>setProjectView("list")}>List</Pill>
        <Pill active={projectView==="pipeline"} onClick={()=>setProjectView("pipeline")}>Pipeline</Pill>
        <Pill active={projectView==="dashboard"} onClick={()=>setProjectView("dashboard")}>Dashboard</Pill>
        <Pill active={projectView==="reports"} onClick={()=>setProjectView("reports")}>Reports</Pill>
      </div>
      {projectView==="dashboard"&&<Dashboard onOpenProject={openProject}/>}
      {projectView==="pipeline"&&<PipelineView onOpenProject={openProject}/>}
      {projectView==="reports"&&<Reports onOpenProject={openProject}/>}
      {projectView==="list"&&<div>
        <div style={{fontSize:13,color:"#8a8780",marginBottom:12}}>{`${totalCount.toLocaleString()} projects${hasF?" (filtered)":""}`}</div>
        <div style={{display:"flex",gap:8,marginBottom:16,flexWrap:"wrap",alignItems:"center"}}>
          <input placeholder="Search jobs, clients, phone, email..." value={search} onChange={e=>setSearch(e.target.value)} onKeyDown={e=>{if(e.key==="Enter")doSearch();}} style={{...filtS,flex:"1 1 180px",minWidth:120}}/>
          <button onClick={doSearch} style={{...filtS,cursor:"pointer",fontWeight:600}}>Search</button>
          <select style={filtS} value={fStage} onChange={e=>{setFStage(e.target.value);setPage(0);}}><option value="">All stages</option>{STAGES.map(s=><option key={s}>{s}</option>)}</select>
          <select style={filtS} value={fType} onChange={e=>{setFType(e.target.value);setPage(0);}}><option value="">All types</option>{PTYPES.map(t=><option key={t}>{t}</option>)}</select>
          <select style={filtS} value={fLoc} onChange={e=>{setFLoc(e.target.value);setPage(0);}}><option value="">All locations</option>{LOCS.map(l=><option key={l}>{l}</option>)}</select>
          <div style={{display:"flex",alignItems:"center",gap:4}}><span style={{fontSize:12,color:"#8a8780"}}>From</span><input type="date" value={dateFrom} onChange={e=>{setDateFrom(e.target.value);setPage(0);}} style={{...filtS,width:150}}/></div>
          <div style={{display:"flex",alignItems:"center",gap:4}}><span style={{fontSize:12,color:"#8a8780"}}>To</span><input type="date" value={dateTo} onChange={e=>{setDateTo(e.target.value);setPage(0);}} style={{...filtS,width:150}}/></div>
          {hasF&&<button onClick={clearFilters} style={{padding:"6px 12px",borderRadius:8,border:"none",background:"#FCEBEB",color:"#791F1F",cursor:"pointer",fontSize:12,fontWeight:600}}>Clear</button>}
        </div>
        {loading?<table style={{width:"100%",borderCollapse:"collapse"}}><thead><tr style={{borderBottom:"1px solid #e8e6df",textAlign:"left"}}>{tableCols.map(col=><th key={col.k} style={{padding:8,fontWeight:700,color:"#8a8780",fontSize:12}}>{col.l}</th>)}</tr></thead><tbody>{Array.from({length:10}).map((_,i)=><SkeletonRow key={i} cols={7}/>)}</tbody></table>:
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:13}}>
            <thead><tr style={{borderBottom:"1px solid #e8e6df",textAlign:"left"}}>{tableCols.map(col=><th key={col.k} onClick={()=>toggleSort(col.k)} style={{padding:8,fontWeight:700,color:"#8a8780",fontSize:12,cursor:"pointer",whiteSpace:"nowrap",userSelect:"none"}}>{col.l}{sortCol===col.k&&<span style={{marginLeft:4}}>{sortDir==="asc"?"↑":"↓"}</span>}</th>)}</tr></thead>
            <tbody>
              {projects.length===0&&<tr><td colSpan={7} style={{padding:40,textAlign:"center",color:"#b0ada6"}}>{hasF?"No projects match.":"No projects found."}</td></tr>}
              {projects.map(p=>{const ct=projectContacts[p.contact_id]; const clientName=ct?`${ct.first_name||""} ${ct.last_name||""}`.trim():""; return <tr key={p.id} onClick={()=>openProject(p)} style={{borderBottom:"1px solid #f0eeea",cursor:"pointer"}} onMouseEnter={e=>{e.currentTarget.style.background="#f7f6f3";}} onMouseLeave={e=>{e.currentTarget.style.background="transparent";}}>
                <td style={{padding:"10px 8px"}}><div style={{fontWeight:500}}>{clientName||p.job_name||"—"}</div>{(clientName&&p.job_name&&p.job_name!==clientName)?<div style={{fontSize:12,color:"#8a8780",marginTop:1}}>{p.job_name}</div>:null}</td>
                <td style={{padding:"10px 8px"}}><Badge stage={p.stage}/></td>
                <td style={{padding:"10px 8px",color:"#6b6960"}}>{p.project_type||"—"}</td>
                <td style={{padding:"10px 8px",color:"#6b6960"}}>{p.job_location||"—"}</td>
                <td style={{padding:"10px 8px",color:"#6b6960"}}>{p.lead_source||"—"}</td>
                <td style={{padding:"10px 8px",color:"#6b6960"}}>{fmtD(p.lead_date)}</td>
                <td style={{padding:"10px 8px",fontWeight:p.sale_amount?600:400,color:p.sale_amount?"#1a1a1a":"#b0ada6"}}>{p.sale_amount?fmtC(p.sale_amount):"—"}</td>
              </tr>})}
            </tbody>
          </table>
          {totalPages>1&&<div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:12,fontSize:13}}>
            <span style={{color:"#8a8780"}}>{`Page ${page+1} of ${totalPages} (${totalCount.toLocaleString()} projects)`}</span>
            <div style={{display:"flex",gap:8}}>
              <button onClick={()=>setPage(Math.max(0,page-1))} disabled={page===0} style={{...filtS,cursor:page>0?"pointer":"default",opacity:page>0?1:0.4}}>Previous</button>
              <button onClick={()=>setPage(Math.min(totalPages-1,page+1))} disabled={page>=totalPages-1} style={{...filtS,cursor:page<totalPages-1?"pointer":"default",opacity:page<totalPages-1?1:0.4}}>Next</button>
            </div>
          </div>}
        </div>}
      </div>}
    </div>}
  </div>);
}
