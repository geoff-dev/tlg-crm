import { useEffect, useMemo, useRef, useState } from "react"
import backgroundUrl from "./background.jpg"
import anchors from "./anchors.json"
import { initialStress, initialYear, loadYear, yearsInFixture } from "./data.js"
import {
  STONE,
  checkLayout,
  layoutScene,
  layoutSignature,
  bounds,
  clamp,
} from "./layout.js"

const FULL = { x: 0, y: 0, w: 1280, h: 720 }
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

function formatDate(iso) {
  if (!iso || typeof iso !== "string" || iso.length < 10) return null
  const [year, month, day] = iso.slice(0, 10).split("-").map(Number)
  if (!year || !month || !day) return null
  return `${MONTHS[month - 1]} ${day}, ${year}`
}

function money(value) {
  if (value === null || value === undefined || value === "") return "—"
  const n = Number(value)
  if (!Number.isFinite(n)) return "—"
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n)
}

function contactOf(project, contacts) {
  if (!project) return null
  return contacts[project.contact_id] || null
}

function personName(project, contacts) {
  const contact = contactOf(project, contacts)
  if (!contact) return project?.job_name || "Unknown"
  return [contact.first_name, contact.last_name].filter(Boolean).join(" ") || project.job_name || "Unknown"
}

function lastName(project, contacts) {
  const contact = contactOf(project, contacts)
  return (contact?.last_name || project?.job_name || "Unknown").toUpperCase()
}

function blossomColors(status) {
  if (status === "Approved") return { petal: "#e56b93", alt: "#ffb0c8", center: "#ffd56a" }
  if (status === "Declined") return { petal: "#9a9691", alt: "#b7b3ad", center: "#d9d4cc" }
  return { petal: "#e2a23a", alt: "#f0c36a", center: "#fff1c4" }
}

function clampView(view) {
  const w = clamp(view.w, 140, 1280)
  const h = w * (720 / 1280)
  const x = clamp(view.x, -30, 1280 - w + 30)
  const y = clamp(view.y, -30, 720 - h + 30)
  return { x, y, w, h }
}

function framePolygon(poly, pad = 36) {
  const box = bounds(poly)
  let w = box.maxX - box.minX + pad * 2
  let h = box.maxY - box.minY + pad * 2
  const aspect = 1280 / 720
  if (w / h < aspect) w = h * aspect
  else h = w / aspect
  return clampView({
    x: (box.minX + box.maxX) / 2 - w / 2,
    y: (box.minY + box.maxY) / 2 - h / 2,
    w,
    h,
  })
}

function stonePath(scale) {
  const w = STONE.w * scale
  const h = STONE.h * scale
  const hw = w / 2
  const r = w / 2
  const shoulder = h - r
  return `M ${-hw} 0 L ${-hw} ${-shoulder} A ${r} ${r} 0 0 1 ${hw} ${-shoulder} L ${hw} 0 Z`
}

function Leaf({ mark }) {
  const light = 34 + (mark.hue % 12)
  return (
    <g
      className="hit"
      data-kind="leaf"
      data-id={mark.id}
      transform={`translate(${mark.x} ${mark.y}) rotate(${mark.rot}) scale(${mark.r * 0.72})`}
      filter="url(#soft)"
    >
      <path
        d="M0,-1.2 C0.85,-0.9 1.15,-0.2 0.2,1.05 C-0.05,0.35 -0.55,-0.05 -1.05,-0.5 C-0.75,-1.05 -0.4,-1.25 0,-1.2 Z"
        fill={`hsl(${mark.hue} 46% ${light}%)`}
        stroke={`hsl(${mark.hue} 40% 18%)`}
        strokeWidth="0.08"
      />
      <path d="M0,-0.9 C-0.05,0 0.05,0.4 0.12,0.85" fill="none" stroke={`hsl(${mark.hue} 35% 16%)`} strokeWidth="0.06" />
    </g>
  )
}

function Blossom({ mark }) {
  const colors = blossomColors(mark.changeOrder.status)
  const r = mark.r
  const petals = [0, 1, 2, 3, 4].map((i) => {
    const angle = (i / 5) * Math.PI * 2 - Math.PI / 2
    const px = Math.cos(angle) * r * 0.52
    const py = Math.sin(angle) * r * 0.52
    const deg = (angle * 180) / Math.PI
    return (
      <ellipse
        key={i}
        cx={px}
        cy={py}
        rx={r * 0.46}
        ry={r * 0.28}
        transform={`rotate(${deg} ${px} ${py})`}
        fill={i % 2 ? colors.petal : colors.alt}
      />
    )
  })
  return (
    <g className="hit" data-kind="blossom" data-id={mark.id} transform={`translate(${mark.x} ${mark.y}) rotate(${mark.rot})`} filter="url(#soft)" opacity={mark.changeOrder.status === "Declined" ? 0.88 : 1}>
      {petals}
      <circle cx="0" cy="0" r={r * 0.24} fill={colors.center} />
    </g>
  )
}

function Headstone({ mark, contacts }) {
  const project = mark.project
  const name = lastName(project, contacts)
  const born = formatDate(project.lead_date)
  const died = formatDate(project.date_lost)
  const reason = project.lost_reason || ""
  const w = STONE.w * mark.scale
  const h = STONE.h * mark.scale
  const nameSize = clamp(w / Math.max(name.length, 6) * 1.25, 3.4, 6.2)
  const dateSize = Math.max(2.8, Math.min(4.2, nameSize * 0.68))
  const reasonSize = Math.max(2.5, dateSize * 0.86)
  const reasonShort = reason.length > 18 ? `${reason.slice(0, 17)}…` : reason
  const inner = Math.max(12, w - 7)
  const bornLine = `b. ${born || "—"}`
  const diedLine = `d. ${died || "—"}`
  return (
    <g className="hit" data-kind="headstone" data-id={mark.id} transform={`translate(${mark.x} ${mark.y})`} filter="url(#stoneShadow)">
      <path d={stonePath(mark.scale)} fill="#d9d3c8" stroke="#6e675e" strokeWidth="0.6" />
      <text x="0" y={-h + nameSize + 2.6} textAnchor="middle" fill="#2a2622" fontFamily="Georgia, serif" fontSize={nameSize} fontWeight="700" textLength={name.length > 8 ? inner : undefined} lengthAdjust="spacingAndGlyphs">
        {name}
      </text>
      <text x="0" y={-h + nameSize + dateSize + 4.2} textAnchor="middle" fill="#4a453f" fontFamily="Georgia, serif" fontSize={dateSize} textLength={inner} lengthAdjust="spacingAndGlyphs">
        {bornLine}
      </text>
      <text x="0" y={-h + nameSize + dateSize * 2 + 5.4} textAnchor="middle" fill="#4a453f" fontFamily="Georgia, serif" fontSize={dateSize} textLength={inner} lengthAdjust="spacingAndGlyphs">
        {diedLine}
      </text>
      <text x="0" y={-h + nameSize + dateSize * 2 + reasonSize + 7.4} textAnchor="middle" fill="#5c564f" fontFamily="Georgia, serif" fontSize={reasonSize} fontStyle="italic" textLength={reasonShort.length > 12 ? inner : undefined} lengthAdjust="spacingAndGlyphs">
        {reasonShort || "—"}
      </text>
    </g>
  )
}

function fact(label, value) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{value || "—"}</dd>
    </>
  )
}

function DetailPanel({ item, data, onClose }) {
  if (!item) return null
  const contacts = data.contacts
  let project = item.project
  let changeOrder = item.changeOrder
  if (changeOrder && !project) project = data.projectsById[changeOrder.project_id]
  const title = changeOrder
    ? changeOrder.description || changeOrder.co_type || "Change order"
    : personName(project, contacts)
  const kicker = changeOrder ? `Change order · ${changeOrder.status}` : project?.stage || ""
  const linkId = project?.id || changeOrder?.project_id
  return (
    <aside className="panel" role="dialog" aria-label="Project detail">
      <header>
        <div>
          <p className="kicker">{kicker}</p>
          <h2>{title}</h2>
        </div>
        <button className="close" type="button" onClick={onClose} aria-label="Close detail">×</button>
      </header>
      {changeOrder && (
        <dl className="facts">
          {fact("Type", changeOrder.co_type)}
          {fact("Status", changeOrder.status)}
          {fact("Estimate", money(changeOrder.estimate_amount))}
          {fact("Sale", money(changeOrder.sale_amount))}
          {fact("Estimated", formatDate(changeOrder.date_estimated))}
          {fact("Decided", formatDate(changeOrder.date_decided))}
          {fact("Created by", changeOrder.created_by)}
          {fact("Parent job", project ? personName(project, contacts) : changeOrder.project_id)}
        </dl>
      )}
      {project && (
        <dl className="facts">
          {fact("Client", personName(project, contacts))}
          {fact("Job", project.job_name)}
          {fact("Stage", project.stage)}
          {fact("Type", project.project_type)}
          {fact("Location", project.job_location)}
          {fact("Lead source", project.lead_source)}
          {fact("Salesperson", project.salesperson)}
          {fact("Lead date", formatDate(project.lead_date))}
          {fact("Sold", formatDate(project.date_sold))}
          {fact("Lost", formatDate(project.date_lost))}
          {fact("Stage lost", project.stage_lost)}
          {fact("Sale", money(project.sale_amount))}
          {fact("Estimate", money(project.estimate_amount))}
          {fact("Forecast", money(project.forecast_amount))}
          {fact("Buying", project.buying_behavior)}
          {fact("Years in home", project.years_in_home)}
          {fact("Staying", project.staying_years)}
          {fact("Reason", project.lost_reason)}
        </dl>
      )}
      {linkId && (
        <p>
          <a href={`/?project=${encodeURIComponent(linkId)}`}>Open in CRM (?project={linkId})</a>
        </p>
      )}
      <p className="note">The main app does not read ?project= yet, so this panel is the demo.</p>
    </aside>
  )
}

function Tooltip({ hover, data }) {
  if (!hover) return null
  const { item, x, y } = hover
  const contacts = data.contacts
  let title = ""
  let lines = []
  if (item.kind === "leaf") {
    const project = item.project
    title = personName(project, contacts)
    lines = [
      project.job_name,
      money(project.sale_amount),
      `Sold ${formatDate(project.date_sold) || "—"}`,
      `Lead ${formatDate(project.lead_date) || "—"}`,
      project.salesperson,
      project.stage,
    ]
  } else if (item.kind === "blossom") {
    const changeOrder = item.changeOrder
    const parent = data.projectsById[changeOrder.project_id]
    title = changeOrder.description || "Change order"
    lines = [
      changeOrder.status,
      money(changeOrder.estimate_amount ?? changeOrder.sale_amount),
      `Estimated ${formatDate(changeOrder.date_estimated) || "—"}`,
      changeOrder.date_decided ? `Decided ${formatDate(changeOrder.date_decided)}` : null,
      parent ? personName(parent, contacts) : null,
      changeOrder.created_by,
    ]
  } else {
    const project = item.project
    title = lastName(project, contacts)
    lines = [
      project.job_name,
      project.stage_lost ? `Lost at ${project.stage_lost}` : "Lost",
      `b. ${formatDate(project.lead_date) || "—"}`,
      `d. ${formatDate(project.date_lost) || "—"}`,
      project.salesperson,
      project.lost_reason,
    ]
  }
  const left = Math.min(x + 16, window.innerWidth - 276)
  const top = Math.min(y + 16, window.innerHeight - 200)
  return (
    <div className="tooltip" style={{ left, top }} role="tooltip">
      <strong>{title}</strong>
      {lines.filter(Boolean).map((line) => (
        <div key={line} className={line === lines[lines.length - 1] ? "reason" : undefined}>{line}</div>
      ))}
    </div>
  )
}

export default function TreeOfLife() {
  const years = useMemo(() => yearsInFixture(), [])
  const [year, setYear] = useState(initialYear)
  const [stress, setStress] = useState(initialStress)
  const [data, setData] = useState(null)
  const [error, setError] = useState("")
  const [page, setPage] = useState(0)
  const [view, setView] = useState(FULL)
  const [hover, setHover] = useState(null)
  const [selected, setSelected] = useState(null)
  const [dragging, setDragging] = useState(false)
  const svgRef = useRef(null)
  const dragRef = useRef(null)

  useEffect(() => {
    let cancel = false
    setError("")
    loadYear(year, stress).then((next) => {
      if (cancel) return
      setData(next)
      setPage(0)
      setSelected(null)
    }).catch((err) => {
      if (!cancel) setError(err.message || "Could not load the year")
    })
    return () => { cancel = true }
  }, [year, stress])

  const layout = useMemo(() => (data ? layoutScene(data, anchors) : null), [data])
  const problems = useMemo(() => (layout && data ? checkLayout(layout, data, anchors) : []), [layout, data])
  const signature = useMemo(() => (layout ? layoutSignature(layout) : ""), [layout])

  useEffect(() => {
    const svg = svgRef.current
    if (!svg) return undefined
    const onWheel = (event) => {
      event.preventDefault()
      const rect = svg.getBoundingClientRect()
      const px = (event.clientX - rect.left) / rect.width
      const py = (event.clientY - rect.top) / rect.height
      const factor = event.deltaY > 0 ? 1.12 : 1 / 1.12
      setView((current) => {
        const w = clamp(current.w * factor, 140, 1280)
        const h = w * (720 / 1280)
        const cx = current.x + px * current.w
        const cy = current.y + py * current.h
        return clampView({ x: cx - px * w, y: cy - py * h, w, h })
      })
    }
    svg.addEventListener("wheel", onWheel, { passive: false })
    return () => svg.removeEventListener("wheel", onWheel)
  }, [data])

  useEffect(() => {
    const onKey = (event) => {
      if (event.key === "Escape") setSelected(null)
      if (event.key === "+" || event.key === "=") setView((current) => clampView({ ...current, w: current.w / 1.2, h: (current.w / 1.2) * (720 / 1280), x: current.x + current.w * 0.08, y: current.y + current.h * 0.08 }))
      if (event.key === "-" || event.key === "_") setView((current) => clampView({ ...current, w: current.w * 1.2 }))
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [])

  function hitTarget(event) {
    return event.target?.closest?.("[data-kind]") || null
  }

  function onPointerDown(event) {
    const hit = hitTarget(event)
    dragRef.current = {
      x: event.clientX,
      y: event.clientY,
      moved: false,
      view,
      kind: hit?.dataset.kind,
      id: hit?.dataset.id,
    }
    setDragging(true)
    event.currentTarget.setPointerCapture(event.pointerId)
  }

  function onPointerMove(event) {
    const drag = dragRef.current
    if (!drag || !svgRef.current) return
    const dx = event.clientX - drag.x
    const dy = event.clientY - drag.y
    if (Math.hypot(dx, dy) > 4) drag.moved = true
    if (drag.moved) {
      const rect = svgRef.current.getBoundingClientRect()
      setView(clampView({
        ...drag.view,
        x: drag.view.x - dx * (drag.view.w / rect.width),
        y: drag.view.y - dy * (drag.view.h / rect.height),
      }))
      setHover(null)
    }
  }

  function onPointerUp() {
    const drag = dragRef.current
    dragRef.current = null
    setDragging(false)
    if (drag && !drag.moved && drag.id && layout) {
      const list = drag.kind === "leaf" ? layout.leaves : drag.kind === "blossom" ? layout.blossoms : layout.headstones
      const item = list.find((entry) => entry.id === drag.id)
      if (item) setSelected(item)
    }
  }

  function onPointerOver(event) {
    const hit = hitTarget(event)
    if (!hit || !layout || dragRef.current?.moved) {
      setHover(null)
      return
    }
    const list = hit.dataset.kind === "leaf" ? layout.leaves : hit.dataset.kind === "blossom" ? layout.blossoms : layout.headstones
    const item = list.find((entry) => entry.id === hit.dataset.id)
    if (item) setHover({ item, x: event.clientX, y: event.clientY })
  }

  function onPointerOut(event) {
    if (!event.relatedTarget || !hitTarget({ target: event.relatedTarget })) setHover(null)
  }

  const visibleStones = layout ? layout.headstones.filter((stone) => stone.page === page) : []
  const pageCount = layout?.pageCount || 1
  const moved = view.w < 1279 || Math.abs(view.x) > 0.5 || Math.abs(view.y) > 0.5

  return (
    <div className="app">
      <div className={data?.source === "live" ? "banner live" : "banner"}>
        {data?.source === "live"
          ? "Local dev only · read-only GET · real names are on this screen"
          : "Prototype · sample fixture · nothing is written"}
      </div>
      <div className="toolbar">
        <div className="brand">
          <strong>{year} · Life & Death</strong>
          <span>
            {data ? `${data.sold.length} sold${data.carryoverSold ? ` (${data.carryoverSold} carried over)` : ""} · ${data.lost.length} lost · ${data.changeOrders.length} change orders` : "Loading"}
          </span>
        </div>
        <label className="field">
          Year
          <select value={year} onChange={(event) => { setYear(Number(event.target.value)); setView(FULL) }}>
            {years.map((entry) => <option key={entry} value={entry}>{entry}</option>)}
          </select>
        </label>
        <label className="field">
          Leaves
          <select value={stress} onChange={(event) => setStress(event.target.value)}>
            <option value="auto">Sample year</option>
            <option value="1">1 leaf</option>
            <option value="120">120 leaves</option>
            <option value="500">500 leaves</option>
          </select>
        </label>
        <div className="pager">
          <button type="button" onClick={() => setPage((current) => Math.max(0, current - 1))} disabled={page === 0}>Prev</button>
          <span className="summary">Cemetery {page + 1} / {pageCount}</span>
          <button type="button" onClick={() => setPage((current) => Math.min(pageCount - 1, current + 1))} disabled={page >= pageCount - 1}>Next</button>
        </div>
        <div className="zoom">
          <button type="button" onClick={() => setView((current) => clampView({ ...current, w: current.w / 1.25, x: current.x + current.w * 0.1, y: current.y + current.h * 0.1 }))} aria-label="Zoom in">+</button>
          <button type="button" onClick={() => setView((current) => clampView({ ...current, w: current.w * 1.25 }))} aria-label="Zoom out">−</button>
          <button type="button" onClick={() => setView(FULL)} disabled={!moved}>Reset</button>
          <button type="button" onClick={() => setView(framePolygon(anchors.canopy))}>Canopy</button>
          <button type="button" onClick={() => setView(framePolygon(anchors.cemetery, 24))}>Cemetery</button>
        </div>
        <div className="legend">
          <span><i className="swatch" style={{ background: "#3f8f45" }} />Sold leaf</span>
          <span><i className="swatch" style={{ background: "#e56b93" }} />Approved</span>
          <span><i className="swatch" style={{ background: "#e2a23a" }} />Estimated / pending</span>
          <span><i className="swatch" style={{ background: "#9a9691" }} />Declined</span>
          <span><i className="swatch" style={{ background: "#d9d3c8" }} />Lost</span>
        </div>
      </div>
      <div className="stage-wrap">
        {error && <div className="empty">{error}</div>}
        {!error && !data && <div className="empty">Loading the year…</div>}
        {data && layout && (
          <svg
            ref={svgRef}
            className={dragging ? "stage dragging" : "stage"}
            viewBox={`${view.x} ${view.y} ${view.w} ${view.h}`}
            onPointerDown={onPointerDown}
            onPointerMove={onPointerMove}
            onPointerUp={onPointerUp}
            onPointerCancel={onPointerUp}
            onPointerOver={onPointerOver}
            onPointerOut={onPointerOut}
            role="application"
            aria-label="Tree of life"
          >
            <defs>
              <filter id="soft" x="-50%" y="-50%" width="200%" height="200%">
                <feDropShadow dx="0.4" dy="0.8" stdDeviation="0.7" floodColor="#1a1408" floodOpacity="0.45" />
              </filter>
              <filter id="stoneShadow" x="-40%" y="-30%" width="180%" height="170%">
                <feDropShadow dx="1" dy="1.4" stdDeviation="1.1" floodColor="#1a1408" floodOpacity="0.4" />
              </filter>
            </defs>
            <image href={backgroundUrl} x="0" y="0" width="1280" height="720" />
            <g>
              {layout.blossoms.map((mark) => <Blossom key={mark.id} mark={mark} />)}
            </g>
            <g>
              {layout.leaves.map((mark) => <Leaf key={mark.id} mark={mark} />)}
            </g>
            <g>
              {visibleStones.map((mark) => <Headstone key={mark.id} mark={mark} contacts={data.contacts} />)}
            </g>
            {selected && (
              <circle
                cx={selected.x}
                cy={selected.kind === "headstone" ? selected.y - 20 : selected.y}
                r={selected.kind === "headstone" ? 28 * selected.scale : (selected.r || 8) + 4}
                fill="none"
                stroke="#f2d48a"
                strokeWidth={view.w / 400}
              />
            )}
          </svg>
        )}
        {layout && data && (
          <div className={problems.length ? "counter warn" : "counter"}>
            {`Leaves ${layout.leaves.length}/${data.sold.length} · Blossoms ${layout.blossoms.length}/${data.changeOrders.length} · Headstones ${layout.headstones.length}/${data.lost.length} · page ${page + 1} shows ${visibleStones.length} · layout ${signature}`}
            {data.sold.length >= 80 && " · Zoom into the canopy to read a crowded year."}
            {problems.length > 0 && ` · ${problems.slice(0, 3).join("; ")}`}
          </div>
        )}
        {hover && data && <Tooltip hover={hover} data={data} />}
        {selected && data && <DetailPanel item={selected} data={data} onClose={() => setSelected(null)} />}
      </div>
    </div>
  )
}
