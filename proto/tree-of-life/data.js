import fixture from "./fixture.json"

const DASH_COLS = [
  "id",
  "job_name",
  "stage",
  "project_type",
  "job_location",
  "lead_source",
  "lead_date",
  "sale_amount",
  "estimate_amount",
  "estimate_date",
  "date_sold",
  "date_lost",
  "stage_lost",
  "lost_reason",
  "contact_id",
  "salesperson",
  "buying_behavior",
  "years_in_home",
  "staying_years",
  "forecast_amount",
  "forecast_date",
]

const CO_COLS = [
  "id",
  "project_id",
  "co_type",
  "description",
  "estimate_amount",
  "sale_amount",
  "status",
  "date_estimated",
  "date_decided",
  "created_by",
]

function inYear(value, year) {
  return typeof value === "string" && value.slice(0, 4) === String(year)
}

function hashId(value) {
  let h = 2166136261
  const s = String(value)
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return h >>> 0
}

export function synthesizeSold(count, year) {
  const y = String(year)
  const sold = []
  for (let i = 0; i < count; i++) {
    const id = `stress-${count}-${String(i).padStart(4, "0")}`
    const bucket = hashId(id)
    let sale = 8000 + (bucket % 90) * 1200
    if (i === 0 && count > 1) sale = null
    if (i === 1 && count > 2) sale = 400000
    const month = String((i % 12) + 1).padStart(2, "0")
    const day = String((i % 27) + 1).padStart(2, "0")
    sold.push({
      id,
      job_name: `Density sample ${i + 1}`,
      stage: "Sold",
      project_type: "Kitchen Remodel",
      job_location: "Carmel",
      lead_source: "Referral",
      lead_date: `${y}-01-${day}`,
      sale_amount: sale,
      estimate_amount: sale || 18000,
      estimate_date: `${y}-02-${day}`,
      date_sold: `${y}-${month}-${day}`,
      date_lost: null,
      stage_lost: null,
      lost_reason: null,
      contact_id: "c-stress",
      salesperson: "Geoff Horen",
      buying_behavior: "2",
      years_in_home: "5-10",
      staying_years: "10+",
      forecast_amount: sale || 18000,
      forecast_date: `${y}-${month}-01`,
    })
  }
  return sold
}

export function shapeYear(source, year, stress) {
  const y = Number(year)
  let sold = source.projects.filter((project) => project.stage === "Sold" && inYear(project.date_sold, y))
  const lost = source.projects.filter((project) => project.stage === "Lost" && inYear(project.lead_date, y))
  const changeOrders = source.changeOrders.filter((changeOrder) => inYear(changeOrder.date_estimated, y))
  if (stress === "1" || stress === "120" || stress === "500") {
    sold = synthesizeSold(Number(stress), y)
  }
  const contacts = {}
  for (const contact of source.contacts) contacts[contact.id] = contact
  contacts["c-stress"] = {
    id: "c-stress",
    first_name: "Sample",
    last_name: "Density",
    home_value: 450000,
  }
  const projectsById = {}
  for (const project of source.projects) projectsById[project.id] = project
  for (const project of sold) projectsById[project.id] = project
  const carryoverSold = sold.filter((project) => !inYear(project.lead_date, y)).length
  return {
    year: y,
    source: "fixture",
    sold,
    lost,
    changeOrders,
    contacts,
    projectsById,
    carryoverSold,
  }
}

export function yearsInFixture(source = fixture) {
  const years = new Set()
  for (const project of source.projects) {
    if (project.date_sold) years.add(Number(project.date_sold.slice(0, 4)))
    if (project.lead_date) years.add(Number(project.lead_date.slice(0, 4)))
  }
  return Array.from(years).filter((year) => Number.isFinite(year)).sort((a, b) => b - a)
}

function wantsLive() {
  if (typeof window === "undefined") return false
  return new URLSearchParams(window.location.search).get("live") === "1"
}

// Read-only GET path. Vite replaces import.meta.env.DEV, so this function
// and the anon key are dropped from production and preview bundles.
async function loadLiveYear(year) {
  const SB_URL = "https://dwkvdelvnmvniewakzoq.supabase.co"
  const SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR3a3ZkZWx2bm12bmlld2Frem9xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1NDAxMTcsImV4cCI6MjA4OTExNjExN30.4B9V-_uKHM8UOf4MxZ_g5tg1kcarPqZ0SoZifEu6O4A"
  const headers = { apikey: SB_KEY, Authorization: `Bearer ${SB_KEY}` }
  async function get(table, params) {
    const response = await fetch(`${SB_URL}/rest/v1/${table}?${params}`, { method: "GET", headers })
    if (!response.ok) throw new Error(`GET ${table} failed (${response.status})`)
    return response.json()
  }
  const y = Number(year)
  const [leadProjects, soldProjects, changeOrders] = await Promise.all([
    get("projects", `select=${DASH_COLS.join(",")}&lead_date=gte.${y}-01-01&lead_date=lte.${y}-12-31&limit=50000`),
    get("projects", `select=${DASH_COLS.join(",")}&stage=eq.Sold&date_sold=gte.${y}-01-01&date_sold=lte.${y}-12-31&limit=50000`),
    get("change_orders", `select=${CO_COLS.join(",")}&date_estimated=gte.${y}-01-01&date_estimated=lte.${y}-12-31&limit=10000`),
  ])
  const sold = soldProjects || []
  const lost = (leadProjects || []).filter((project) => project.stage === "Lost")
  const ids = {}
  for (const project of [...(leadProjects || []), ...sold]) {
    if (project.contact_id) ids[project.contact_id] = true
  }
  const idList = Object.keys(ids)
  const contacts = {}
  for (let i = 0; i < idList.length; i += 200) {
    const batch = idList.slice(i, i + 200)
    const rows = await get("contacts", `select=id,first_name,last_name&id=in.(${batch.join(",")})`)
    for (const contact of rows || []) contacts[contact.id] = contact
  }
  const projectsById = {}
  for (const project of [...(leadProjects || []), ...sold]) projectsById[project.id] = project
  const carryoverSold = sold.filter((project) => !inYear(project.lead_date, y)).length
  return {
    year: y,
    source: "live",
    sold,
    lost,
    changeOrders: changeOrders || [],
    contacts,
    projectsById,
    carryoverSold,
  }
}

export function loadYear(year, stress) {
  // DEV is false in `vite build` / `vite preview`, so ?live=1 cannot run there.
  if (import.meta.env.DEV && wantsLive()) {
    return loadLiveYear(year)
  }
  return Promise.resolve(shapeYear(fixture, year, stress))
}

export function initialYear() {
  const years = yearsInFixture()
  if (typeof window !== "undefined") {
    const requested = Number(new URLSearchParams(window.location.search).get("year"))
    if (years.includes(requested)) return requested
  }
  if (years.includes(2026)) return 2026
  return years[0] || 2026
}

export function initialStress() {
  if (typeof window === "undefined") return "auto"
  const stress = new URLSearchParams(window.location.search).get("stress")
  if (stress === "1" || stress === "120" || stress === "500") return stress
  return "auto"
}
