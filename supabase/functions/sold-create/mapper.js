import { PROJECT_TEMPLATES, PROJ_PALETTE } from "./templates.js";

export const FLAG_KEY = "SOLD_CREATE_ENABLED";
export const SOLD_STAGE = "Sold";
export const OPS_STATUS = "forecast";

const TRUE_VALUES = new Set(["true", "1", "yes", "on"]);

export function isFlagEnabled(value) {
  if (value === true || value === 1) return true;
  if (value == null) return false;
  return TRUE_VALUES.has(String(value).trim().toLowerCase());
}

function textHay(project) {
  return [project?.job_name, project?.project_description]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

export function isBathroomRemodelType(project) {
  const type = String(project?.project_type || "").toLowerCase().trim();
  return type === "bathroom remodel";
}

export function isKitchenOrOther(project) {
  return !isBathroomRemodelType(project);
}

function explicitFloorKey(project) {
  const hay = textHay(project);
  const lvp = /\blvp\b/.test(hay) || /luxury vinyl/.test(hay) || /vinyl plank/.test(hay);
  const tileFloor =
    /tile\s+floors?\b/.test(hay) ||
    /tiled\s+floors?\b/.test(hay) ||
    /tile\s+flooring\b/.test(hay);
  if (lvp && tileFloor) return null;
  if (lvp) return "bath-lvp";
  if (tileFloor) return "bath-tile";
  return null;
}

export function resolveBathTemplateKey(project) {
  if (!isBathroomRemodelType(project)) return null;
  return explicitFloorKey(project);
}

export function formatClientName(contact) {
  if (!contact) return null;
  const full = String(contact.full_name || "").trim();
  if (full) return full;
  const joined = `${contact.first_name || ""} ${contact.last_name || ""}`.trim();
  return joined || null;
}

export function formatAddress(contact) {
  if (!contact) return null;
  const line = contact.address || null;
  const line2 = contact.address_line2 || null;
  const cityState = [contact.city, contact.state].filter(Boolean).join(", ");
  const parts = [line, line2, cityState, contact.zip].filter((p) => p && String(p).trim());
  return parts.length ? parts.join(" ").replace(/\s+/g, " ").trim() : null;
}

export function colorFromPalette(existingProjectCount) {
  const n = Number(existingProjectCount) || 0;
  return PROJ_PALETTE[n % PROJ_PALETTE.length];
}

export function nextQueuePosition(sameStatusRows) {
  const maxQ = (sameStatusRows || []).reduce((m, p) => Math.max(m, p.queue_position || 0), 0);
  return maxQ + 1;
}

export function forecastBaseOffset(existingPhases) {
  let mx = 0;
  for (const ph of existingPhases || []) {
    mx = Math.max(mx, (Number(ph.start_day_offset) || 0) + (Number(ph.planned_days) || 0));
  }
  return mx + 1;
}

export function buildOpsProjectRow(project, contact, { queuePosition, color } = {}) {
  return {
    name: project.job_name || "Untitled CRM project",
    project_type: project.project_type || null,
    client_name: formatClientName(contact),
    client_email: contact?.email || null,
    address: formatAddress(contact),
    status: OPS_STATUS,
    color: color || colorFromPalette(0),
    queue_position: queuePosition ?? 1,
    is_published: false,
    crm_project_id: project.id,
  };
}

export function computePhasePlan(templateKey, baseOffset = 0) {
  const tpl = PROJECT_TEMPLATES[templateKey];
  if (!tpl) return [];
  return tpl.phases.map((phase, i) => ({
    name: phase.name,
    planned_days: phase.days,
    original_planned_days: phase.days,
    start_day_offset: baseOffset + i,
    sort_order: i + 1,
    status: "upcoming",
    days_worked: 0,
    lag_days: phase.lag || 0,
    depIdx: phase.depIdx,
  }));
}

export function extractProjectId(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (payload.project_id) return payload.project_id;
  if (payload.record?.id) return payload.record.id;
  if (payload.record?.project_id) return payload.record.project_id;
  return null;
}

export function isSoldRecord(record) {
  return record?.stage === SOLD_STAGE;
}

export function mapSoldCreate(project, contact, { queuePosition, color, baseOffset } = {}) {
  const templateKey = resolveBathTemplateKey(project);
  return {
    templateKey,
    opsProject: buildOpsProjectRow(project, contact, { queuePosition, color }),
    phases: templateKey ? computePhasePlan(templateKey, baseOffset ?? 0) : [],
  };
}
