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

function haystack(project) {
  return [project?.project_type, project?.job_name, project?.project_description]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

export function isKitchenOrWholeHouse(project) {
  const hay = haystack(project);
  return /\bkitchen\b/.test(hay) || /whole[-\s]?house/.test(hay);
}

export function isPrimaryBathroom(project) {
  if (!project || isKitchenOrWholeHouse(project)) return false;
  const type = String(project.project_type || "").toLowerCase().trim();
  const name = String(project.job_name || "").toLowerCase();
  if (type === "primary bathroom" || type === "primary bath") return true;
  const combined = `${type} ${name}`;
  if (/\bpowder\b/.test(combined)) return false;
  return /primary\s+(bath|bathroom)\b/.test(combined);
}

export function resolveBathTemplateKey(project) {
  if (!isPrimaryBathroom(project)) return null;
  const hay = haystack(project);
  const lvp = /\blvp\b/.test(hay) || /luxury vinyl/.test(hay) || /vinyl plank/.test(hay);
  const tileFloor = /tile\s+floor/.test(hay) || /\bbath[-\s]?tile\b/.test(hay);
  const tile = tileFloor || (/\btile\b/.test(hay) && !lvp);
  if (lvp && !/\btile\b/.test(hay)) return "bath-lvp";
  if (tileFloor && !lvp) return "bath-tile";
  if (lvp && /\btile\b/.test(hay)) return null;
  if (tile && !lvp) return "bath-tile";
  return null;
}

export function formatClientName(contact) {
  if (!contact) return null;
  const full = String(contact.full_name || "").trim();
  if (full) return full;
  const joined = `${contact.first_name || ""} ${contact.last_name || ""}`.trim();
  return joined || null;
}

export function formatAddress(project, contact) {
  const line = project?.job_address || contact?.address || null;
  const line2 = project?.job_address2 || contact?.address_line2 || null;
  const city = project?.job_city || contact?.city || null;
  const state = project?.job_state || contact?.state || null;
  const zip = project?.job_zip || contact?.zip || null;
  const cityState = [city, state].filter(Boolean).join(", ");
  const parts = [line, line2, cityState, zip].filter((p) => p && String(p).trim());
  return parts.length ? parts.join(" ").replace(/\s+/g, " ").trim() : null;
}

export function metadataNotes(project) {
  const bits = [`crm_project_id=${project.id}`];
  if (project.date_sold) bits.push(`date_sold=${project.date_sold}`);
  return `CRM sold-create (metadata only; not a schedule start). ${bits.join("; ")}`;
}

function colorForProject(projectId) {
  const s = String(projectId || "");
  let n = 0;
  for (let i = 0; i < s.length; i++) n = (n + s.charCodeAt(i)) % PROJ_PALETTE.length;
  return PROJ_PALETTE[n];
}

export function buildOpsProjectRow(project, contact, queuePosition) {
  return {
    name: project.job_name || "Untitled CRM project",
    project_type: project.project_type || null,
    client_name: formatClientName(contact),
    client_email: contact?.email || null,
    address: formatAddress(project, contact),
    status: OPS_STATUS,
    is_published: false,
    color: colorForProject(project.id),
    queue_position: queuePosition ?? 1,
    crm_project_id: project.id,
    forecast_notes: metadataNotes(project),
  };
}

export function computePhasePlan(templateKey) {
  const tpl = PROJECT_TEMPLATES[templateKey];
  if (!tpl) return [];
  const phases = tpl.phases;
  const offsets = phases.map(() => 0);
  const visiting = new Set();

  function offsetAt(i) {
    if (visiting.has(i)) return offsets[i];
    visiting.add(i);
    const phase = phases[i];
    if (phase.depIdx == null) {
      offsets[i] = 0;
      return 0;
    }
    const dep = offsetAt(phase.depIdx);
    const depDays = phases[phase.depIdx].days || 0;
    offsets[i] = dep + depDays + (phase.lag || 0);
    return offsets[i];
  }

  phases.forEach((_, i) => offsetAt(i));

  return phases.map((phase, i) => ({
    name: phase.name,
    planned_days: phase.days,
    original_planned_days: phase.days,
    start_day_offset: offsets[i],
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

export function mapSoldCreate(project, contact) {
  const templateKey = resolveBathTemplateKey(project);
  return {
    templateKey,
    opsProject: buildOpsProjectRow(project, contact, 1),
    phases: templateKey ? computePhasePlan(templateKey) : [],
  };
}
