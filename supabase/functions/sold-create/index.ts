import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import {
  FLAG_KEY,
  SOLD_STAGE,
  buildOpsProjectRow,
  computePhasePlan,
  extractProjectId,
  isFlagEnabled,
  resolveBathTemplateKey,
} from "./mapper.js";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

function serviceClient() {
  const url = Deno.env.get("SUPABASE_URL") ?? "https://dwkvdelvnmvniewakzoq.supabase.co";
  const key = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!key) throw new Error("SUPABASE_SERVICE_ROLE_KEY is not set");
  return createClient(url, key, { auth: { persistSession: false, autoRefreshToken: false } });
}

async function readFlag(sb: ReturnType<typeof createClient>) {
  const { data, error } = await sb
    .from("ops_app_settings")
    .select("value")
    .eq("key", FLAG_KEY)
    .maybeSingle();
  if (error) throw error;
  return isFlagEnabled(data?.value);
}

async function loadProject(sb: ReturnType<typeof createClient>, id: string) {
  const { data, error } = await sb.from("projects").select("*").eq("id", id).maybeSingle();
  if (error) throw error;
  return data;
}

async function loadContact(sb: ReturnType<typeof createClient>, contactId: string | null) {
  if (!contactId) return null;
  const { data, error } = await sb.from("contacts").select("*").eq("id", contactId).maybeSingle();
  if (error) throw error;
  return data;
}

async function existingOps(sb: ReturnType<typeof createClient>, crmProjectId: string) {
  const { data, error } = await sb
    .from("ops_project_schedule")
    .select("id, crm_project_id, name, status")
    .eq("crm_project_id", crmProjectId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

async function nextForecastQueuePosition(sb: ReturnType<typeof createClient>) {
  const { data, error } = await sb
    .from("ops_project_schedule")
    .select("queue_position")
    .eq("status", "forecast")
    .order("queue_position", { ascending: false, nullsFirst: false })
    .limit(1);
  if (error) throw error;
  const max = data?.[0]?.queue_position;
  return (typeof max === "number" ? max : 0) + 1;
}

async function insertPhases(
  sb: ReturnType<typeof createClient>,
  projectScheduleId: string,
  plan: ReturnType<typeof computePhasePlan>,
) {
  const created: { id: string }[] = [];
  for (const phase of plan) {
    const { data, error } = await sb
      .from("ops_phases")
      .insert({
        project_schedule_id: projectScheduleId,
        name: phase.name,
        planned_days: phase.planned_days,
        original_planned_days: phase.original_planned_days,
        start_day_offset: phase.start_day_offset,
        sort_order: phase.sort_order,
        status: phase.status,
        days_worked: phase.days_worked,
        lag_days: phase.lag_days,
      })
      .select("id")
      .single();
    if (error) throw error;
    created.push(data);
  }

  for (let i = 0; i < plan.length; i++) {
    const depIdx = plan[i].depIdx;
    if (depIdx == null || !created[depIdx]) continue;
    const { error } = await sb
      .from("ops_phases")
      .update({ depends_on: created[depIdx].id })
      .eq("id", created[i].id);
    if (error) throw error;
  }

  return created.length;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204 });
  if (req.method !== "POST") return json({ error: "POST only" }, 405);

  let payload: Record<string, unknown> = {};
  try {
    payload = (await req.json()) as Record<string, unknown>;
  } catch {
    return json({ error: "invalid JSON" }, 400);
  }

  const projectId = extractProjectId(payload);
  const dryRun = payload.dry_run === true || payload.dryRun === true;
  if (!projectId) return json({ error: "project_id required" }, 400);

  try {
    const sb = serviceClient();
    const project = await loadProject(sb, String(projectId));
    if (!project) return json({ skipped: true, reason: "project_not_found", project_id: projectId });
    if (project.stage !== SOLD_STAGE) {
      return json({ skipped: true, reason: "stage_not_sold", project_id: projectId, stage: project.stage });
    }

    const contact = await loadContact(sb, project.contact_id);
    const templateKey = resolveBathTemplateKey(project);
    const queuePosition = await nextForecastQueuePosition(sb);
    const opsProject = buildOpsProjectRow(project, contact, queuePosition);
    const phases = templateKey ? computePhasePlan(templateKey) : [];

    if (dryRun) {
      return json({
        dry_run: true,
        would_insert: true,
        project_id: project.id,
        template_key: templateKey,
        ops_project: opsProject,
        phase_count: phases.length,
        phases,
      });
    }

    const enabled = await readFlag(sb);
    if (!enabled) {
      return json({
        skipped: true,
        reason: "flag_off",
        project_id: project.id,
        hint: `Set ops_app_settings.${FLAG_KEY} = true to write.`,
      });
    }

    const already = await existingOps(sb, project.id);
    if (already) {
      return json({
        skipped: true,
        reason: "already_linked",
        project_id: project.id,
        ops_project_schedule_id: already.id,
      });
    }

    const { data: created, error: insertError } = await sb
      .from("ops_project_schedule")
      .insert(opsProject)
      .select("id, crm_project_id, name, status")
      .single();

    if (insertError) {
      if (insertError.code === "23505") {
        const again = await existingOps(sb, project.id);
        return json({
          skipped: true,
          reason: "already_linked",
          project_id: project.id,
          ops_project_schedule_id: again?.id ?? null,
        });
      }
      throw insertError;
    }

    let phaseCount = 0;
    if (phases.length && created?.id) {
      phaseCount = await insertPhases(sb, created.id, phases);
    }

    return json({
      created: true,
      project_id: project.id,
      ops_project_schedule_id: created.id,
      template_key: templateKey,
      phase_count: phaseCount,
      status: opsProject.status,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error("sold-create failed", message);
    return json({ error: message }, 500);
  }
});
