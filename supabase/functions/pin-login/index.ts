import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { handlePinLogin } from "./handler.js";
import { clientIpFromHeaders } from "./ip.js";

function serviceClient() {
  const url = Deno.env.get("SUPABASE_URL") ?? "https://dwkvdelvnmvniewakzoq.supabase.co";
  const key = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!key) throw new Error("SUPABASE_SERVICE_ROLE_KEY is not set");
  return createClient(url, key, { auth: { persistSession: false, autoRefreshToken: false } });
}

function depsFor(sb: ReturnType<typeof createClient>) {
  return {
    now: () => Date.now(),
    secret: Deno.env.get("STAFF_SESSION_SECRET") ?? "",
    ipSalt: Deno.env.get("STAFF_IP_SALT") ?? "",
    clientIp(headers: Headers) {
      return clientIpFromHeaders(headers);
    },
    async recentAttempts(sinceIso: string) {
      const { data, error } = await sb
        .from("staff_login_attempts")
        .select("at, ip_hash, ok, crm_user_id, app")
        .gte("at", sinceIso)
        .order("at", { ascending: false })
        .limit(5000);
      if (error) throw error;
      return data || [];
    },
    async recordAttempt(row: { at: string; ip_hash: string; ok: boolean; crm_user_id: number | null; app: string }) {
      const { error } = await sb.from("staff_login_attempts").insert({
        at: row.at,
        ip_hash: row.ip_hash,
        ok: row.ok,
        crm_user_id: row.crm_user_id,
        app: row.app,
      });
      if (error) throw error;
    },
    async recordAlert(alert: { kind: string; detail: Record<string, unknown> }) {
      const { error } = await sb.from("staff_login_alerts").insert({
        kind: alert.kind,
        detail: alert.detail,
      });
      if (error) throw error;
    },
    async checkPin(userId: number, pin: string) {
      const { data, error } = await sb.rpc("staff_check_pin", { p_user_id: userId, p_pin: pin });
      if (error) throw error;
      return data;
    },
    async loadUser(id: number) {
      const { data, error } = await sb.rpc("staff_session", { p_id: id });
      if (error) throw error;
      if (!data) return null;
      return {
        id: data.id,
        name: data.name,
        role: data.role,
        active: data.active,
        token_version: data.tv,
      };
    },
    async listNames() {
      const { data, error } = await sb.rpc("staff_list_names");
      if (error) throw error;
      return data || [];
    },
  };
}

Deno.serve(async (req) => {
  try {
    return await handlePinLogin(req, depsFor(serviceClient()));
  } catch {
    return new Response(JSON.stringify({ error: "staff auth is not configured" }), {
      status: 500,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    });
  }
});
