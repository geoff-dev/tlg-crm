const EXACT_ORIGINS = new Set([
  "https://tlg-crm-nine.vercel.app",
  "https://tlg-scheduler.vercel.app",
  "http://localhost:5173",
  "http://localhost:4173",
  "http://127.0.0.1:5173",
  "http://127.0.0.1:4173",
]);

const PREVIEW_ORIGIN = /^https:\/\/(?:(?:tlg-crm-nine|tlg-scheduler)-[a-z0-9-]+\.vercel\.app|tlg-[a-z0-9-]+-geoff-devs-projects\.vercel\.app)$/;

export function isAllowedOrigin(origin) {
  if (!origin) return false;
  if (EXACT_ORIGINS.has(origin)) return true;
  return PREVIEW_ORIGIN.test(origin);
}

export function corsHeaders(origin) {
  const headers = {
    "Access-Control-Allow-Headers": "authorization, apikey, content-type, x-staff-token, x-client-info",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin",
    "Cache-Control": "no-store",
  };
  if (origin && isAllowedOrigin(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
  }
  return headers;
}
