// Browser side of pin-login. The PIN goes in a POST body to the edge function.
// It is never put on a crm_users query string.

export function postStaffAuth(sbUrl, anonKey, body, token) {
  var headers = {
    apikey: anonKey,
    "Content-Type": "application/json",
    Authorization: "Bearer " + (token || anonKey),
  };
  if (token) headers["x-staff-token"] = token;
  return fetch(sbUrl + "/functions/v1/pin-login", {
    method: "POST",
    headers: headers,
    body: JSON.stringify(body),
  }).then(function(response) {
    return response.json().catch(function() { return {}; }).then(function(data) {
      return { status: response.status, data: data || {} };
    });
  });
}

export function readStoredSession(raw, now) {
  if (!raw || typeof raw !== "object") return null;
  if (typeof raw.token !== "string" || raw.token.length === 0) return null;
  if (!(Number(raw.expires) > now)) return null;
  return raw;
}

export function sessionFromLogin(data, now) {
  if (!data || typeof data.token !== "string" || !data.user) return null;
  var expires = Date.parse(data.expires_at);
  if (!expires) expires = now + 8 * 60 * 60 * 1000;
  return {
    id: data.user.id,
    name: data.user.name,
    role: data.user.role,
    expires: expires,
    token: data.token,
  };
}

export function sessionFromWhoami(stored, whoami, now) {
  if (!readStoredSession(stored, now)) return null;
  if (!whoami || whoami.active !== true || whoami.role == null || whoami.id == null) return null;
  return {
    id: whoami.id,
    name: whoami.name,
    role: whoami.role,
    expires: stored.expires,
    token: stored.token,
  };
}

export function lockSeconds(data) {
  var secs = Number(data && data.retry_after);
  if (!isFinite(secs) || secs < 1) return 15 * 60;
  return Math.ceil(secs);
}
