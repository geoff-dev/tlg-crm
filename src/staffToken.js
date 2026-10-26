// Staff badge for Supabase REST calls.
// The token stays in the browser. It is attached only when this request's
// origin is the configured Supabase origin, and only while it is unexpired.
// Reads do not call checkStaffWrite. A write does, before the request.

export var SESSION_EXPIRED_MESSAGE = "Session expired, please sign in again";
export var EMPTY_WRITE_MESSAGE = "Not saved. Please sign in again.";

var failureListeners = [];

export function subscribeStaffWriteFailure(listener) {
  failureListeners.push(listener);
  return function unsubscribe() {
    var index = failureListeners.indexOf(listener);
    if (index >= 0) failureListeners.splice(index, 1);
  };
}

export function reportStaffWriteFailure(message) {
  for (var i = 0; i < failureListeners.length; i++) failureListeners[i](message);
}

export function readStoredTlgSession(storage) {
  var store = storage;
  if (!store) {
    try {
      if (typeof localStorage !== "undefined") store = localStorage;
    } catch (e) {
      store = null;
    }
  }
  if (!store || typeof store.getItem !== "function") return null;
  try {
    var raw = store.getItem("tlg_session");
    if (!raw) return null;
    var parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (e) {
    return null;
  }
}

function decodeJwtPayload(token) {
  if (typeof token !== "string") return null;
  var parts = token.split(".");
  if (parts.length < 2 || !parts[1]) return null;
  try {
    var padded = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    var remainder = padded.length % 4;
    if (remainder) padded += "====".slice(remainder);
    var binary = atob(padded);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch (e) {
    return null;
  }
}

export function staffSessionState(raw, now) {
  if (!raw || typeof raw !== "object") return { status: "absent" };
  if (typeof raw.token !== "string" || raw.token.length === 0) return { status: "absent" };
  var payload = decodeJwtPayload(raw.token);
  var jwtExpMs = null;
  if (payload && payload.exp != null && payload.exp !== "") {
    var expNum = Number(payload.exp);
    if (isFinite(expNum)) jwtExpMs = expNum * 1000;
  }
  var storedExpMs = null;
  if (raw.expires != null && raw.expires !== "") {
    var storedNum = Number(raw.expires);
    if (isFinite(storedNum)) storedExpMs = storedNum;
  }
  if (jwtExpMs != null && jwtExpMs <= now) return { status: "expired" };
  if (storedExpMs != null && !(storedExpMs > now)) return { status: "expired" };
  return { status: "active", token: raw.token };
}

function sessionFromOptions(options) {
  var opts = options || {};
  if (Object.prototype.hasOwnProperty.call(opts, "session")) return opts.session;
  return readStoredTlgSession(opts.storage);
}

export function isSupabaseRequestUrl(targetUrl, supabaseUrl) {
  if (typeof targetUrl !== "string" || typeof supabaseUrl !== "string") return false;
  if (!targetUrl || !supabaseUrl) return false;
  try {
    var target = new URL(targetUrl);
    var supabase = new URL(supabaseUrl);
    return target.origin === supabase.origin && target.origin !== "null";
  } catch (e) {
    return false;
  }
}

export function staffRequestHeaders(baseHeaders, targetUrl, supabaseUrl, options) {
  var opts = options || {};
  var now = opts.now != null ? opts.now : Date.now();
  var headers = Object.assign({}, baseHeaders || {});
  delete headers["x-staff-token"];
  delete headers["X-Staff-Token"];
  if (!isSupabaseRequestUrl(targetUrl, supabaseUrl)) return headers;
  var state = staffSessionState(sessionFromOptions(opts), now);
  if (state.status === "active") headers["x-staff-token"] = state.token;
  return headers;
}

export function checkStaffWrite(options) {
  var opts = options || {};
  var now = opts.now != null ? opts.now : Date.now();
  var state = staffSessionState(sessionFromOptions(opts), now);
  if (state.status === "expired") return { ok: false, message: SESSION_EXPIRED_MESSAGE };
  return { ok: true };
}

// PostgREST returns [] (HTTP 200) when RLS hides every row of a PATCH or DELETE.
export function interpretWriteResult(result) {
  if (result == null || (Array.isArray(result) && result.length === 0)) {
    return { ok: false, message: EMPTY_WRITE_MESSAGE };
  }
  var row = Array.isArray(result) ? (result[0] || result) : result;
  return { ok: true, row: row };
}
