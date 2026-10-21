// Lockout rules approved with the PIN plan (D1 tap-name, plus per-IP and global).
//
// Per person: 5 failures in 15 minutes locks that person for 15 minutes.
//   A correct PIN is refused until the lock ends. The lock does not double.
// Per IP: 5 failures in 15 minutes locks that IP for 15 minutes. Each further
//   completed burst doubles the lock, capped at 24 hours.
// Global: more than 30 failures across all IPs in 60 minutes pauses PIN login
//   for everyone for 15 minutes. The pause does not stack inside itself.
//
// Failures that occur while a lock is already active do not extend it.
// `at` is epoch milliseconds. `ok: false` rows are the only ones that count.

export const USER_FAIL_LIMIT = 5;
export const USER_WINDOW_MS = 15 * 60 * 1000;
export const USER_LOCK_MS = 15 * 60 * 1000;

export const IP_FAIL_LIMIT = 5;
export const IP_WINDOW_MS = 15 * 60 * 1000;
export const IP_BASE_LOCK_MS = 15 * 60 * 1000;
export const IP_MAX_LOCK_MS = 24 * 60 * 60 * 1000;

export const GLOBAL_FAIL_LIMIT = 30;
export const GLOBAL_WINDOW_MS = 60 * 60 * 1000;
export const GLOBAL_PAUSE_MS = 15 * 60 * 1000;

const SCOPE_RANK = { global: 3, ip: 2, user: 1 };

function burstLockUntil(timesAsc, { limit, windowMs, baseLockMs, maxLockMs, doubling }) {
  let streak = [];
  let level = 0;
  let lockUntil = 0;
  for (const t of timesAsc) {
    if (t < lockUntil) continue;
    streak = streak.filter((x) => t - x <= windowMs);
    streak.push(t);
    if (streak.length >= limit) {
      level += 1;
      const factor = doubling ? 2 ** (level - 1) : 1;
      const duration = Math.min(baseLockMs * factor, maxLockMs);
      lockUntil = t + duration;
      streak = [];
    }
  }
  return lockUntil;
}

function globalPauseUntil(timesAsc) {
  let pauseUntil = 0;
  for (const t of timesAsc) {
    if (t < pauseUntil) continue;
    let count = 0;
    for (const x of timesAsc) {
      if (x <= t && t - x <= GLOBAL_WINDOW_MS) count += 1;
    }
    if (count > GLOBAL_FAIL_LIMIT) pauseUntil = t + GLOBAL_PAUSE_MS;
  }
  return pauseUntil;
}

function failTimes(attempts, pred) {
  return (attempts || [])
    .filter((row) => row && row.ok === false && typeof row.at === "number" && pred(row))
    .map((row) => row.at)
    .sort((a, b) => a - b);
}

export function lockState({ attempts, now, ipHash, userId }) {
  const all = failTimes(attempts, () => true);
  const ip = failTimes(attempts, (row) => row.ip_hash === ipHash);
  const user = failTimes(
    attempts,
    (row) => userId != null && Number(row.crm_user_id) === Number(userId),
  );

  const globalUntil = globalPauseUntil(all);
  const ipUntil = burstLockUntil(ip, {
    limit: IP_FAIL_LIMIT,
    windowMs: IP_WINDOW_MS,
    baseLockMs: IP_BASE_LOCK_MS,
    maxLockMs: IP_MAX_LOCK_MS,
    doubling: true,
  });
  const userUntil = burstLockUntil(user, {
    limit: USER_FAIL_LIMIT,
    windowMs: USER_WINDOW_MS,
    baseLockMs: USER_LOCK_MS,
    maxLockMs: USER_LOCK_MS,
    doubling: false,
  });

  const active = [];
  if (globalUntil > now) active.push({ scope: "global", until: globalUntil });
  if (ipUntil > now) active.push({ scope: "ip", until: ipUntil });
  if (userUntil > now) active.push({ scope: "user", until: userUntil });
  if (active.length === 0) return { locked: false };

  active.sort((a, b) => (b.until - a.until) || (SCOPE_RANK[b.scope] - SCOPE_RANK[a.scope]));
  const top = active[0];
  return {
    locked: true,
    scope: top.scope,
    retry_after: Math.max(1, Math.ceil((top.until - now) / 1000)),
    until: top.until,
  };
}
