// whoami decision. `verifyResult` comes from verifyStaffToken.
// `row` is the current crm_users row. Role, active, and token_version
// always come from the row, never from the token.

export function authorizeSession(verifyResult, row) {
  if (!verifyResult || verifyResult.ok !== true) {
    return { ok: false, status: 401, error: verifyResult?.error || "invalid session" };
  }
  const claims = verifyResult.claims;
  if (!row || row.active !== true) {
    return { ok: false, status: 401, error: "invalid session" };
  }
  if (Number(row.id) !== Number(claims.crm_user_id)) {
    return { ok: false, status: 401, error: "invalid session" };
  }
  const rowVersion = row.token_version != null ? row.token_version : row.tv;
  if (Number(rowVersion) !== Number(claims.tv)) {
    return { ok: false, status: 401, error: "invalid session" };
  }
  return {
    ok: true,
    status: 200,
    user: {
      id: Number(row.id),
      name: row.name,
      role: row.role,
      active: true,
    },
  };
}
