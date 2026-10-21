// Scheduler rules Geoff approved with the PIN plan.
// Editing the board: Owner and Admin.
// Approving or rejecting job history: Owner, and only the ids in
// APPROVER_CRM_USER_IDS (Geoff Horen's crm_users.id). Do not guess the id.
// Viewing is any active staff; the whoami re-read is what makes them active.

export const SCHEDULER_EDIT_ROLES = ["Owner", "Admin"];

export function canEditScheduler(role) {
  return SCHEDULER_EDIT_ROLES.includes(role);
}

export function canApproveHistory(role, userId, approverIds) {
  const ids = (Array.isArray(approverIds) ? approverIds : String(approverIds || "").split(","))
    .map((value) => String(value).trim())
    .filter(Boolean);
  return role === "Owner" && ids.includes(String(userId));
}
