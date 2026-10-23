// Slice 0 UI policy for the read-only Viewer role.
// Unknown roles take the same limited set (default deny): never more than Viewer.

export const STAFF_ROLES = ["Owner", "Admin", "Sales", "Production"];

export const VIEWER_HOME_TILES = ["pipeline", "dashboard", "lifedeath"];

export const VIEWER_CONSOLE_TILES = ["thermometers", "scorecard", "history", "pipeline"];

export const VIEW_AS_ROLES = ["Admin", "Sales", "Production", "Viewer"];

export function isLimitedRole(role) {
  return STAFF_ROLES.indexOf(role) < 0;
}

export function homeTileVisible(role, tile) {
  if (!tile || !tile.id) return false;
  if (isLimitedRole(role)) return VIEWER_HOME_TILES.indexOf(tile.id) >= 0;
  return !tile.roles || tile.roles.indexOf(role) >= 0;
}

export function visibleHomeTiles(role, tiles) {
  const visible = (tiles || []).filter((item) => homeTileVisible(role, item));
  if (role === "Production") {
    visible.sort((a, b) => {
      if (a.id === "_production") return -1;
      if (b.id === "_production") return 1;
      return 0;
    });
  }
  return visible;
}

export function consoleTilesFor(role, tiles) {
  const list = tiles || [];
  if (!isLimitedRole(role)) return list.slice();
  return list.filter((item) => VIEWER_CONSOLE_TILES.indexOf(item.id) >= 0);
}

export function consoleSectionVisible(allowed, section) {
  if (section === "mchome") return true;
  if (!allowed) return true;
  return allowed.indexOf(section) >= 0;
}

export function showsNewButtons(role) {
  return !isLimitedRole(role) && role !== "Production";
}

export function showsManagementConsole(role) {
  return role === "Owner" || role === "Admin" || isLimitedRole(role);
}

export function showsHiddenConsole(role) {
  return role === "Owner" || role === "Admin";
}

export function canOpenRecords(role) {
  return !isLimitedRole(role);
}

export function canShowProjectView(role, viewId) {
  if (!isLimitedRole(role)) return true;
  return viewId === "home" || viewId === "mgmtconsole" || VIEWER_HOME_TILES.indexOf(viewId) >= 0;
}

export function canUseViewAs(role) {
  return role === "Owner";
}

// Catch-all for project and contact opens. Limited roles never reach the detail screens.
export function openRecord(role, record, setDetail, kind) {
  if (!canOpenRecords(role) || record == null) return false;
  setDetail({ type: kind, data: record });
  return true;
}
