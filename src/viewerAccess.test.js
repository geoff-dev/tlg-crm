import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import {
  STAFF_ROLES,
  VIEWER_CONSOLE_TILES,
  VIEWER_HOME_TILES,
  VIEW_AS_ROLES,
  canOpenRecords,
  canShowProjectView,
  canUseViewAs,
  consoleSectionVisible,
  consoleTilesFor,
  homeTileVisible,
  isLimitedRole,
  openRecord,
  showsHiddenConsole,
  showsManagementConsole,
  showsNewButtons,
  visibleHomeTiles,
} from "./viewerAccess.js";

const appSource = readFileSync(new URL("./App.jsx", import.meta.url), "utf8");

function sliceBetween(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.equal(start >= 0 && end > start, true, `missing ${startMarker}`);
  return source.slice(start, end);
}

function parseTiles(block) {
  const tiles = [];
  for (const line of block.split("\n")) {
    const id = line.match(/\{id:"([^"]+)"/);
    if (!id) continue;
    const label = line.match(/label:"([^"]+)"/);
    const rolesMatch = line.match(/roles:\[([^\]]*)\]/);
    const roles = rolesMatch
      ? [...rolesMatch[1].matchAll(/"([^"]+)"/g)].map((part) => part[1])
      : null;
    tiles.push({ id: id[1], label: label ? label[1] : "", roles });
  }
  return tiles;
}

const homeTiles = parseTiles(sliceBetween(appSource, '{id:"list",label:"Projects"', "]).map(function(item)"));
const consoleTiles = parseTiles(sliceBetween(appSource, '{id:"thermometers",label:"Goal Tracker"', "].filter(function(item)"));

const STAFF_HOME = {
  Owner: ["list", "pipeline", "openitems", "dashboard", "stale", "ordertrack", "sops", "contacts", "lifedeath", "_production", "_builderstds", "tradepartners"],
  Admin: ["list", "pipeline", "openitems", "dashboard", "stale", "ordertrack", "sops", "contacts", "lifedeath", "_production", "_builderstds", "tradepartners"],
  Sales: ["list", "pipeline", "openitems", "dashboard", "stale", "ordertrack", "sops", "contacts", "lifedeath", "_production", "_builderstds", "tradepartners"],
  Production: ["_production", "pipeline", "ordertrack", "_builderstds", "addresslookup", "tradelookup"],
};

describe("Viewer home tiles", () => {
  it("parses the home tile catalog from App.jsx", () => {
    assert.deepEqual(homeTiles.map((tile) => tile.id), [
      "list", "pipeline", "openitems", "dashboard", "stale", "ordertrack", "sops",
      "contacts", "lifedeath", "_production", "_builderstds", "addresslookup",
      "tradepartners", "tradelookup",
    ]);
  });

  it("shows a Viewer only Kanban, Sales Funnel and Life & Death", () => {
    const visible = visibleHomeTiles("Viewer", homeTiles);
    assert.deepEqual(visible.map((tile) => tile.id), VIEWER_HOME_TILES);
    assert.deepEqual(visible.map((tile) => tile.label), ["Kanban", "Sales Funnel", "Life & Death"]);
    assert.equal(visible.some((tile) => tile.id === "list" || tile.label === "Projects"), false);
  });

  it("gives an unknown role the Viewer home set and nothing more", () => {
    for (const role of ["Intern", "viewer", "owner", "", null, undefined]) {
      const ids = visibleHomeTiles(role, homeTiles).map((tile) => tile.id);
      assert.deepEqual(ids, VIEWER_HOME_TILES);
      for (const id of ids) assert.equal(VIEWER_HOME_TILES.indexOf(id) >= 0, true);
    }
  });
});

describe("existing role home tiles stay unchanged", () => {
  for (const role of STAFF_ROLES) {
    it(`${role} keeps the current home tiles`, () => {
      assert.equal(isLimitedRole(role), false);
      assert.deepEqual(visibleHomeTiles(role, homeTiles).map((tile) => tile.id), STAFF_HOME[role]);
    });
  }
});

describe("Viewer management console", () => {
  it("parses the console catalog from App.jsx", () => {
    assert.deepEqual(consoleTiles.map((tile) => tile.id), [
      "thermometers", "scorecard", "history", "variance", "lost", "reports", "goals",
      "pipeline", "performance", "changeorders", "stale", "_estimator",
    ]);
  });

  it("shows a Viewer Goal Tracker, Scorecard, Sales History and Forecast only", () => {
    const visible = consoleTilesFor("Viewer", consoleTiles);
    assert.deepEqual(visible.map((tile) => tile.id), VIEWER_CONSOLE_TILES);
    assert.deepEqual(visible.map((tile) => tile.label), ["Goal Tracker", "Scorecard", "Sales History", "Forecast"]);
    assert.equal(visible.some((tile) => tile.id === "goals" || tile.label === "Goals"), false);
    assert.equal(showsManagementConsole("Viewer"), true);
    assert.equal(showsHiddenConsole("Viewer"), false);
  });

  it("gives an unknown role the Viewer console set and nothing more", () => {
    const viewerIds = consoleTilesFor("Viewer", consoleTiles).map((tile) => tile.id);
    for (const role of ["Intern", "viewer", null]) {
      const ids = consoleTilesFor(role, consoleTiles).map((tile) => tile.id);
      assert.deepEqual(ids, viewerIds);
      assert.equal(ids.indexOf("goals") < 0, true);
      assert.equal(showsManagementConsole(role), true);
      assert.equal(showsHiddenConsole(role), false);
    }
  });

  it("keeps every console tile, including Goals, for Owner and Admin", () => {
    for (const role of ["Owner", "Admin"]) {
      assert.deepEqual(consoleTilesFor(role, consoleTiles).map((tile) => tile.id), consoleTiles.map((tile) => tile.id));
      assert.equal(showsManagementConsole(role), true);
      assert.equal(showsHiddenConsole(role), true);
      assert.equal(consoleSectionVisible(null, "goals"), true);
    }
    assert.equal(consoleSectionVisible(VIEWER_CONSOLE_TILES, "goals"), false);
    assert.equal(consoleSectionVisible(VIEWER_CONSOLE_TILES, "reports"), false);
    assert.equal(consoleSectionVisible(VIEWER_CONSOLE_TILES, "pipeline"), true);
    assert.equal(consoleSectionVisible(VIEWER_CONSOLE_TILES, "mchome"), true);
  });

  it("still hides the management console from Sales and Production", () => {
    assert.equal(showsManagementConsole("Sales"), false);
    assert.equal(showsManagementConsole("Production"), false);
    assert.equal(showsHiddenConsole("Sales"), false);
    assert.equal(showsHiddenConsole("Production"), false);
  });
});

describe("+ New buttons", () => {
  it("hides create buttons for Viewer, unknown roles and Production", () => {
    for (const role of ["Viewer", "Intern", "viewer", "", null, "Production"]) {
      assert.equal(showsNewButtons(role), false);
    }
  });

  it("keeps create buttons for Owner, Admin and Sales", () => {
    for (const role of ["Owner", "Admin", "Sales"]) {
      assert.equal(showsNewButtons(role), true);
    }
  });
});

describe("openProject guard", () => {
  it("does not open a project or contact for Viewer or an unknown role", () => {
    for (const role of ["Viewer", "Intern", "viewer", null]) {
      let opened = null;
      assert.equal(canOpenRecords(role), false);
      assert.equal(openRecord(role, { id: 9 }, (detail) => { opened = detail; }, "project"), false);
      assert.equal(openRecord(role, { id: 4 }, (detail) => { opened = detail; }, "contact"), false);
      assert.equal(opened, null);
      assert.equal(canShowProjectView(role, "list"), false);
      assert.equal(canShowProjectView(role, "contactsview"), false);
      assert.equal(canShowProjectView(role, "hiddenconsole"), false);
      assert.equal(canShowProjectView(role, "goals"), false);
      assert.equal(canShowProjectView(role, "pipeline"), true);
      assert.equal(canShowProjectView(role, "dashboard"), true);
      assert.equal(canShowProjectView(role, "lifedeath"), true);
      assert.equal(canShowProjectView(role, "mgmtconsole"), true);
    }
  });

  it("still opens records for Owner, Admin, Sales and Production", () => {
    for (const role of STAFF_ROLES) {
      let opened = null;
      const project = { id: role.length };
      assert.equal(openRecord(role, project, (detail) => { opened = detail; }, "project"), true);
      assert.deepEqual(opened, { type: "project", data: project });
      assert.equal(canShowProjectView(role, "list"), true);
      assert.equal(canShowProjectView(role, "hiddenconsole"), true);
      assert.equal(homeTileVisible(role, { id: "pipeline", roles: ["Owner", "Admin", "Sales", "Production"] }), true);
    }
  });

  it("ignores a missing record", () => {
    let opened = false;
    assert.equal(openRecord("Owner", null, () => { opened = true; }, "project"), false);
    assert.equal(opened, false);
  });
});

describe("View as", () => {
  it("offers Viewer to the Owner only", () => {
    assert.deepEqual(VIEW_AS_ROLES, ["Admin", "Sales", "Production", "Viewer"]);
    assert.equal(canUseViewAs("Owner"), true);
    for (const role of ["Admin", "Sales", "Production", "Viewer", "Intern"]) {
      assert.equal(canUseViewAs(role), false);
    }
  });
});

describe("App.jsx wires the Viewer policy", () => {
  it("gates create buttons, record opens, the console and View as", () => {
    assert.match(appSource, /showsNewButtons\(effectiveRole\)&&<button[^>]*>\+ New project<\/button>/);
    assert.match(appSource, /showsNewButtons\(effectiveRole\)&&<button[^>]*>\+ New contact<\/button>/);
    assert.match(appSource, /showNewProject&&showsNewButtons\(effectiveRole\)&&<NewProjectModal/);
    assert.match(appSource, /showNewContact&&showsNewButtons\(effectiveRole\)&&<NewContactModal/);
    assert.match(appSource, /const openProject = p => \{ openRecord\(effectiveRole, p, setDetailView, "project"\); \};/);
    assert.match(appSource, /const openContact = c => \{ openRecord\(effectiveRole, c, setDetailView, "contact"\); \};/);
    assert.match(appSource, /const showDetail = !!\(detailView && canOpenRecords\(effectiveRole\)\)/);
    assert.match(appSource, /display:showDetail\?"none":"block"/);
    assert.match(appSource, /onOpenContact=\{openContact\}/);
    assert.match(appSource, /onOpenProject=\{openProject\}/);
    assert.match(appSource, /visibleHomeTiles\(effectiveRole,/);
    assert.match(appSource, /if \(!homeTileVisible\(effectiveRole, item\)\) return;/);
    assert.match(appSource, /<option value="Viewer">View as Viewer<\/option>/);
    assert.match(appSource, /authUser\.role==="Owner"&&<select/);
    assert.match(appSource, /showsManagementConsole\(effectiveRole\)/);
    assert.match(appSource, /projectView==="hiddenconsole"&&mgmtAccess&&<HiddenConsole/);
    assert.match(appSource, /allowed=\{isLimited\?VIEWER_CONSOLE_TILES:null\}/);
    assert.match(appSource, /readOnly=\{isProduction\|\|isLimited\}/);
    assert.match(appSource, /consoleSectionVisible\(allowed, "goals"\) && <GoalsSettings/);
    assert.match(appSource, /section==="mchome"&&!allowed&&/);
    assert.match(appSource, /\.filter\(function\(item\)\{ return !allowed \|\| allowed\.indexOf\(item\.id\) >= 0; \}\)/);
    assert.equal(appSource.includes('id:"list",label:"Projects"'), true);
    assert.equal((appSource.match(/<option value="Viewer">/g) || []).length, 1);
  });
});
