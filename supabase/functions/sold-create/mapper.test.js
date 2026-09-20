import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  buildOpsProjectRow,
  colorFromPalette,
  computePhasePlan,
  extractProjectId,
  forecastBaseOffset,
  formatAddress,
  formatClientName,
  isBathroomRemodelType,
  isFlagEnabled,
  isKitchenOrOther,
  mapSoldCreate,
  nextQueuePosition,
  resolveBathTemplateKey,
} from "./mapper.js";
import { PROJECT_TEMPLATES, PROJ_PALETTE } from "./templates.js";

describe("isFlagEnabled", () => {
  it("is off by default and for falsey values", () => {
    assert.equal(isFlagEnabled(undefined), false);
    assert.equal(isFlagEnabled(null), false);
    assert.equal(isFlagEnabled("false"), false);
    assert.equal(isFlagEnabled(""), false);
  });

  it("accepts explicit true tokens", () => {
    assert.equal(isFlagEnabled(true), true);
    assert.equal(isFlagEnabled("true"), true);
    assert.equal(isFlagEnabled("1"), true);
  });
});

describe("template mapping — Bathroom Remodel + explicit floor only", () => {
  it("Bathroom Remodel without floor signal is a shell", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Haskin Residence" };
    assert.equal(isBathroomRemodelType(p), true);
    assert.equal(resolveBathTemplateKey(p), null);
    assert.deepEqual(mapSoldCreate(p, null).phases, []);
  });

  it("Bathroom Remodel + LVP maps bath-lvp", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Smith Residence LVP" };
    assert.equal(resolveBathTemplateKey(p), "bath-lvp");
  });

  it("Bathroom Remodel + tile floor maps bath-tile", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Jones tile floor" };
    assert.equal(resolveBathTemplateKey(p), "bath-tile");
  });

  it("does not guess tile floor from the word tile alone", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Jones shower tile" };
    assert.equal(resolveBathTemplateKey(p), null);
  });

  it("does not guess when both LVP and tile floor are present", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Smith LVP tile floor" };
    assert.equal(resolveBathTemplateKey(p), null);
  });

  it("kitchen and everything else are shells even with a floor word", () => {
    const kitchen = { project_type: "Kitchen Remodel", job_name: "Smith LVP" };
    const whole = { project_type: "Whole House Renovation", job_name: "Adams tile floor" };
    const flooring = { project_type: "Flooring", job_name: "LVP install" };
    assert.equal(isKitchenOrOther(kitchen), true);
    assert.equal(resolveBathTemplateKey(kitchen), null);
    assert.equal(resolveBathTemplateKey(whole), null);
    assert.equal(resolveBathTemplateKey(flooring), null);
  });
});

describe("saveNewProject field mapping", () => {
  it("writes the forecast payload only — no calendar dates", () => {
    const project = {
      id: "11111111-1111-1111-1111-111111111111",
      job_name: "Test Residence",
      project_type: "Bathroom Remodel",
      date_sold: "2026-09-01",
    };
    const contact = {
      first_name: "Ada",
      last_name: "Lovelace",
      email: "ada@example.com",
      address: "123 Main",
      city: "Carmel",
      state: "IN",
      zip: "46032",
    };
    const row = buildOpsProjectRow(project, contact, {
      queuePosition: 3,
      color: colorFromPalette(25),
    });
    assert.deepEqual(Object.keys(row).sort(), [
      "address",
      "client_email",
      "client_name",
      "color",
      "crm_project_id",
      "is_published",
      "name",
      "project_type",
      "queue_position",
      "status",
    ]);
    assert.equal(row.name, "Test Residence");
    assert.equal(row.project_type, "Bathroom Remodel");
    assert.equal(row.client_name, "Ada Lovelace");
    assert.equal(row.client_email, "ada@example.com");
    assert.equal(row.address, "123 Main Carmel, IN 46032");
    assert.equal(row.status, "forecast");
    assert.equal(row.is_published, false);
    assert.equal(row.crm_project_id, project.id);
    assert.equal(row.queue_position, 3);
    assert.equal(row.color, PROJ_PALETTE[25 % PROJ_PALETTE.length]);
    assert.equal("estimated_start" in row, false);
    assert.equal("estimated_end" in row, false);
    assert.equal("date_locked" in row, false);
    assert.equal("forecast_notes" in row, false);
  });

  it("takes client fields from contacts when present", () => {
    assert.equal(formatClientName({ full_name: "Pat Smith" }), "Pat Smith");
    assert.equal(
      formatAddress({ address: "9 Oak", city: "Fishers", state: "IN", zip: "46038" }),
      "9 Oak Fishers, IN 46038",
    );
    assert.equal(formatAddress(null), null);
  });

  it("queue_position is max same status + 1", () => {
    assert.equal(nextQueuePosition([]), 1);
    assert.equal(nextQueuePosition([{ queue_position: 4 }, { queue_position: 1 }]), 5);
  });
});

describe("phase offsets match saveNewProject (baseOffset+i)", () => {
  it("forecast baseOffset is max(start_day_offset+planned_days)+1", () => {
    assert.equal(forecastBaseOffset([]), 1);
    assert.equal(
      forecastBaseOffset([
        { start_day_offset: 10, planned_days: 5 },
        { start_day_offset: 2, planned_days: 1 },
      ]),
      16,
    );
  });

  it("seeds bath-lvp with sequential offsets and template lag, no calendar dates", () => {
    const plan = computePhasePlan("bath-lvp", 16);
    assert.equal(plan.length, PROJECT_TEMPLATES["bath-lvp"].phases.length);
    plan.forEach((p, i) => {
      assert.equal(p.start_day_offset, 16 + i);
      assert.equal(p.sort_order, i + 1);
      assert.equal(p.status, "upcoming");
      assert.equal(p.days_worked, 0);
      assert.equal("planned_start" in p, false);
    });
    const glass = plan.find((p) => p.name === "Shower Glass Install");
    assert.equal(glass.lag_days, 9);
    assert.equal(glass.depIdx, 10);
  });

  it("returns no phases for an unknown template key", () => {
    assert.deepEqual(computePhasePlan("kitchen"), []);
  });
});

describe("webhook payload", () => {
  it("extracts project id from Database Webhook or direct invoke", () => {
    assert.equal(extractProjectId({ project_id: "abc" }), "abc");
    assert.equal(extractProjectId({ record: { id: "def", stage: "Sold" } }), "def");
    assert.equal(extractProjectId({}), null);
  });
});
