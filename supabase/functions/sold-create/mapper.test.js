import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  buildOpsProjectRow,
  computePhasePlan,
  extractProjectId,
  formatAddress,
  formatClientName,
  isFlagEnabled,
  isKitchenOrWholeHouse,
  isPrimaryBathroom,
  mapSoldCreate,
  resolveBathTemplateKey,
} from "./mapper.js";
import { PROJECT_TEMPLATES } from "./templates.js";

describe("isFlagEnabled", () => {
  it("is off by default and for falsey values", () => {
    assert.equal(isFlagEnabled(undefined), false);
    assert.equal(isFlagEnabled(null), false);
    assert.equal(isFlagEnabled("false"), false);
    assert.equal(isFlagEnabled("FALSE"), false);
    assert.equal(isFlagEnabled(""), false);
    assert.equal(isFlagEnabled("off"), false);
  });

  it("accepts explicit true tokens", () => {
    assert.equal(isFlagEnabled(true), true);
    assert.equal(isFlagEnabled("true"), true);
    assert.equal(isFlagEnabled("TRUE"), true);
    assert.equal(isFlagEnabled("1"), true);
    assert.equal(isFlagEnabled("yes"), true);
  });
});

describe("primary bathroom + template mapping", () => {
  it("does not treat generic Bathroom Remodel as Primary Bathroom", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Haskin Residence" };
    assert.equal(isPrimaryBathroom(p), false);
    assert.equal(resolveBathTemplateKey(p), null);
  });

  it("maps Primary Bathroom + LVP", () => {
    const p = { project_type: "Primary Bathroom", job_name: "Smith — LVP" };
    assert.equal(isPrimaryBathroom(p), true);
    assert.equal(resolveBathTemplateKey(p), "bath-lvp");
  });

  it("maps name-only primary bath + tile floor", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Jones Primary Bath tile floor" };
    assert.equal(isPrimaryBathroom(p), true);
    assert.equal(resolveBathTemplateKey(p), "bath-tile");
  });

  it("creates a shell when primary bath but LVP vs tile is unclear", () => {
    const p = { project_type: "Primary Bathroom", job_name: "Adams Residence" };
    assert.equal(isPrimaryBathroom(p), true);
    assert.equal(resolveBathTemplateKey(p), null);
    assert.deepEqual(mapSoldCreate(p, null).phases, []);
  });

  it("never applies bath phases to kitchen or whole-house", () => {
    const kitchen = { project_type: "Kitchen Remodel", job_name: "Primary Bath leftover LVP" };
    const whole = { project_type: "Whole House Renovation", job_name: "Primary Bathroom tile floor" };
    assert.equal(isKitchenOrWholeHouse(kitchen), true);
    assert.equal(isKitchenOrWholeHouse(whole), true);
    assert.equal(resolveBathTemplateKey(kitchen), null);
    assert.equal(resolveBathTemplateKey(whole), null);
  });

  it("does not treat powder bath as primary", () => {
    const p = { project_type: "Bathroom Remodel", job_name: "Martin - Powder Bath LVP" };
    assert.equal(isPrimaryBathroom(p), false);
    assert.equal(resolveBathTemplateKey(p), null);
  });
});

describe("field mapping", () => {
  it("maps CRM fields onto a forecast ops row and keeps date_sold as metadata", () => {
    const project = {
      id: "11111111-1111-1111-1111-111111111111",
      job_name: "Test Residence",
      project_type: "Bathroom Remodel",
      date_sold: "2026-09-01",
      job_address: "123 Main",
      job_city: "Carmel",
      job_state: "IN",
      job_zip: "46032",
    };
    const contact = { first_name: "Ada", last_name: "Lovelace", email: "ada@example.com" };
    const row = buildOpsProjectRow(project, contact, 3);
    assert.equal(row.name, "Test Residence");
    assert.equal(row.project_type, "Bathroom Remodel");
    assert.equal(row.client_name, "Ada Lovelace");
    assert.equal(row.client_email, "ada@example.com");
    assert.equal(row.address, "123 Main Carmel, IN 46032");
    assert.equal(row.status, "forecast");
    assert.equal(row.is_published, false);
    assert.equal(row.crm_project_id, project.id);
    assert.equal(row.queue_position, 3);
    assert.match(row.forecast_notes, /date_sold=2026-09-01/);
    assert.equal("estimated_start" in row, false);
    assert.equal("estimated_end" in row, false);
    assert.equal("actual_start" in row, false);
  });

  it("formats contact-only address and name", () => {
    assert.equal(formatClientName({ full_name: "Pat Smith" }), "Pat Smith");
    assert.equal(
      formatAddress({}, { address: "9 Oak", city: "Fishers", state: "IN", zip: "46038" }),
      "9 Oak Fishers, IN 46038",
    );
  });
});

describe("phase plan (offsets, no calendar dates)", () => {
  it("seeds bath-lvp with planned_days / start_day_offset / deps", () => {
    const plan = computePhasePlan("bath-lvp");
    assert.equal(plan.length, PROJECT_TEMPLATES["bath-lvp"].phases.length);
    assert.equal(plan[0].start_day_offset, 0);
    assert.equal(plan[0].planned_days, 1);
    assert.equal(plan[1].depIdx, 0);
    assert.equal(plan[1].start_day_offset, 1);
    const glass = plan.find((p) => p.name === "Shower Glass Install");
    assert.ok(glass);
    assert.equal(glass.lag_days, 9);
    assert.equal(glass.depIdx, 10);
    const measure = plan[10];
    assert.equal(glass.start_day_offset, measure.start_day_offset + measure.planned_days + 9);
    for (const p of plan) {
      assert.equal("planned_start" in p, false);
    }
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
