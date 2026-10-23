import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { LeadSourceSelect, leadSourceOptions, leadSourceUpdateFields, projectFormFromRecord } from "./leadSource.js";

function appSource() {
  return readFileSync(new URL("./App.jsx", import.meta.url), "utf8");
}

function readLSources(source) {
  const match = source.match(/const LSOURCES = (\[[\s\S]*?\]);/);
  assert.ok(match, "LSOURCES list is missing");
  return JSON.parse(match[1]);
}

function decodeAttr(value) {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, "\"")
    .replace(/&#x27;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function optionValues(html) {
  return [...html.matchAll(/<option\b([^>]*)>/g)].map((match) => {
    const value = match[1].match(/\bvalue="([^"]*)"/);
    return value ? decodeAttr(value[1]) : "";
  });
}

function selectedValue(html) {
  const selected = [...html.matchAll(/<option\b([^>]*)>/g)].filter((match) => /\bselected\b/.test(match[1]));
  assert.equal(selected.length, 1);
  const value = selected[0][1].match(/\bvalue="([^"]*)"/);
  return value ? decodeAttr(value[1]) : "";
}

const SANDY = {
  id: "4c07ae96-2ad9-42f6-afd2-277564064ed0",
  job_name: "Staehler - Room Addition",
  stage: "Estimated",
  lead_source: "Previous Lead",
  lead_source_secondary: "Referral - Loren Wood Builders",
};

describe("secondary lead source round trip", () => {
  it("loads a project onto the form and saves the same lead_source_secondary", () => {
    const form = projectFormFromRecord(SANDY);
    assert.equal(form.job_name, "Staehler - Room Addition");
    assert.equal(form.lead_source, "Previous Lead");
    assert.equal(form.lead_source_secondary, "Referral - Loren Wood Builders");
    assert.deepEqual(leadSourceUpdateFields(form), {
      lead_source: "Previous Lead",
      lead_source_secondary: "Referral - Loren Wood Builders",
    });
  });

  it("clears only the field set to None", () => {
    const form = projectFormFromRecord(SANDY);
    form.lead_source_secondary = "";
    assert.deepEqual(leadSourceUpdateFields(form), {
      lead_source: "Previous Lead",
      lead_source_secondary: null,
    });
  });
});

describe("lead source values outside the list", () => {
  it("keeps a stored value that is not in LSOURCES on both selects and on save", () => {
    const sources = readLSources(appSource());
    const legacyPrimary = "Angie List--Web";
    const legacySecondary = "2005";
    assert.equal(sources.includes(legacyPrimary), false);
    assert.equal(sources.includes(legacySecondary), false);

    assert.deepEqual(leadSourceOptions(sources, legacyPrimary).slice(-1), [legacyPrimary]);
    assert.equal(leadSourceOptions(sources, "Referral").filter((s) => s === "Referral").length, 1);
    assert.deepEqual(leadSourceOptions(sources, ""), sources);

    const form = projectFormFromRecord({
      lead_source: legacyPrimary,
      lead_source_secondary: legacySecondary,
    });
    assert.deepEqual(leadSourceUpdateFields(form), {
      lead_source: legacyPrimary,
      lead_source_secondary: legacySecondary,
    });

    const primaryHtml = renderToStaticMarkup(createElement(LeadSourceSelect, {
      label: "Lead source",
      value: form.lead_source,
      sources,
      emptyLabel: "Select...",
      onChange() {},
    }));
    assert.deepEqual(optionValues(primaryHtml), ["", ...sources, legacyPrimary]);
    assert.equal(selectedValue(primaryHtml), legacyPrimary);

    const secondaryHtml = renderToStaticMarkup(createElement(LeadSourceSelect, {
      label: "Secondary lead source",
      value: form.lead_source_secondary,
      sources,
      emptyLabel: "None",
      onChange() {},
    }));
    assert.deepEqual(optionValues(secondaryHtml), ["", ...sources, legacySecondary]);
    assert.equal(selectedValue(secondaryHtml), legacySecondary);
  });
});

describe("secondary dropdown", () => {
  it("populates Referral - Loren Wood Builders from the shared LSOURCES list", () => {
    const sources = readLSources(appSource());
    assert.equal(sources.includes("Referral - Loren Wood Builders"), true);
    const form = projectFormFromRecord(SANDY);
    const html = renderToStaticMarkup(createElement(LeadSourceSelect, {
      label: "Secondary lead source",
      value: form.lead_source_secondary,
      sources,
      emptyLabel: "None",
      onChange() {},
    }));
    assert.match(html, /aria-label="Secondary lead source"/);
    assert.equal(optionValues(html)[0], "");
    assert.match(html, />None<\/option>/);
    assert.deepEqual(optionValues(html), ["", ...sources]);
    assert.equal(selectedValue(html), "Referral - Loren Wood Builders");
  });
});

describe("project edit form wiring", () => {
  it("places the secondary dropdown under Lead source and saves the column", () => {
    const source = appSource();
    const detailStart = source.indexOf("function ProjectDetail");
    const detailEnd = source.indexOf("function ContactDetail");
    assert.equal(detailStart > 0 && detailEnd > detailStart, true);
    const detail = source.slice(detailStart, detailEnd);

    const leadIdx = detail.indexOf('label="Lead source"');
    const secondaryIdx = detail.indexOf('label="Secondary lead source"');
    const salespersonIdx = detail.indexOf('label="Salesperson"', secondaryIdx);
    assert.equal(leadIdx > 0 && secondaryIdx > leadIdx && salespersonIdx > secondaryIdx, true);
    assert.equal(detail.indexOf('label="Secondary lead source"', secondaryIdx + 1), -1);
    assert.match(detail, /projectFormFromRecord\(r\[0\]\)/);
    assert.match(detail, /leadSourceUpdateFields\(form\)/);
    assert.match(detail, /lead_source_secondary:leadFields\.lead_source_secondary/);
    assert.match(detail, /emptyLabel="None"/);
    assert.match(detail, /emptyLabel="Select\.\.\."/);
    assert.equal(detail.includes("LSOURCES.map"), false);

    const modalStart = source.indexOf("function NewProjectModal");
    const modal = source.slice(modalStart, source.indexOf("function NewContactModal"));
    assert.match(modal, /LSOURCES\.map/);
    assert.equal(modal.includes("lead_source_secondary"), false);
    assert.equal(source.includes("lead_source_secondary") && source.slice(detailEnd).includes("lead_source_secondary"), false);
  });
});
