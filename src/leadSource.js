import { createElement } from "react";

// A stored lead source stays selectable when it is not in the canonical list,
// so the edit form can show it and a save does not blank the column.
export function leadSourceOptions(sources, storedValue) {
  const list = Array.isArray(sources) ? sources.slice() : [];
  if (storedValue == null) return list;
  const value = String(storedValue);
  if (value !== "" && list.indexOf(value) === -1) list.push(value);
  return list;
}

export function leadSourceUpdateFields(form) {
  const f = form || {};
  return {
    lead_source: f.lead_source ? f.lead_source : null,
    lead_source_secondary: f.lead_source_secondary ? f.lead_source_secondary : null,
  };
}

export function projectFormFromRecord(record) {
  const row = record && typeof record === "object" ? record : {};
  return {
    ...row,
    lead_source: row.lead_source || "",
    lead_source_secondary: row.lead_source_secondary || "",
  };
}

export function LeadSourceSelect({ value, sources, emptyLabel, onChange, style, label }) {
  const selected = value || "";
  const options = leadSourceOptions(sources, selected);
  return createElement(
    "select",
    { style, value: selected, onChange, "aria-label": label },
    [
      createElement("option", { key: "__empty", value: "" }, emptyLabel),
      ...options.map((s) => createElement("option", { key: s, value: s }, s)),
    ]
  );
}
