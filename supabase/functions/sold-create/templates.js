// First-pass bath templates from live tlg-scheduler.vercel.app (2026-09-20).
// tlg-scheduler GitHub was not readable from this agent; these match the
// attached project_templates_excerpt.js and the live SPA PROJECT_TEMPLATES.
// Keys only: bath-lvp, bath-tile. Never apply these to kitchen / whole-house.

export const PROJECT_TEMPLATES = {
  "bath-lvp": {
    name: "Primary Bath — LVP Floor",
    type: "Primary Bathroom",
    phases: [
      { name: "Site Prep", days: 1, who: "crew" },
      { name: "Demo", days: 1, who: "sub", depIdx: 0 },
      { name: "Framing", days: 2, who: "crew", depIdx: 1 },
      { name: "Rough Plumbing", days: 2, who: "sub", role: "Plumbing", depIdx: 2 },
      { name: "Rough Electrical", days: 1, who: "sub", role: "Electrical", depIdx: 3 },
      { name: "Drywall", days: 5, who: "sub", role: "Drywall", depIdx: 4 },
      { name: "Paint Ceilings & Prime", days: 1, who: "sub", role: "Painting", depIdx: 5 },
      { name: "Tile (Walls/Shower)", days: 5, who: "crew", depIdx: 6 },
      { name: "Cabinet Install", days: 1, who: "crew", depIdx: 6 },
      { name: "Base Trim", days: 2, who: "crew", depIdx: 8 },
      { name: "Shower Glass Measure", days: 1, who: "sub", role: "Glass", depIdx: 7 },
      { name: "LVP Flooring", days: 1, who: "sub", role: "Flooring", depIdx: 7 },
      { name: "Finish Painting", days: 2, who: "sub", role: "Painting", depIdx: 11 },
      { name: "Finish Electrical", days: 1, who: "sub", role: "Electrical", depIdx: 12 },
      { name: "Plumbing — Shower & Sink", days: 1, who: "sub", role: "Plumbing", depIdx: 12 },
      { name: "Plumbing — Tub", days: 1, who: "sub", role: "Plumbing", depIdx: 14 },
      { name: "Accessories", days: 1, who: "crew", depIdx: 13 },
      { name: "Shower Glass Install", days: 1, who: "sub", role: "Glass", depIdx: 10, lag: 9 },
    ],
  },
  "bath-tile": {
    name: "Primary Bath — Tile Floor",
    type: "Primary Bathroom",
    phases: [
      { name: "Site Prep", days: 1, who: "crew" },
      { name: "Demo", days: 1, who: "sub", depIdx: 0 },
      { name: "Framing", days: 2, who: "crew", depIdx: 1 },
      { name: "Rough Plumbing", days: 2, who: "sub", role: "Plumbing", depIdx: 2 },
      { name: "Rough Electrical", days: 1, who: "sub", role: "Electrical", depIdx: 3 },
      { name: "Drywall", days: 5, who: "sub", role: "Drywall", depIdx: 4 },
      { name: "Paint Ceilings & Prime", days: 1, who: "sub", role: "Painting", depIdx: 5 },
      { name: "Tile (Walls/Shower)", days: 5, who: "crew", depIdx: 6 },
      { name: "Cabinet Install", days: 1, who: "crew", depIdx: 6 },
      { name: "Shower Glass Measure", days: 1, who: "sub", role: "Glass", depIdx: 7 },
      { name: "Concrete Board (Floor)", days: 1, who: "crew", depIdx: 7 },
      { name: "Tile Install (Floor)", days: 3, who: "crew", depIdx: 10 },
      { name: "Grout (Floor)", days: 1, who: "crew", depIdx: 11 },
      { name: "Base Trim", days: 1, who: "crew", depIdx: 12 },
      { name: "Finish Painting", days: 2, who: "sub", role: "Painting", depIdx: 13 },
      { name: "Finish Electrical", days: 1, who: "sub", role: "Electrical", depIdx: 14 },
      { name: "Plumbing — Shower & Sink", days: 1, who: "sub", role: "Plumbing", depIdx: 14 },
      { name: "Plumbing — Tub", days: 1, who: "sub", role: "Plumbing", depIdx: 16 },
      { name: "Accessories", days: 1, who: "crew", depIdx: 15 },
      { name: "Shower Glass Install", days: 1, who: "sub", role: "Glass", depIdx: 9, lag: 9 },
    ],
  },
};

export const PROJ_PALETTE = [
  "#2D6A4F",
  "#1E40AF",
  "#B45309",
  "#9333EA",
  "#DC2626",
  "#059669",
  "#BE185D",
  "#6D28D9",
  "#D97706",
  "#0891B2",
];
