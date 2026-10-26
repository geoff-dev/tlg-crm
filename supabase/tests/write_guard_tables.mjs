// Single table list for the local staff write-guard harness.
// Every table in section 4 of the database-lock plan, with that section's write rule.
// staff_is_admin = Owner/Admin. staff_may_write = Owner/Admin/Sales/Production.
// Viewer and every bad-token case are denied. This file is not applied anywhere.

export const TABLES = [
  { name: "projects", rule: "staff_may_write", rls: "on" },
  { name: "contacts", rule: "staff_may_write", rls: "on" },
  { name: "change_orders", rule: "staff_may_write", rls: "on" },
  { name: "activity_log", rule: "staff_may_write", rls: "on" },
  { name: "goals", rule: "staff_may_write", rls: "on" },
  { name: "project_orders", rule: "staff_may_write", rls: "off" },
  { name: "trade_partners", rule: "staff_may_write", rls: "off" },
  { name: "crm_items", rule: "staff_may_write", rls: "off" },
  { name: "crm_item_notes", rule: "staff_may_write", rls: "off" },
  { name: "inventory", rule: "staff_is_admin", rls: "off" },
  { name: "vault", rule: "staff_is_admin", rls: "off" },
  { name: "employees", rule: "staff_is_admin", rls: "off" },
  { name: "sops", rule: "staff_is_admin", rls: "off" },
  { name: "team_members", rule: "staff_may_write", rls: "on" },
];

const ALLOWED = {
  staff_may_write: ["Owner", "Admin", "Sales", "Production"],
  staff_is_admin: ["Owner", "Admin"],
};

export function expectedAllow(table, actor) {
  const roles = ALLOWED[table.rule];
  if (!roles) throw new Error("unknown write rule for " + table.name);
  return roles.includes(actor);
}

function policyFunction(rule) {
  if (rule === "staff_is_admin") return "private.staff_is_admin()";
  if (rule === "staff_may_write") return "private.staff_may_write()";
  throw new Error("unknown write rule " + rule);
}

export function generateLockSql(tables) {
  const lines = [
    "-- Local test helper. NOT a live migration. NOT APPLIED to any database.",
    "-- Generated from the single table list in supabase/tests/write_guard_tables.mjs.",
    "-- RLS already on: drop the open anon write policies and add staff write policies. The read policy stays.",
    "-- RLS currently off: create the read-everything policy FIRST, then staff write policies, then enable RLS.",
    "begin;",
  ];
  for (const table of tables) {
    const fn = policyFunction(table.rule);
    if (table.rls === "on") {
      lines.push(`drop policy if exists "Allow anon write" on public.${table.name};`);
      lines.push(`drop policy if exists "Allow anon update" on public.${table.name};`);
      lines.push(`drop policy if exists "Allow anon delete" on public.${table.name};`);
    } else if (table.rls === "off") {
      lines.push(`create policy ${table.name}_read on public.${table.name} for select to anon, authenticated using (true);`);
    } else {
      throw new Error("unknown rls state for " + table.name);
    }
    lines.push(`create policy ${table.name}_staff_insert on public.${table.name} for insert to anon, authenticated with check ((select ${fn}));`);
    lines.push(`create policy ${table.name}_staff_update on public.${table.name} for update to anon, authenticated using ((select ${fn})) with check ((select ${fn}));`);
    lines.push(`create policy ${table.name}_staff_delete on public.${table.name} for delete to anon, authenticated using ((select ${fn}));`);
    if (table.rls === "off") {
      lines.push(`alter table public.${table.name} enable row level security;`);
    }
  }
  lines.push("commit;");
  return lines.join("\n") + "\n";
}

export function generateHarnessSql(tables) {
  const lines = [
    "create table public.crm_users(id int primary key, name text, role text, active boolean, token_version int);",
    "insert into public.crm_users(id, name, role, active, token_version) values",
    "(1,'o','Owner',true,1),",
    "(2,'a','Admin',true,1),",
    "(3,'s','Sales',true,1),",
    "(4,'p','Production',true,1),",
    "(5,'v','Viewer',true,1),",
    "(6,'x','Sales',false,1);",
  ];
  for (const table of tables) {
    lines.push(`create table public.${table.name}(id serial primary key, label text);`);
  }
  const qualified = tables.map((table) => "public." + table.name).join(", ");
  lines.push(`grant select, insert, update, delete on ${qualified} to anon, authenticated;`);
  lines.push("grant usage, select on all sequences in schema public to anon, authenticated;");
  for (const table of tables) {
    if (table.rls !== "on") continue;
    lines.push(`alter table public.${table.name} enable row level security;`);
    lines.push(`create policy "Allow anon read" on public.${table.name} for select to anon, authenticated using (true);`);
    lines.push(`create policy "Allow anon write" on public.${table.name} for insert to anon, authenticated with check (true);`);
    lines.push(`create policy "Allow anon update" on public.${table.name} for update to anon, authenticated using (true);`);
    lines.push(`create policy "Allow anon delete" on public.${table.name} for delete to anon, authenticated using (true);`);
  }
  for (const table of tables) {
    lines.push(`insert into public.${table.name}(label) values ('seed');`);
  }
  return lines.join("\n") + "\n";
}
