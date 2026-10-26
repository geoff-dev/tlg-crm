import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { PGlite } from "@electric-sql/pglite";
import { pgcrypto } from "@electric-sql/pglite/contrib/pgcrypto";
import { signStaffToken } from "../functions/pin-login/token.js";
import { TABLES, expectedAllow, generateHarnessSql, generateLockSql } from "./write_guard_tables.mjs";

const SECRET = "test-secret-not-real-" + "x".repeat(24);
const ACTORS = [
  "no token",
  "garbage token",
  "Owner",
  "Admin",
  "Sales",
  "Production",
  "Viewer",
  "deactivated user",
  "stale tv",
  "expired",
  "wrong secret",
  "alg=none",
  "role-claim mismatch",
];

function b64url(value) {
  const buf = Buffer.isBuffer(value) ? value : Buffer.from(JSON.stringify(value));
  return buf.toString("base64url");
}

function signRaw(header, payload, secret) {
  const data = `${b64url(header)}.${b64url(payload)}`;
  const sig = createHmac("sha256", secret).update(data).digest("base64url");
  return `${data}.${sig}`;
}

function sqlBody(text) {
  const start = text.indexOf("create schema if not exists private;");
  assert.equal(start >= 0, true);
  return text.slice(start).trim();
}

describe("staff write guard files", () => {
  it("builds the lock SQL from the one section-4 table list", () => {
    assert.equal(TABLES.length, 14);
    assert.deepEqual(
      TABLES.filter((table) => table.rule === "staff_is_admin").map((table) => table.name),
      ["inventory", "vault", "employees", "sops"],
    );
    const generated = generateLockSql(TABLES);
    const saved = readFileSync(new URL("./lock_tables.sql", import.meta.url), "utf8");
    assert.equal(saved, generated);
    for (const table of TABLES) {
      const fn = table.rule === "staff_is_admin" ? "private.staff_is_admin()" : "private.staff_may_write()";
      assert.equal(generated.includes(`public.${table.name}`), true, table.name);
      assert.equal(generated.includes(fn), true, table.name);
      if (table.rls === "off") {
        const readAt = generated.indexOf(`create policy ${table.name}_read`);
        const enableAt = generated.indexOf(`alter table public.${table.name} enable row level security;`);
        assert.equal(readAt >= 0 && enableAt > readAt, true, table.name);
      }
    }
    assert.equal(generated.includes(SECRET), false);
  });

  it("saves the U1 migration and rollback as NOT APPLIED review copies", () => {
    const helper = readFileSync(new URL("./staff_write_guard.sql", import.meta.url), "utf8");
    const migration = readFileSync(new URL("../migrations/20261026120000_staff_write_guard.sql", import.meta.url), "utf8");
    const rollback = readFileSync(new URL("../rollbacks/20261026120000_staff_write_guard_rollback.sql", import.meta.url), "utf8");
    assert.match(migration, /^-- NOT APPLIED/);
    assert.match(rollback, /^-- NOT APPLIED/);
    assert.equal(sqlBody(migration), sqlBody(helper));
    assert.equal(migration.includes("create policy"), false);
    assert.equal(migration.includes("insert into vault"), false);
    assert.match(rollback, /drop schema if exists private cascade/);
    assert.match(rollback, /vault\.delete_secret/);
    for (const text of [helper, migration, rollback]) {
      assert.equal(text.includes(SECRET), false);
      assert.equal(text.includes("other-secret"), false);
    }
  });
});

describe("local pglite staff write guard", { concurrency: false, timeout: 180000 }, () => {
  it("allows today's anon writes, then enforces the section 11 matrix", async () => {
    const db = new PGlite({ extensions: { pgcrypto } });
    const failures = [];
    const check = (name, got, want) => {
      if (got !== want) failures.push(`${name} -> ${got}, expected ${want}`);
    };

    async function as(role, token, sql) {
      const headerJson = JSON.stringify(token ? { "x-staff-token": token } : {}).replaceAll("'", "''");
      try {
        await db.exec(`reset role; select set_config('request.headers', '${headerJson}', false); set role ${role};`);
        const result = await db.query(sql);
        return { ok: true, rows: result.rows.length, data: result.rows };
      } catch (error) {
        try { await db.exec("rollback"); } catch { /* clear the aborted transaction */ }
        return { ok: false, err: String(error && error.message || error).slice(0, 200) };
      } finally {
        try {
          await db.exec("reset role");
        } catch {
          try { await db.exec("rollback; reset role;"); } catch { /* next call resets */ }
        }
      }
    }

    try {
      await db.exec(`
        create schema if not exists extensions;
        create extension if not exists pgcrypto schema extensions;
        create schema if not exists vault;
        create table vault.secrets(id uuid default gen_random_uuid(), name text, decrypted_secret text);
        create view vault.decrypted_secrets as select * from vault.secrets;
        create role anon nologin;
        create role authenticated nologin;
        grant usage on schema public to anon, authenticated;
      `);
      await db.query(
        "insert into vault.secrets(name, decrypted_secret) values ('staff_session_secret', $1)",
        [SECRET],
      );
      await db.exec(generateHarnessSql(TABLES));

      const baselineProject = await as("anon", null, "insert into public.projects(label) values ('baseline') returning id");
      const baselineInventory = await as("anon", null, "insert into public.inventory(label) values ('baseline') returning id");
      assert.equal(baselineProject.ok, true, baselineProject.err);
      assert.equal(baselineInventory.ok, true, baselineInventory.err);

      await db.exec(readFileSync(new URL("./staff_write_guard.sql", import.meta.url), "utf8"));
      await db.exec(readFileSync(new URL("./lock_tables.sql", import.meta.url), "utf8"));

      const nowSec = Math.floor(Date.now() / 1000);
      const claim = (id, role, extra = {}) => signStaffToken(
        { crm_user_id: id, name: "n", role, tv: 1, app: "lms", ...extra },
        SECRET,
        nowSec,
      ).token;
      const forgedNone = b64url({ alg: "none", typ: "JWT" }) + "." + b64url({
        iss: "tlg-staff-auth",
        aud: "tlg-staff",
        crm_user_id: 1,
        tv: 1,
        role: "Owner",
        exp: 9999999999,
      }) + ".";
      const tokens = {
        "no token": null,
        "garbage token": "a.b.c",
        Owner: claim(1, "Owner"),
        Admin: claim(2, "Admin"),
        Sales: claim(3, "Sales"),
        Production: claim(4, "Production"),
        Viewer: claim(5, "Viewer"),
        "deactivated user": claim(6, "Sales"),
        "stale tv": claim(3, "Sales", { tv: 0 }),
        expired: signStaffToken(
          { crm_user_id: 1, name: "n", role: "Owner", tv: 1, app: "lms" },
          SECRET,
          nowSec - 9 * 3600,
        ).token,
        "wrong secret": signStaffToken(
          { crm_user_id: 1, name: "n", role: "Owner", tv: 1, app: "lms" },
          "other-secret",
          nowSec,
        ).token,
        "alg=none": forgedNone,
        "role-claim mismatch": claim(5, "Owner"),
      };

      let checks = 0;
      for (const table of TABLES) {
        for (const actor of ACTORS) {
          const want = expectedAllow(table, actor);
          const token = tokens[actor];

          const inserted = await as("anon", token, `insert into public.${table.name}(label) values ('t') returning id`);
          check(`${actor} insert ${table.name}`, inserted.ok === true && inserted.rows > 0, want);
          checks += 1;

          const updated = await as("anon", token, `update public.${table.name} set label = 'seed' where id = 1 returning id`);
          check(`${actor} update ${table.name}`, updated.ok === true && updated.rows > 0, want);
          checks += 1;

          await db.exec("reset role");
          await db.exec(`insert into public.${table.name}(label) values ('d')`);
          const deleted = await as("anon", token, `delete from public.${table.name} where label = 'd' returning id`);
          check(`${actor} delete ${table.name}`, deleted.ok === true && deleted.rows > 0, want);
          checks += 1;
          await db.exec("reset role");
          await db.exec(`delete from public.${table.name} where label = 'd'`);
        }

        const anonRead = await as("anon", null, `select id from public.${table.name}`);
        check(`no token read ${table.name}`, anonRead.ok === true && anonRead.rows > 0, true);
        const viewerRead = await as("anon", tokens.Viewer, `select id from public.${table.name}`);
        check(`Viewer read ${table.name}`, viewerRead.ok === true && viewerRead.rows > 0, true);
      }
      assert.equal(checks, TABLES.length * ACTORS.length * 3);

      const viewerUpdate = await as("anon", tokens.Viewer, "update public.projects set label = 'hidden' where id = 1 returning id");
      check("Viewer update is a silent empty result", viewerUpdate.ok === true && viewerUpdate.rows === 0, true);

      const mismatchRole = await as("anon", tokens["role-claim mismatch"], "select private.staff_role() as role");
      check("role claim is ignored and the db role is used", mismatchRole.ok === true && mismatchRole.data[0].role === "Viewer", true);

      const signedNone = signRaw(
        { alg: "none", typ: "JWT" },
        { iss: "tlg-staff-auth", aud: "tlg-staff", crm_user_id: 1, tv: 1, role: "Owner", exp: 9999999999 },
        SECRET,
      );
      const noneInsert = await as("anon", signedNone, "insert into public.projects(label) values ('none') returning id");
      check("signed alg=none blocked", noneInsert.ok, false);

      const nonNumeric = signRaw(
        { alg: "HS256", typ: "JWT" },
        { iss: "tlg-staff-auth", aud: "tlg-staff", crm_user_id: "nope", tv: "nope", role: "Owner", exp: 9999999999 },
        SECRET,
      );
      const nonNumericRole = await as("anon", nonNumeric, "select private.staff_role() as role");
      check("non-numeric claims fail closed", nonNumericRole.ok === true && nonNumericRole.data[0].role === null, true);
      const nonNumericInsert = await as("anon", nonNumeric, "insert into public.projects(label) values ('bad') returning id");
      check("non-numeric claims cannot insert", nonNumericInsert.ok, false);

      await db.exec("reset role; select set_config('request.headers', 'not-json', false); set role anon;");
      let malformedRaised = false;
      let malformedRole = "raised";
      try {
        const malformed = await db.query("select private.staff_role() as role");
        malformedRole = malformed.rows[0].role;
      } catch {
        malformedRaised = true;
      }
      await db.exec("reset role");
      check("malformed header does not raise", malformedRaised, false);
      check("malformed header returns null", malformedRole, null);

      const secretCall = await as("anon", null, "select private.staff_secret()");
      check("anon cannot call staff_secret()", secretCall.ok, false);
      const userRows = await as("anon", null, "select * from public.crm_users");
      check("anon cannot read crm_users", userRows.ok, false);
      const noHeader = await as("anon", null, "select private.staff_role() as role");
      check("staff_role() is null without a token", noHeader.ok === true && noHeader.data[0].role === null, true);

      const authDenied = await as("authenticated", null, "insert into public.projects(label) values ('auth') returning id");
      check("authenticated without a token is blocked", authDenied.ok, false);
      const authOwner = await as("authenticated", tokens.Owner, "insert into public.projects(label) values ('auth') returning id");
      check("authenticated with an Owner token can write", authOwner.ok === true && authOwner.rows > 0, true);
      const authSalesInventory = await as("authenticated", tokens.Sales, "insert into public.inventory(label) values ('auth') returning id");
      check("Sales cannot write inventory", authSalesInventory.ok, false);

      await db.exec(`reset role;
        create table public.rls_order_blocked(id int primary key, label text);
        insert into public.rls_order_blocked values (1, 'seed');
        grant select on public.rls_order_blocked to anon;
        alter table public.rls_order_blocked enable row level security;
        create table public.rls_order_ok(id int primary key, label text);
        insert into public.rls_order_ok values (1, 'seed');
        grant select on public.rls_order_ok to anon;
        create policy rls_order_ok_read on public.rls_order_ok for select to anon, authenticated using (true);
        alter table public.rls_order_ok enable row level security;
      `);
      const blockedRead = await as("anon", null, "select id from public.rls_order_blocked");
      check("enable rls without a read policy hides rows", blockedRead.ok === true && blockedRead.rows === 0, true);
      const keptRead = await as("anon", null, "select id from public.rls_order_ok");
      check("read policy before enable rls keeps the row visible", keptRead.ok === true && keptRead.rows === 1, true);

      await db.exec("reset role");
      await db.exec(readFileSync(new URL("../rollbacks/20261026120000_staff_write_guard_rollback.sql", import.meta.url), "utf8"));
      const gone = await db.query("select to_regnamespace('private') as ns");
      assert.equal(gone.rows[0].ns, null);

      if (failures.length) {
        throw new Error(failures.length + " failed\n" + failures.join("\n"));
      }
    } finally {
      await db.close();
    }
  });
});
