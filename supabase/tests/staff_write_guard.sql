-- Local test copy of the U1 helper functions.
-- NOT APPLIED to any database. The live migration is a separate file under supabase/migrations/.
-- The test harness inserts a TEST secret into a fake vault.decrypted_secrets view.
-- No secret value belongs in this file.

create schema if not exists private;
revoke all on schema private from public;
grant usage on schema private to anon, authenticated;

-- Reads Vault by secret name. Tests substitute a fake view with the same column. No secret value is stored here.
create or replace function private.staff_secret() returns text
language sql stable security definer set search_path = pg_catalog, vault
as $$ select decrypted_secret from vault.decrypted_secrets where name = 'staff_session_secret' limit 1 $$;

create or replace function private.b64url_decode(t text) returns bytea
language sql immutable set search_path = pg_catalog
as $$ select decode(rpad(translate(t, '-_', '+/'), length(t) + ((4 - length(t) % 4) % 4), '='), 'base64') $$;

-- Returns the caller's CURRENT crm_users.role if (and only if) the x-staff-token header
-- is a valid, unexpired HS256 token signed with the staff secret AND the user is still
-- active AND the token_version still matches. Otherwise NULL. Never raises.
create or replace function private.staff_role() returns text
language plpgsql stable security definer set search_path = pg_catalog, public, extensions
as $$
declare
  h jsonb; tok text; p text[]; secret text; expect text; hdr jsonb; c jsonb; r text;
begin
  begin h := nullif(current_setting('request.headers', true), '')::jsonb; exception when others then return null; end;
  tok := h ->> 'x-staff-token';
  if tok is null then return null; end if;
  p := string_to_array(tok, '.');
  if array_length(p, 1) is distinct from 3 then return null; end if;
  secret := private.staff_secret();
  if secret is null or secret = '' then return null; end if;
  expect := rtrim(translate(encode(hmac(convert_to(p[1] || '.' || p[2], 'utf8'), convert_to(secret, 'utf8'), 'sha256'), 'base64'), '+/', '-_'), '=');
  if expect is distinct from p[3] then return null; end if;
  hdr := convert_from(private.b64url_decode(p[1]), 'utf8')::jsonb;
  if hdr ->> 'alg' is distinct from 'HS256' then return null; end if;
  c := convert_from(private.b64url_decode(p[2]), 'utf8')::jsonb;
  if c ->> 'iss' is distinct from 'tlg-staff-auth' or c ->> 'aud' is distinct from 'tlg-staff' then return null; end if;
  if (c ->> 'exp')::bigint <= extract(epoch from now())::bigint then return null; end if;
  select u.role into r from public.crm_users u
   where u.id = (c ->> 'crm_user_id')::int and u.active is true
     and u.token_version = (c ->> 'tv')::int;
  return r;
exception when others then
  return null;
end $$;

create or replace function private.staff_may_write() returns boolean
language sql stable security definer set search_path = pg_catalog, public
as $$ select coalesce(private.staff_role() in ('Owner','Admin','Sales','Production'), false) $$;

create or replace function private.staff_is_admin() returns boolean
language sql stable security definer set search_path = pg_catalog, public
as $$ select coalesce(private.staff_role() in ('Owner','Admin'), false) $$;

revoke all on function private.staff_secret(), private.b64url_decode(text), private.staff_role(),
  private.staff_may_write(), private.staff_is_admin() from public;
grant execute on function private.staff_role(), private.staff_may_write(), private.staff_is_admin() to anon, authenticated;
