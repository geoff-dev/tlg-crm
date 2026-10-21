-- Staff PIN auth (additive). Shared project dwkvdelvnmvniewakzoq.
--
-- DO NOT APPLY until the pin-login edge function is deployed in the same
-- release window as the new LMS client and the tlg-scheduler staff-token
-- client. This file does not revoke anon access; that is the next migration
-- and it will break the live LMS if it lands first.
--
-- Safe to apply before the revoke: existing plaintext pins stay in place
-- for a one-week rollback. The anon key can still read them until
-- 20261021120100_crm_users_revoke_anon.sql runs.
--
-- pgcrypto 1.3 is already installed in the extensions schema on this project.

create schema if not exists extensions;

create extension if not exists pgcrypto with schema extensions;

alter table public.crm_users
  add column if not exists pin_hash text,
  add column if not exists pin_set_at timestamptz,
  add column if not exists token_version integer not null default 1;

comment on column public.crm_users.pin_hash is
  'bcrypt (pgcrypto bf cost 10) of the 6-digit PIN. Login compares with extensions.crypt.';
comment on column public.crm_users.pin_set_at is
  'When pin_hash was last written.';
comment on column public.crm_users.token_version is
  'Bumped to invalidate outstanding staff tokens for this user. Verifiers re-read it.';
comment on column public.crm_users.pin is
  'Plaintext rollback copy. Unreadable after the anon revoke. Null and drop in a later cleanup; do not drop in this migration.';

-- Existing PINs are unchanged. Re-running does not rehash a row that already has pin_hash.
update public.crm_users
set
  pin_hash = extensions.crypt(pin, extensions.gen_salt('bf', 10)),
  pin_set_at = now()
where pin_hash is null
  and pin is not null
  and length(pin) > 0;

create table if not exists public.staff_login_attempts (
  id bigint generated always as identity primary key,
  at timestamptz not null default now(),
  ip_hash text not null,
  ok boolean not null,
  crm_user_id integer,
  app text
);

comment on table public.staff_login_attempts is
  'PIN login audit. ip_hash is HMAC-SHA256(STAFF_IP_SALT, client ip), never the raw IP. Service role only.';

create index if not exists staff_login_attempts_at_idx
  on public.staff_login_attempts (at desc);

create index if not exists staff_login_attempts_ip_idx
  on public.staff_login_attempts (ip_hash, at desc);

create index if not exists staff_login_attempts_user_idx
  on public.staff_login_attempts (crm_user_id, at desc);

create table if not exists public.staff_login_alerts (
  id bigint generated always as identity primary key,
  at timestamptz not null default now(),
  kind text not null,
  detail jsonb not null default '{}'::jsonb
);

comment on table public.staff_login_alerts is
  'Written when the global PIN pause trips (more than 30 failures in 60 minutes). Service role only.';

revoke all privileges on table public.staff_login_attempts from public, anon, authenticated;
revoke all privileges on table public.staff_login_alerts from public, anon, authenticated;

grant select, insert on table public.staff_login_attempts to service_role;
grant select, insert on table public.staff_login_alerts to service_role;

alter table public.staff_login_attempts enable row level security;
alter table public.staff_login_alerts enable row level security;

-- No policies. anon/authenticated have no grant and no policy.
-- service_role on Supabase has BYPASSRLS, which is how pin-login writes attempts.

-- Fixed cost-10 hash of a non-PIN string. Used only to spend a bcrypt compare
-- when the user id is missing or inactive, so that path is not cheaper.
-- It is not a staff PIN and does not unlock anyone.

create or replace function public.staff_check_pin(p_user_id integer, p_pin text)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  rec public.crm_users%rowtype;
  dummy constant text := '$2a$10$ApCe9vtcRHc33NcNnt2sL.fz4I/QzO5Jd9TUcnc9wFp1zgCgMAKOO';
begin
  if p_pin is null or p_pin !~ '^[0-9]{6}$' then
    perform extensions.crypt(coalesce(p_pin, ''), dummy);
    return jsonb_build_object('ok', false);
  end if;

  select * into rec from public.crm_users where id = p_user_id;
  if not found or rec.active is distinct from true or rec.pin_hash is null then
    perform extensions.crypt(p_pin, dummy);
    return jsonb_build_object('ok', false);
  end if;

  if rec.pin_hash is distinct from extensions.crypt(p_pin, rec.pin_hash) then
    return jsonb_build_object('ok', false);
  end if;

  return jsonb_build_object(
    'ok', true,
    'id', rec.id,
    'name', rec.name,
    'role', rec.role,
    'tv', rec.token_version,
    'active', true
  );
end;
$$;

create or replace function public.staff_session(p_id integer)
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
  select jsonb_build_object(
    'id', id,
    'name', name,
    'role', role,
    'active', active,
    'tv', token_version
  )
  from public.crm_users
  where id = p_id;
$$;

create or replace function public.staff_list_names()
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
  select coalesce(
    jsonb_agg(jsonb_build_object('id', id, 'name', name) order by name),
    '[]'::jsonb
  )
  from public.crm_users
  where active is true;
$$;

create or replace function public.set_staff_pin(p_id integer, p_pin text)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  other record;
  new_hash text;
begin
  if p_pin is null or p_pin !~ '^[0-9]{6}$' then
    return jsonb_build_object('ok', false, 'error', 'pin must be 6 digits');
  end if;
  if p_pin ~ '^([0-9])\1{5}$'
     or p_pin in ('123456', '654321', '012345', '543210', '987654', '456789') then
    return jsonb_build_object('ok', false, 'error', 'pin is too easy to guess');
  end if;
  if not exists (select 1 from public.crm_users where id = p_id) then
    return jsonb_build_object('ok', false, 'error', 'user not found');
  end if;

  for other in
    select id, pin_hash
    from public.crm_users
    where id <> p_id
      and active is true
      and pin_hash is not null
  loop
    if other.pin_hash = extensions.crypt(p_pin, other.pin_hash) then
      return jsonb_build_object('ok', false, 'error', 'pin already used');
    end if;
  end loop;

  new_hash := extensions.crypt(p_pin, extensions.gen_salt('bf', 10));
  update public.crm_users
  set
    pin_hash = new_hash,
    pin_set_at = now(),
    pin = p_pin,
    token_version = token_version + 1
  where id = p_id;

  return jsonb_build_object('ok', true);
end;
$$;

revoke all on function public.staff_check_pin(integer, text) from public, anon, authenticated;
revoke all on function public.staff_session(integer) from public, anon, authenticated;
revoke all on function public.staff_list_names() from public, anon, authenticated;
revoke all on function public.set_staff_pin(integer, text) from public, anon, authenticated;

grant execute on function public.staff_check_pin(integer, text) to service_role;
grant execute on function public.staff_session(integer) to service_role;
grant execute on function public.staff_list_names() to service_role;
grant execute on function public.set_staff_pin(integer, text) to service_role;

comment on function public.staff_check_pin(integer, text) is
  'One bcrypt compare for a chosen staff id. Service role only. Does not log attempts.';
comment on function public.staff_session(integer) is
  'Re-read id, name, role, active, token_version. No PIN. Service role only.';
comment on function public.staff_list_names() is
  'Active staff id and name only, for the tap-name screen. No PIN, no role.';
comment on function public.set_staff_pin(integer, text) is
  'Service-role PIN reset. Rejects weak and duplicate PINs, bumps token_version.';
