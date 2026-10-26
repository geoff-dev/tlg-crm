-- NOT APPLIED
-- Status: NOT APPLIED. Human review only. Do not run this file from CI or from an agent.
-- Rollback for supabase/migrations/20261026120000_staff_write_guard.sql (unit U1).
-- Safe only while no policy references private.* . Later units must roll back first.
-- Drops the helper schema, then deletes the Vault secret by name if Vault is installed.
-- This file does not contain a secret value.

drop schema if exists private cascade;

do $rollback$
declare
  sid uuid;
begin
  if to_regnamespace('vault') is null then
    return;
  end if;
  if to_regprocedure('vault.delete_secret(uuid)') is null then
    return;
  end if;
  select s.id into sid
    from vault.secrets s
   where s.name = 'staff_session_secret'
   limit 1;
  if sid is not null then
    perform vault.delete_secret(sid);
  end if;
end
$rollback$;
