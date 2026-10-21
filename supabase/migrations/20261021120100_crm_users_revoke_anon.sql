-- STOP. This is the LMS login cut-over.
--
-- Do not apply until all of these are true in the same release window:
--   1. 20261021120000_staff_pin_auth.sql is already applied.
--   2. The pin-login edge function is deployed and a real staff login works.
--   3. The LMS client from this change is the client serving production.
--   4. tlg-scheduler is deployed with the staff-token contract (or is confirmed
--      not to read crm_users, which is true of scheduler main as of this plan).
--
-- Applying this earlier turns off the live LMS. The old client signs in by
-- reading public.crm_users with the anon key. After this file commits, that
-- read is permission denied.
--
-- Rollback (emergency, restores the old insecure read):
--   grant select, insert, update, delete, truncate, references, trigger
--     on table public.crm_users to anon, authenticated;
--   alter table public.crm_users disable row level security;

revoke all privileges on table public.crm_users from anon, authenticated;
revoke all privileges on table public.crm_users from public;

revoke all privileges on sequence public.crm_users_id_seq from anon, authenticated, public;

alter table public.crm_users enable row level security;

-- No policies on purpose. anon and authenticated cannot see or change rows.
-- postgres keeps ownership. service_role keeps its existing grants and,
-- on Supabase, BYPASSRLS, so pin-login can still read hashes.
