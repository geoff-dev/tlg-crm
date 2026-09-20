-- Sold → ops_project_schedule create (flagged, default OFF)
--
-- Shared DB: dwkvdelvnmvniewakzoq
-- Owner of this migration: tlg-crm (Sold signal is public.projects.stage)
--
-- This migration:
--   1. Adds durable crm_project_id on ops_project_schedule (unique when set)
--   2. Seeds ops_app_settings.SOLD_CREATE_ENABLED = false (no-op if already set)
--   3. Adds AFTER INSERT/UPDATE trigger that calls the sold-create Edge Function
--      ONLY when stage becomes Sold AND the flag is true
--
-- Apply: supabase db push / SQL editor as a privileged role.
-- Deploy function: supabase functions deploy sold-create
--
-- Enable writes (explicit Geoff yes — never in prod config):
--   update public.ops_app_settings
--     set value = 'true'
--     where key = 'SOLD_CREATE_ENABLED';
--
-- Optional: Database Webhook (dashboard) instead of / in addition to the trigger:
--   Table: public.projects | Events: INSERT, UPDATE
--   URL: https://dwkvdelvnmvniewakzoq.supabase.co/functions/v1/sold-create
--   Header: Authorization: Bearer <service_role or anon>
--   The function re-reads the row and still no-ops unless stage is Sold and flag is true.
--
-- Optional auth for pg_net:
--   alter database postgres set app.sold_create_auth = 'Bearer <service_role>';
--   alter database postgres set app.sold_create_url  = 'https://dwkvdelvnmvniewakzoq.supabase.co/functions/v1/sold-create';

-- 1) Durable CRM ↔ ops link
alter table public.ops_project_schedule
  add column if not exists crm_project_id uuid;

create unique index if not exists ops_project_schedule_crm_project_id_uidx
  on public.ops_project_schedule (crm_project_id)
  where crm_project_id is not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ops_project_schedule_crm_project_id_fkey'
  ) then
    alter table public.ops_project_schedule
      add constraint ops_project_schedule_crm_project_id_fkey
      foreign key (crm_project_id) references public.projects(id)
      on delete set null;
  end if;
end $$;

comment on column public.ops_project_schedule.crm_project_id is
  'CRM public.projects.id. Unique when set; Sold→create idempotency key.';

-- 2) Feature flag — default OFF. Missing row is also treated as OFF by trigger + function.
insert into public.ops_app_settings (key, value)
select 'SOLD_CREATE_ENABLED', 'false'
where not exists (
  select 1 from public.ops_app_settings where key = 'SOLD_CREATE_ENABLED'
);

-- 3) Trigger → Edge Function (pg_net). No writes happen here.
create or replace function public.trg_projects_sold_create()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  flag_val text;
  fn_url text;
  auth_header text;
  headers jsonb;
begin
  if NEW.stage is distinct from 'Sold' then
    return NEW;
  end if;

  if TG_OP = 'UPDATE' and OLD.stage is not distinct from 'Sold' then
    return NEW;
  end if;

  select s.value into flag_val
  from public.ops_app_settings s
  where s.key = 'SOLD_CREATE_ENABLED'
  limit 1;

  if lower(coalesce(flag_val, 'false')) not in ('true', '1', 'yes', 'on') then
    return NEW;
  end if;

  if not exists (
    select 1
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'net' and p.proname = 'http_post'
  ) then
    raise warning 'sold-create: net.http_post missing — configure a Database Webhook on public.projects';
    return NEW;
  end if;

  fn_url := coalesce(
    nullif(current_setting('app.sold_create_url', true), ''),
    'https://dwkvdelvnmvniewakzoq.supabase.co/functions/v1/sold-create'
  );
  auth_header := nullif(current_setting('app.sold_create_auth', true), '');

  headers := jsonb_build_object('Content-Type', 'application/json');
  if auth_header is not null then
    headers := headers || jsonb_build_object('Authorization', auth_header);
  end if;

  perform net.http_post(
    url := fn_url,
    headers := headers,
    body := jsonb_build_object(
      'type', TG_OP,
      'table', TG_TABLE_NAME,
      'schema', TG_TABLE_SCHEMA,
      'record', to_jsonb(NEW),
      'old_record', case when TG_OP = 'UPDATE' then to_jsonb(OLD) else null end
    )
  );

  return NEW;
end;
$$;

drop trigger if exists trg_projects_sold_create on public.projects;
create trigger trg_projects_sold_create
  after insert or update of stage on public.projects
  for each row
  execute function public.trg_projects_sold_create();

comment on function public.trg_projects_sold_create() is
  'When projects.stage becomes Sold and SOLD_CREATE_ENABLED is true, POST to sold-create Edge Function. No row inserts here.';
