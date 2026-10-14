-- Change order archive flag.
-- Additive only: existing rows stay active (archived_at stays null).
-- Not applied by this change: archived_at and archived_by already exist on public.change_orders.
alter table public.change_orders
  add column if not exists archived_at timestamptz null,
  add column if not exists archived_by text null;

create index if not exists change_orders_active_idx
  on public.change_orders (project_id) where archived_at is null;
