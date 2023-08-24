require "../src/interro"

pg = DB.open(ENV.fetch("DATABASE_URL", "postgres:///"))
pg.exec %{CREATE EXTENSION IF NOT EXISTS "uuid-ossp"}
pg.exec "DROP TABLE IF EXISTS users"
pg.exec <<-SQL
  CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    deactivated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL
pg.exec "CREATE INDEX IF NOT EXISTS index_users_on_email ON users (email)"
pg.exec "CREATE INDEX IF NOT EXISTS index_users_on_created_at ON users (created_at)"

pg.exec "DROP TABLE IF EXISTS groups"
pg.exec <<-SQL
  CREATE TABLE groups (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    member_count INT8 NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL

pg.exec "DROP TABLE IF EXISTS group_memberships"
pg.exec <<-SQL
  CREATE TABLE group_memberships (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    group_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL
pg.exec "CREATE INDEX IF NOT EXISTS index_group_memberships_on_group_id ON group_memberships (group_id)"
pg.exec "CREATE INDEX IF NOT EXISTS index_group_memberships_on_user_id ON group_memberships (user_id)"
pg.exec "CREATE INDEX IF NOT EXISTS index_group_memberships_on_created_at ON group_memberships (created_at)"

pg.exec "DROP TABLE IF EXISTS tasks"
pg.exec <<-SQL
  CREATE TABLE tasks (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL

pg.exec "DROP TABLE IF EXISTS group_tasks"
pg.exec <<-SQL
  CREATE TABLE group_tasks (
    id UUID PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    group_id UUID NOT NULL,
    task_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
SQL

Interro.config do |c|
  c.db = pg
  # equivalent to:
  #   c.read_db = pg
  #   c.write_db = pg

  # c.log.level = Log::Severity::Debug
end
