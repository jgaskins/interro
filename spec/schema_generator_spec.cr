require "./spec_helper"
require "./config"
require "../src/schema_generator"

describe Interro::SchemaGenerator do
  # Ensure the migrations table exists and has a few records
  before_all do
    Interro::CONFIG.write_db.exec <<-SQL
      CREATE TABLE IF NOT EXISTS schema_migrations (
        name TEXT UNIQUE NOT NULL,
        added_at TIMESTAMPTZ UNIQUE NOT NULL
      )
    SQL

    # Insert some dummy migrations so they appear in the schema
    Interro::CONFIG.write_db.exec <<-SQL
      INSERT INTO schema_migrations (name, added_at)
      VALUES 
        ('2023_01_01_12_00_00_000000000-init', '2023-01-01 12:00:00Z'),
        ('2023_01_02_12_00_00_000000000-add_something', '2023-01-02 12:00:00Z')
      ON CONFLICT DO NOTHING;
    SQL
  end

  describe "#extract_schema" do
    it "writes the database schema into the given IO" do
      io = IO::Memory.new
      generator = Interro::SchemaGenerator.new(Interro::CONFIG.write_db)
      generator.extract_schema(io)
      schema = io.to_s

      # Verify Extensions
      schema.should contain("CREATE EXTENSION IF NOT EXISTS uuid-ossp;")

      # Verify Tables
      schema.should match(/CREATE TABLE IF NOT EXISTS users \(\n\s+id uuid NOT NULL DEFAULT gen_random_uuid\(\),\n\s+email text NOT NULL,\n\s+name text NOT NULL,\n\s+deactivated_at timestamp with time zone,\n\s+created_at timestamp with time zone NOT NULL DEFAULT now\(\),\n\s+updated_at timestamp with time zone NOT NULL DEFAULT now\(\)\n\);/m)
      schema.should match(/CREATE TABLE IF NOT EXISTS groups \(\n.*name text NOT NULL/m)
      schema.should match(/CREATE TABLE IF NOT EXISTS group_memberships \(/m)
      schema.should match(/CREATE TABLE IF NOT EXISTS tasks \(/m)
      schema.should match(/CREATE TABLE IF NOT EXISTS group_tasks \(/m)
      schema.should match(/CREATE TABLE IF NOT EXISTS notifications \(/m)

      # Verify Migrations
      schema.should contain("INSERT INTO schema_migrations (name, added_at)")
      schema.should contain("VALUES")
      schema.should contain("('2023_01_02_12_00_00_000000000-add_something', '2023-01-02 12:00:00')")
      schema.should contain("('2023_01_01_12_00_00_000000000-init', '2023-01-01 12:00:00')")

      # Verify Indexes
      schema.should contain("CREATE UNIQUE INDEX IF NOT EXISTS users_email_key ON users (email);")
      schema.should contain("CREATE INDEX IF NOT EXISTS index_users_on_created_at ON users (created_at);")
      schema.should contain("CREATE INDEX IF NOT EXISTS index_group_memberships_on_group_id ON group_memberships (group_id);")

      # Verify Foreign Keys
      schema.should contain("ALTER TABLE notifications ADD CONSTRAINT notifications_user_id_fkey FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE;")
    end

    it "handles tables whose names collide with tables in other schemas" do
      # information_schema.domains also exists
      Interro::CONFIG.write_db.exec "CREATE TABLE IF NOT EXISTS domains (name text NOT NULL)"

      begin
        io = IO::Memory.new
        generator = Interro::SchemaGenerator.new(Interro::CONFIG.write_db)
        generator.extract_schema(io)

        io.to_s.should match(/CREATE TABLE IF NOT EXISTS domains \(\n\s+name text NOT NULL\n\);/m)
      ensure
        Interro::CONFIG.write_db.exec "DROP TABLE IF EXISTS domains"
      end
    end
  end

  describe "#save_schema" do
    it "saves the schema to the given file path" do
      path = File.join(Dir.tempdir, "schema-#{UUID.random}.sql")
      generator = Interro::SchemaGenerator.new(Interro::CONFIG.write_db)
      generator.save_schema(path)

      begin
        content = File.read(path)
        content.should contain("CREATE TABLE IF NOT EXISTS users")
        content.should contain("ALTER TABLE notifications ADD CONSTRAINT notifications_user_id_fkey FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE;")
      ensure
        File.delete(path)
      end
    end
  end
end
