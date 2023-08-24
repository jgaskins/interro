require "./spec_helper"
require "./config"

require "../src/migration"

module Interro
  describe Migration do
    db = Interro::CONFIG.write_db

    it "can run migrations both up and down" do
      table_name = "interro_spec_#{Random::Secure.hex}"
      migration = Migration.new(
        name: "DoSomethingUseful",
        added_at: Time.utc,
        up: "CREATE TABLE #{table_name} (id int8 NOT NULL)",
        down: "DROP TABLE #{table_name}",
      )

      begin
        migration.up

        db.query_one("INSERT INTO #{table_name} VALUES (1) RETURNING id", as: Int64).should eq 1
      ensure
        migration.down
      end

      db.query_one?(<<-SQL, table_name, as: String).should eq nil
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE'
        AND table_name = $1
      SQL
    end

    it "can run migrations with multiple queries" do
      table_name = "interro_spec_#{Random::Secure.hex}"
      migration = Migration.new(
        name: "DoSomethingUseful",
        added_at: Time.utc,
        up: <<-SQL,
          CREATE TABLE #{table_name} (id int8 NOT NULL);

          ALTER TABLE #{table_name}
          ADD COLUMN name TEXT;

          INSERT INTO #{table_name} (id, name)
          VALUES (1, 'Jamie');

          ALTER TABLE #{table_name}
          ALTER COLUMN name SET NOT NULL;
        SQL
        down: "DROP TABLE #{table_name}",
      )

      begin
        migration.up

        not_null_columns = db.query_all(<<-SQL, table_name, as: String)
          SELECT column_name
          FROM information_schema.columns
          WHERE NOT is_nullable::boolean
          AND table_name = $1
          ORDER BY column_name
        SQL

        not_null_columns.should eq %w[id name]
      rescue ex
      ensure
        begin
          migration.down
        rescue ex2
          raise ex || ex2
        end
      end
    end

    it "interpolates ENV vars" do
      env = {
        "UP_VAR"   => "69",
        "DOWN_VAR" => "420",
      }

      migration = Migration.new(
        name: "Yep",
        added_at: Time.utc,
        up: "SELECT $UP_VAR",
        down: "SELECT $DOWN_VAR",
      )

      migration.up_sql(env).should eq "SELECT 69"
      migration.down_sql(env).should eq "SELECT 420"
    end
  end
end
