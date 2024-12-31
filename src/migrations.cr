require "option_parser"

require "./interro"
require "./migration"

module Interro
  module Migrations
    MigrationLog = ::Log.for("migrations", level: :info)

    TIME_FORMAT = Time::Format::ISO_8601_DATE_TIME

    NAME_MAP       = {} of String => Migration
    ALL_MIGRATIONS = Dir["db/migrations/**/*.sql"].map do |path|
      filename = File.basename File.dirname(path)
      timestamp, name = filename.split "-"
      time = Time.parse_utc(timestamp, "%Y_%m_%d_%H_%M_%S_%9N")
      migration = NAME_MAP[name] ||= Migration.new(name, time)

      if path.ends_with? "up.sql"
        migration.up = File.read(path)
      elsif path.ends_with? "down.sql"
        migration.down = File.read(path)
      else
        raise "Migration files must be named up.sql or down.sql"
      end

      SchemaMigration.new(name, time)
    end.uniq

    ::Log.setup do |c|
      c.bind "migrations", :info, Log::IOBackend.new(
        formatter: Log::Formatter.new { |entry, io|
          io << "\e[1m"
          io << entry.message
          io << "\e[0m"
        },
      )
      c.bind "sql", :info, Log::IOBackend.new(
        formatter: Log::Formatter.new { |entry, io|
          message = entry.message.rstrip
          indent = message
            .each_line
            .min_by { |line| line.index(/\S/) || 0 }
            .index(/\S/)

          message.each_line(chomp: false) do |line|
            io << line.sub(/\A\s{#{indent}}/, "")
          end
        },
      )
    end

    def self.call(args : Array(String))
      operation = ->{}

      OptionParser.parse args.dup do |parser|
        selected_migration = nil
        parser.on "-n NAME", "--name=NAME", "Specify a migration class name to perform the operation on (not the filename)" do |name|
          selected_migration = ALL_MIGRATIONS.find { |m| m.name == name }
        end

        parser.on "rollback", "Rollback the specified migration (default: latest)" do
          operation = ->{ rollback selected_migration }
        end

        parser.on "redo", "Redo (rollback+run) the specified migration (default: latest)" do
          operation = ->{ redo selected_migration }
        end

        parser.on "run", "Run the specified migration (default: all incomplete)" do
          operation = ->{ run selected_migration }
        end

        parser.on "g", "Generate a migration with the specified name" do
          name = ""
          parser.unknown_args { |args| name = args.first }
          operation = ->{ SQLGenerator.call name }
        end
      end

      operation.call
    end

    def self.run(migration : Nil)
      ensure_migration_table_exists
      all_migrations = ALL_MIGRATIONS.sort_by(&.added_at)

      (all_migrations - completed_migrations).each do |migration|
        run migration
      end
    end

    def self.run(migration : SchemaMigration)
      MigrationLog.info { "Running #{migration.name}" }
      measurement = Benchmark.measure do
        NAME_MAP[migration.name].up
        CONFIG.write_db.exec <<-SQL, migration.name, migration.added_at
          INSERT INTO schema_migrations (name, added_at)
          VALUES ($1, $2)
        SQL
      end
      MigrationLog.info { "#{migration.name}: #{measurement.real.humanize}s (#{measurement.total.humanize}s CPU)" }
    end

    def self.rollback(migration : Nil)
      if migration = completed_migrations.last?
        rollback migration
      else
        MigrationLog.warn { "No migration to roll back" }
        nil
      end
    end

    def self.rollback(migration : SchemaMigration)
      ensure_migration_table_exists
      MigrationLog.info { "Rolling back #{migration.name}" }
      measurement = Benchmark.measure do
        NAME_MAP[migration.name].down
        CONFIG.write_db.exec <<-SQL, migration.name, migration.added_at
          DELETE FROM schema_migrations
          WHERE name = $1
          AND added_at = $2
        SQL
      end
      MigrationLog.info { "#{migration.name}: #{measurement.real.humanize}s" }
      migration
    end

    def self.redo(migration : Nil)
      if migration = rollback(nil)
        run migration
      end
    end

    def self.redo(migration : SchemaMigration)
      rollback migration
      run migration
    end

    def self.completed_migrations
      CONFIG.write_db.query_all <<-SQL, as: SchemaMigration
        SELECT name, added_at
        FROM schema_migrations
        ORDER BY added_at
      SQL
    end

    def self.ensure_migration_table_exists
      CONFIG.write_db.exec <<-SQL
        CREATE TABLE IF NOT EXISTS schema_migrations (
          name TEXT UNIQUE NOT NULL,
          added_at TIMESTAMPTZ UNIQUE NOT NULL
        )
      SQL
    end

    class SQLGenerator
      def self.call(*args, **kwargs)
        new.call(*args, **kwargs)
      end

      def call(name : String)
        puts "Generating #{name}..."
        dir = "db/migrations/#{Time.utc.to_s("%Y_%m_%d_%H_%M_%S_%9N")}-#{name}"
        Dir.mkdir_p dir
        File.write "#{dir}/up.sql", <<-SQL
        CREATE TABLE foo(
          id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
          -- other attributes can go here
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )

        -- or

        CREATE INDEX CONCURRENTLY index_foo_on_bar
        ON foo (bar)
        SQL

        File.write "#{dir}/down.sql", "DROP TABLE foo"
      end
    end

    struct SchemaMigration
      include DB::Serializable

      property name : String
      property added_at : Time

      def initialize(@name, @added_at)
      end
    end
  end
end

require "dotenv"
Dotenv.load?

Interro.config do |c|
  c.db = DB.open ENV["DATABASE_URL"]
end

Interro::Migrations.call ARGV
