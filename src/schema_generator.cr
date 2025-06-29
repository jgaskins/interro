require "db"
require "pg"
require "file_utils"

module Interro
  class SchemaGenerator
    @db : DB::Database

    def initialize(@db = CONFIG.write_db)
    end

    def extract_schema(to io : IO) : Nil
      io << "-- Generated Schema\n\n"

      # Extract extensions
      extensions = query_extensions
      extensions.each do |ext|
        io << extension_definition(ext)
        io << "\n"
      end
      io << "\n" if !extensions.empty?

      # Extract and group tables by name
      tables = query_tables.group_by(&.table_name)
      tables.each do |table_name, columns|
        io << table_definition(columns)
        io << "\n\n"
      end

      migrations = query_migrations
      if migrations.any?
        io.puts "INSERT INTO schema_migrations (name, added_at)"
        io.puts "VALUES"
        migrations.each_with_index 1 do |migration, index|
          io << "  ('#{migration.name}', '#{migration.added_at}')"
          if index < migrations.size
            io.puts ','
          else
            io.puts ';'
          end
        end
        io.puts
      end

      # Extract indexes
      indexes = query_indexes
      indexes.each do |index|
        io << index_definition(index)
        io << "\n"
      end

      # Extract foreign keys
      foreign_keys = query_foreign_keys
      foreign_keys.each do |fk|
        io << foreign_key_definition(fk)
        io << "\n"
      end
    end

    def save_schema(path : String = "db/schema.sql")
      File.open path, "w" do |file|
        extract_schema file
      end
    end

    private def query_extensions
      @db.query_all <<-SQL, as: Extension
        SELECT
          extname,
          extversion
        FROM pg_extension
        WHERE extname != 'plpgsql'
        ORDER BY extname
      SQL
    end

    struct Extension
      include DB::Serializable

      getter extname : String
      getter extversion : String
    end

    private def query_tables
      @db.query_all <<-SQL, as: ColumnMetadata
        SELECT
          tables.table_name,
          column_name,
          udt_name data_type,
          column_default,
          is_nullable = 'YES' AS nullable,
          character_maximum_length
        FROM information_schema.columns
        JOIN information_schema.tables
          USING (table_name)
        WHERE tables.table_schema = 'public'
        AND tables.table_type = 'BASE TABLE'
        ORDER BY table_name, ordinal_position
      SQL
    end

    struct ColumnMetadata
      include DB::Serializable

      getter table_name : String
      getter column_name : String
      getter data_type : String
      getter column_default : String?
      getter? nullable : Bool
      getter character_maximum_length : Int32?
    end

    private def query_migrations
      @db.query_all <<-SQL, as: SchemaMigration
        SELECT name, added_at::text
        FROM schema_migrations
        ORDER BY added_at
        SQL
    end

    struct SchemaMigration
      include DB::Serializable

      getter name : String
      getter added_at : String
    end

    private def query_indexes
      @db.query_all <<-SQL, as: Index
        SELECT
          t.relname AS table_name,
          i.relname AS index_name,
          array_to_string(array_agg(a.attname ORDER BY a.attnum), ', ') AS column_name,
          ix.indisunique AS unique
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON ix.indexrelid = i.oid
        JOIN pg_attribute a ON t.oid = a.attrelid
        WHERE a.attnum = ANY(ix.indkey)
          AND t.relkind = 'r'
          AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        GROUP BY t.relname, i.relname, ix.indisunique
        ORDER BY t.relname, i.relname
      SQL
    end

    struct Index
      include DB::Serializable

      getter table_name : String
      getter index_name : String
      getter column_name : String
      getter? unique : Bool
    end

    private def query_foreign_keys
      @db.query_all <<-SQL, as: ForeignKey
        SELECT
          tc.table_name,
          kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
      SQL
    end

    struct ForeignKey
      include DB::Serializable

      getter table_name : String
      getter column_name : String
      getter foreign_table_name : String
      getter foreign_column_name : String
    end

    private def extension_definition(extension : Extension) : String
      "CREATE EXTENSION IF NOT EXISTS #{extension.extname};"
    end

    private def table_definition(columns : Array(ColumnMetadata)) : String
      current_table = columns.first.table_name
      String.build do |str|
        str << "CREATE TABLE IF NOT EXISTS #{current_table} (\n"
        column_definitions = columns.map { |col| "  #{column_definition(col)}" }.join(",\n")
        str << column_definitions
        str << "\n);"
      end
    end

    private def column_definition(column) : String
      definition = "#{column.column_name} #{column.data_type}"

      if column.character_maximum_length
        definition += "(#{column.character_maximum_length})"
      end

      definition += " NOT NULL" unless column.nullable?

      if column.column_default
        definition += " DEFAULT #{column.column_default}"
      end

      definition
    end

    private def index_definition(index) : String
      unique = index.unique? ? "UNIQUE " : ""
      "CREATE #{unique}INDEX IF NOT EXISTS #{index.index_name} ON #{index.table_name} (#{index.column_name});"
    end

    private def foreign_key_definition(fk) : String
      "ALTER TABLE #{fk.table_name} ADD FOREIGN KEY (#{fk.column_name}) " \
      "REFERENCES #{fk.foreign_table_name} (#{fk.foreign_column_name});"
    end
  end
end
