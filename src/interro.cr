require "benchmark"
require "db"
require "pg"

require "./types"
require "./query"
require "./config"
require "./query_builder"

#
module Interro
  VERSION = "0.1.0"

  def self.transaction
    CONFIG.write_db.transaction do |txn|
      yield txn
    end
  end

  alias OrderBy = Hash(String, String)

  struct CreateOperation(T)
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(table_name, params) : T
      sql = String.build do |str|
        str << "INSERT INTO " << table_name << " ("
        params.each_with_index(1) do |key, value, index|
          key.to_s str
          str << ", " if index < params.size
        end
        str << ") VALUES ("
        params.each_with_index(1) do |key, value, index|
          str << '$' << index
          str << ", " if index < params.size
        end
        str << ") RETURNING *"
      end

      @queryable.query_one sql, *params.values, as: T
    end
  end

  struct UpdateOperation(T)
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(table_name, set values : NamedTuple, where : QueryExpression? = nil)
      sql = String.build do |str|
        str << "UPDATE " << table_name << ' '
        str << "SET "
        values.each_with_index((where.try(&.values.size) || 0) + 1) do |key, value, index|
          key.to_s str
          str << " = $" << index
          if index < values.size
            str << ", "
          end
        end

        if where
          str << " WHERE "
          where.to_sql str
        end

        str << " RETURNING *"
      end

      args = values.values.to_a
      if where
        args = where.values + args
      end

      @queryable.query_all sql, args: args, as: T
    end
  end

  struct DeleteOperation
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(table_name : String, where : QueryExpression) : Nil
      sql = String.build do |str|
        str << "DELETE FROM " << table_name
        str << " WHERE "
        where.to_sql str
      end

      @queryable.exec sql, args: where.values
    end

    def call(table_name : String, where : Nil)
      raise UnscopedDeleteOperation.new("Invoked a DeleteOperation with no WHERE clause. If this is intentional, use a TruncateOperation instead")
    end

    class UnscopedDeleteOperation < Exception
    end
  end

  class Exception < ::Exception
  end

  class NotFound < Exception
  end
end
