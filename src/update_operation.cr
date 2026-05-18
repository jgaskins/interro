require "db"

require "./query_expression"

module Interro
  # :nodoc:
  struct UpdateOperation(T)
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(query, set values : NamedTuple, where : QueryExpression? = nil) forall U
      args = build_args(values, where)
      @queryable.query_all to_sql(query, where, values, returning: true), args: args, as: T
    end

    def call(query, set values : String, args : Array(Value) = [] of Value, where : QueryExpression? = nil)
      if where
        args = where.values + args
      end

      @queryable.query_all to_sql(query, where, values, returning: true), args: args, as: T
    end

    def call!(query, set values : NamedTuple, where : QueryExpression? = nil) : Int64
      args = build_args(values, where)
      @queryable.exec(to_sql(query, where, values, returning: false), args: args).rows_affected
    end

    def call!(query, set values : String, args : Array(Value) = [] of Value, where : QueryExpression? = nil) : Int64
      if where
        args = where.values + args
      end

      @queryable.exec(to_sql(query, where, values, returning: false), args: args).rows_affected
    end

    private def build_args(values : NamedTuple, where)
      if values.is_a? NamedTuple()
        args = [] of String
      else
        args = values.values.to_a
        if where
          args = where.values + args
        end
      end
      args
    end

    def to_sql(query, where, values, *, returning : Bool = true)
      table_name = query.sql_table_name

      sql = String.build do |str|
        str << "UPDATE " << table_name << ' '
        str << "SET "
        sqlize values, where, to: str

        if where
          str << " WHERE "
          where.to_sql str
        end

        if returning
          str << " RETURNING "
          query.select_columns str
        end
      end
    end

    private def sqlize(values : NamedTuple, where, to io) : Nil
      where_size = (where.try(&.values.size) || 0)
      last_index = values.size + where_size
      values.each_with_index(where_size + 1) do |key, value, index|
        key.to_s io
        io << " = $" << index
        if index < last_index
          io << ", "
        end
      end
    end

    private def sqlize(values : String, where, to io) : Nil
      start = where.try(&.values.size) || 0
      io << values.gsub /\$\d+/ do |match|
        "$#{match[1..].to_i + start}"
      end
    end
  end
end
