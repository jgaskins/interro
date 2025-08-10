require "db"
require "./conflict_handler"
require "./types"

module Interro
  # :nodoc:
  struct CreateManyOperation(T)
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(query : QueryBuilder(T), params, on_conflict conflict_handler : ConflictHandler? = nil) : T
      table_name = query.sql_table_name
      args = params
        .values
        .map { |value| Interro::Any.new(value) }
        .to_a
      sql = generate_query query.sql_table_name, params, args,
        on_conflict: conflict_handler,
        returning: ->(io : IO) { query.select_columns io }

      @queryable.query_one sql, args: args, as: T
    end

    def call!(query : QueryBuilder(T), params : Array(NamedTuple), on_conflict conflict_handler : ConflictHandler? = nil) : Int32
      table_name = query.sql_table_name
      args = params
        .flat_map(&.values.to_a)
        .map { |value| Interro::Any.new(value) }
        .to_a
      sql = generate_query query.sql_table_name, params, args,
        on_conflict: conflict_handler

      @queryable.exec(sql, args: args)
        .rows_affected
        # Postgres returns an Int64, but this will always be an Int32 because
        # Crystal arrays can only hold Int32::MAX elements.
        .to_i32
    end

    protected def generate_query(
      table_name : String,
      params : Array(NamedTuple),
      args,
      on_conflict conflict_handler : ConflictHandler?,
    )
      String.build do |str|
        str << "INSERT INTO " << table_name << " ("
        params.first.each_with_index(1) do |key, value, index|
          key.to_s.inspect str
          str << ", " if index < params.first.size
        end
        str << ") VALUES "
        params.each_with_index do |param, param_index|
          str << '('
          param.each_with_index(1) do |_key, _value, record_index|
            arg_index = param_index * param.size + record_index
            str << '$' << arg_index
            str << ", " if record_index < param.size
          end
          str << ')'
          if param_index < params.size - 1
            str << ','
          end
          str << ' '
        end
        if conflict_handler
          if (action = conflict_handler.action) && (handler_params = action.params)
            start = params.size
            handler_params.each_value do |value|
              args << Interro::Any.new(value)
            end
          end
          conflict_handler.to_sql str, start_at: start || 1
        end
      end
    end
  end
end
