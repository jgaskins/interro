require "benchmark"
require "db"
require "pg"

require "./types"
require "./query"
require "./config"
require "./query_builder"
require "./model"

#
module Interro
  VERSION = "0.1.8"

  class Error < ::Exception
  end

  class UnexpectedEmptyResultSet < Error
  end

  def self.transaction(& : DB::Transaction -> T) forall T
    result = uninitialized T
    completed = false
    CONFIG.write_db.transaction do |txn|
      result = yield txn
      completed = true
    end

    if completed
      result
    else
      raise "Transaction block is incomplete - unexpected rollback?"
    end
  end

  alias OrderBy = Hash(String, String)

  struct CreateOperation(T)
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(query : QueryBuilder(T), params) : T
      table_name = query.sql_table_name
      sql = String.build do |str|
        str << "INSERT INTO " << table_name << " ("
        params.each_with_index(1) do |key, value, index|
          key.to_s.inspect str
          str << ", " if index < params.size
        end
        str << ") VALUES ("
        params.each_with_index(1) do |key, value, index|
          str << '$' << index
          str << ", " if index < params.size
        end
        str << ") RETURNING "
        query.select_columns str
      end

      @queryable.query_one sql, *params.values, as: T
    end
  end

  struct UpdateOperation(T)
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(query, set values : NamedTuple, where : QueryExpression? = nil)
      args = values.values.to_a
      if where
        args = where.values + args
      end

      @queryable.query_all to_sql(query, where, values), args: args, as: T
    end

    def call(query, set values : String, args : Array(Value) = [] of Value, where : QueryExpression? = nil)
      if where
        args += where.values
      end

      @queryable.query_all to_sql(query, where, values), args: args, as: T
    end

    def to_sql(query, where, values)
      table_name = query.sql_table_name

      sql = String.build do |str|
        str << "UPDATE " << table_name << ' '
        str << "SET "
        sqlize values, where, to: str

        if where
          str << " WHERE "
          where.to_sql str
        end

        str << " RETURNING "
        query.select_columns str
      end
    end

    private def sqlize(values : NamedTuple, where, to io) : Nil
      values.each_with_index((where.try(&.values.size) || 0) + 1) do |key, value, index|
        key.to_s io
        io << " = $" << index
        if index <= values.size
          io << ", "
        end
      end
    end

    private def sqlize(values : String, where, to io) : Nil
      io << values
    end
  end

  struct DeleteOperation
    def initialize(@queryable : DB::Database | DB::Connection)
    end

    def call(table_name : String, where : QueryExpression)
      sql = String.build do |str|
        str << "DELETE FROM " << table_name
        str << " WHERE "
        where.to_sql str
      end

      @queryable
        .exec(sql, args: where.values)
        .rows_affected
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

# ############ MONKEYPATCHES #######################

# :nodoc:
module DB
  annotation Field
  end

  # :nodoc:
  module Serializable
    # Adding support for deserializing multiple entities in a single query. This
    # is almost identical to the canonical DB::Serializable, but we had to copy
    # the entire thing because it's all inside a single macro.
    macro included
      include ::DB::Mappable

      # Define a `new` and `from_rs` directly in the type, like JSON::Serializable
      # For proper overload resolution

      def self.new(rs : ::DB::ResultSet)
        instance = allocate
        instance.initialize(__set_for_db_serializable: rs)
        GC.add_finalizer(instance) if instance.responds_to?(:finalize)
        instance
      end

      def self.from_rs(rs : ::DB::ResultSet)
        objs = Array(self).new
        rs.each do
          objs << self.new(rs)
        end
        objs
      ensure
        rs.close
      end
    end

    def initialize(*, __set_for_db_serializable rs : ::DB::ResultSet)
      {% begin %}
        {% properties = {} of Nil => Nil %}
        {% for ivar in @type.instance_vars %}
          {% ann = ivar.annotation(::DB::Field) %}
          {% unless ann && ann[:ignore] %}
            {%
              properties[ivar.id] = {
                type:      ivar.type,
                key:       ((ann && ann[:key]) || ivar).id.stringify,
                default:   ivar.default_value,
                nilable:   ivar.type.nilable?,
                converter: ann && ann[:converter],
              }
            %}
          {% end %}
        {% end %}

        {% for name, value in properties %}
          %var{name} = nil
          %found{name} = false
        {% end %}

        rs.each_column_from_last do |col_name|
          case col_name
            {% for name, value in properties %}
              when {{value[:key]}}
                %found{name} = true
                begin
                  %var{name} =
                    {% if value[:converter] %}
                      {{value[:converter]}}.from_rs(rs)
                    {% elsif value[:nilable] || value[:default] != nil %}
                      rs.read(::Union({{value[:type]}} | Nil))
                    {% else %}
                      rs.read({{value[:type]}})
                    {% end %}
                rescue exc
                  ::raise ::DB::MappingException.new(exc.message, self.class.to_s, {{name.stringify}}, cause: exc)
                end

                {% for name_for_check, __value in properties %}
                  next unless %found{name_for_check}
                {% end %}
                break
            {% end %}
          else
            rs.read # Advance set, but discard result
            on_unknown_db_column(col_name)
          end
        end

        {% for key, value in properties %}
          {% unless value[:nilable] || value[:default] != nil %}
            if %var{key}.nil? && !%found{key}
              ::raise ::DB::MappingException.new("Missing column {{value[:key].id}}", self.class.to_s, {{key.stringify}})
            end
          {% end %}
        {% end %}

        {% for key, value in properties %}
          {% if value[:nilable] %}
            {% if value[:default] != nil %}
              @{{key}} = %found{key} ? %var{key} : {{value[:default]}}
            {% else %}
              @{{key}} = %var{key}
            {% end %}
          {% elsif value[:default] != nil %}
            if %var{key}.nil?
              @{{key}} = {{value[:default]}}
            end
          {% else %}
            @{{key}} = %var{key}.as({{value[:type]}})
          {% end %}
        {% end %}
      {% end %}
    end
  end
end

# :nodoc:
class PG::ResultSet
  # We have to monkeypatch this to support the modification in DB::Serializable
  # above
  def each_column_from_last
    (@column_index...column_count).each do |i|
      yield column_name(i)
    end
  end
end
