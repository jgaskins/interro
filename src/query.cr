require "db"

module Interro
  abstract struct Query
    def self.[](*args, **kwargs)
      new.call(*args, **kwargs)
    end

    def self.[]?(*args, **kwargs)
      new.call?(*args, **kwargs)
    end

    def self.call(*args, **kwargs)
      new.call(*args, **kwargs)
    end

    def [](*args, **kwargs)
      call(*args, **kwargs)
    end

    @read_db : ::DB::Database | ::DB::Connection
    @write_db : ::DB::Database | ::DB::Connection

    def self.[](transaction : ::DB::Transaction)
      new(read_db: transaction.connection, write_db: transaction.connection)
    end

    def self.new(query_builder : Interro::QueryBuilder)
      if txn = query_builder.transaction
        new(
          read_db: txn.connection,
          write_db: txn.connection,
        )
      else
        new
      end
    end

    def initialize(@read_db = CONFIG.read_db, @write_db = CONFIG.write_db)
    end

    private def read_all(query, *_args, args : Array? = nil, as type : T) forall T
      {% begin %}
        result = Array({{T.instance}}).new
        completed = false
        measurement = Benchmark.measure { result = @read_db.query_all(query, *_args, args: args, as: type).not_nil! }
        completed = true
        result
      {% end %}
    end

    # I don't know if we actually need this one. We'll probably always want to
    # check the case where it doesn't exist.
    private def read_one(query, *args, as type : T) forall T
      @read_db.query_one query, *args, as: type
    end

    private def read_scalar(query, *args)
      @read_db.scalar query, *args
    end

    private def read_each(query, *args, as type : T) : Nil forall T
      result_count = 0
      @read_db.query_each(query, *args) do |rs|
        result_count += 1
        {% if T < Tuple %}
          yield({
            {% for type in T %}
              rs.read({{type.instance}}),
            {% end %}
          })
        {% else %}
          yield T.new(rs)
        {% end %}
      end
    end

    private def read_one?(query, *args, as type)
      @read_db.query_one? query, *args, as: type
    end

    private def write_one(query, *args, as type)
      @write_db.query_one query, *args, as: type
    end

    private def write(query, *args)
      @write_db.exec(query, *args)
    end

    private def write(query, *, args : Array)
      @write_db.exec(query, args: args)
    end
  end
end
