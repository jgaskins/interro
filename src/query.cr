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

    def initialize(@read_db = CONFIG.read_db, @write_db = CONFIG.write_db, @log = LOG)
    end

    private def read_all(query, *_args, args : Array? = nil, as type : T) forall T
      {% begin %}
        begin
          result = Array({{T.instance}}).new
          completed = false
          measurement = Benchmark.measure { result = @read_db.query_all(query, *_args, args: args, as: type).not_nil! }
          completed = true
          result
        ensure
          result_count = completed ? "#{result.not_nil!.size} results" : "did not finish"
          measurement ||= Benchmark.measure {}
          @log.debug { "[read] #{self.class.name} - #{query.gsub(/\s+/, " ").strip} - #{args.inspect} - #{result_count} - #{measurement.real.humanize}s (#{measurement.total.humanize}s CPU)" }
        end
      {% end %}
    end

    # I don't know if we actually need this one. We'll probably always want to
    # check the case where it doesn't exist.
    private def read_one(query, *args, as type : T) forall T
      {% begin %}
        begin
          result = uninitialized {{T.instance}}
          completed = false
          measurement = Benchmark.measure { result = @read_db.query_one query, *args, as: type }
          completed = true
          result
        ensure
          result_count = completed ? "1 result" : "did not finish"
          measurement ||= Benchmark.measure {}
          @log.debug { "[read] #{self.class.name} - #{query.gsub(/\s+/, " ").strip} - #{args.inspect} - #{result_count} - #{measurement.real.humanize}s (#{measurement.total.humanize}s CPU)" }
        end
      {% end %}
    end

    private def read_scalar(query, *args)
      result = 0
      completed = false
      measurement = Benchmark.measure { result = @read_db.scalar query, *args }
      completed = true
      result
    ensure
      result_count = completed ? "1 result" : "did not finish"
      measurement ||= Benchmark.measure { }
      @log.debug { "[read] #{self.class.name} - #{query.gsub(/\s+/, " ").strip} - #{args.inspect} - #{result_count} - #{measurement.real.humanize}s (#{measurement.total.humanize}s CPU)" }
    end

    private def read_one?(query, *args, as type)
      begin
        result = nil
        completed = false
        measurement = Benchmark.measure { result = @read_db.query_one? query, *args, as: type }
        completed = true
        result
      ensure
        result_count = if result
                         "1 result"
                       elsif completed
                         "0 results"
                       else
                         "did not finish"
                       end
        measurement ||= Benchmark.measure { }
        @log.debug { "[read] #{self.class.name} - #{query.gsub(/\s+/, " ").strip} - #{args.inspect} - #{result_count} - #{measurement.real.humanize}s (#{measurement.total.humanize}s CPU)" }
      end
    end

    private def write_one(query, *args, as type)
      @write_db.query_one query, *args, as: type
    end

    private def write(query, *args)
      @write_db.exec(query, *args)
    end
  end
end
