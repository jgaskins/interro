require "log"
require "benchmark"

module Interro
  class Migration
    QueryLog = ::Log.for("sql", level: :info)

    getter name, added_at
    setter up = ""
    setter down = ""

    def initialize(@name : String, @added_at : Time)
    end

    def initialize(@name : String, @added_at : Time, @up : String, @down : String)
    end

    def execute(sql : String, *args, **kwargs)
      QueryLog.info { sql }
      measurement = Benchmark.measure do
        CONFIG.write_db.using_connection(&.as(PG::Connection).exec_all sql, *args, **kwargs)
      end
      QueryLog.info { "-- #{measurement.real.humanize}s (#{measurement.total.humanize}s CPU)" }
    end

    def up(env = ENV)
      execute up_sql(env)
    end

    def down(env = ENV)
      execute down_sql(env)
    end

    def up_sql(env)
      @up.gsub(/\$(\w+)/) { env[$1] }
    end

    def down_sql(env)
      @down.gsub(/\$(\w+)/) { env[$1] }
    end
  end
end
