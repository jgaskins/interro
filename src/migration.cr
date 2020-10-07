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

    def execute(sql : String, *args, **kwargs)
      QueryLog.info { sql }
      measurement = Benchmark.measure do
        CONFIG.write_db.exec sql, *args, **kwargs
      end
      QueryLog.info { "-- #{measurement.real.humanize}s (#{measurement.total.humanize}s CPU)" }
    end

    def up
      execute @up
    end

    def down
      execute @down
    end
  end
end
