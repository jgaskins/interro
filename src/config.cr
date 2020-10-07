require "log"
require "db"

module Interro
  CONFIG = Config.new
  LOG    = ::Log.for("interro")

  def self.config
    yield CONFIG
    CONFIG
  end

  class Config
    property read_db : DB::Database = DB.open("postgres:///")
    property write_db : DB::Database = DB.open("postgres:///")
    property log = LOG

    def db=(db)
      self.read_db = db
      self.write_db = db
    end
  end
end
