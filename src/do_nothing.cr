require "./conflict_handler/action"

module Interro
  struct DoNothing
    include ConflictHandler::Action

    def params
    end

    def to_sql(io, start_at) : Nil
      io << "NOTHING"
    end
  end
end
