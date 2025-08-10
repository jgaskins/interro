require "./conflict_handler/action"

module Interro
  struct Update(T)
    include ConflictHandler::Action

    getter params : T

    def initialize(set @params)
    end

    def to_sql(io, start_at initial_index) : Nil
      io << "UPDATE SET "
      params.each_with_index 1 do |key, _, index|
        io << key.to_s << " = $" << initial_index + index
        if index < params.size
          io << ", "
        end
      end
    end
  end
end
