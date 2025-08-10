module Interro
  struct ConflictHandler(UpdateHandler)
    module Action
      abstract def to_sql(io, start_at initial_index) : Nil
      abstract def params
    end
  end
end
