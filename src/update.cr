require "./conflict_handler/action"

module Interro
  struct Update(T)
    include ConflictHandler::Action

    getter params : T

    def initialize(set @params)
    end

    def to_sql(io, start_at initial_index) : Nil
      io << "UPDATE SET "
      {% if T <= Hash || T <= NamedTuple %}
        params.each_with_index 1 do |key, _, index|
          io << key.to_s << " = $" << initial_index + index
          if index < params.size
            io << ", "
          end
        end
      {% elsif T <= String %}
        io << params
      {% else %}
        {% raise "The `set` argument for `Interro::Update.new` must be a Hash, NamedTuple, or String. Got: #{T}" %}
      {% end %}
    end
  end
end
