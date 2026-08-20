require "./query_value"

module Interro
  struct QueryRecord
    def initialize(@relation : String)
    end

    macro method_missing(call)
      QueryValue.new("#{@relation}.{{call.id}}")
    end
  end
end
