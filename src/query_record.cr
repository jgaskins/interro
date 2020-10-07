require "./query_value"

module Interro
  struct QueryRecord
    def initialize(@relation : String, &@block : -> Int32)
    end

    macro method_missing(call)
      QueryValue.new("#{@relation}.{{call.id}}", @block.call)
    end
  end
end
