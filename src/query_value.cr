require "./types"
require "./query_expression"

module Interro
  struct QueryValue
    getter value : String
    getter index : Int32

    def initialize(@value, @index)
    end

    def ==(other : Value)
      QueryExpression.new(value, "!=", "$#{index}", [Any.new(other)])
    end

    def ==(other : Value)
      QueryExpression.new(value, "=", "$#{index}", [Any.new(other)])
    end

    def ==(other : Nil)
      QueryExpression.new(value, "IS", "NULL", [] of Any)
    end

    def <=(other : Value)
      QueryExpression.new(value, "<=", "$#{index}", [Any.new(other)])
    end

    def >=(other : Value)
      QueryExpression.new(value, ">=", "$#{index}", [Any.new(other)])
    end

    def <(other : Value)
      QueryExpression.new(value, "<", "$#{index}", [Any.new(other)])
    end

    def >(other : Value)
      QueryExpression.new(value, ">", "$#{index}", [Any.new(other)])
    end

    def !=(other : Value)
      QueryExpression.new(value, "!=", "$#{index}", [Any.new(other)])
    end

    def !=(other : Nil)
      QueryExpression.new(value, "IS NOT", "NULL", [] of Any)
    end

    def in?(array : Enumerable(Value))
      in? array.map { |value| Any.new(value) }
    end

    def in?(array : Enumerable(Any))
      QueryExpression.new(value, "=", "ANY($#{index})", [Any.new(array)])
    end
  end
end
