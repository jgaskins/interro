require "./types"
require "./query_expression"

module Interro
  struct QueryValue
    getter value : String
    getter index : Int32

    def initialize(@value, @index)
    end

    def ==(other : Value)
      QueryExpression.new(value, "!=", "$#{index}", [other.as(Value)])
    end

    def ==(other : Value)
      QueryExpression.new(value, "=", "$#{index}", [other.as(Value)])
    end

    def ==(other : Nil)
      QueryExpression.new(value, "IS", "NULL", [] of Value)
    end

    def <=(other : Value)
      QueryExpression.new(value, "<=", "$#{index}", [other.as(Value)])
    end

    def >=(other : Value)
      QueryExpression.new(value, ">=", "$#{index}", [other.as(Value)])
    end

    def <(other : Value)
      QueryExpression.new(value, "<", "$#{index}", [other.as(Value)])
    end

    def >(other : Value)
      QueryExpression.new(value, ">", "$#{index}", [other.as(Value)])
    end

    def !=(other : Value)
      QueryExpression.new(value, "!=", "$#{index}", [other.as(Value)])
    end

    def !=(other : Nil)
      QueryExpression.new(value, "IS NOT", "NULL", [] of Value)
    end

    def in?(array : Array(Value))
      # Recursive type aliases with data structures are a little funky to work with
      values = [array.map(&.as(Primitive)).as(Value)]

      QueryExpression.new(value, "= ANY(", "$#{index})", values)
    end
  end
end
