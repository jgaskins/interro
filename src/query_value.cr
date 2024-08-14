require "./types"
require "./query_expression"

module Interro
  struct QueryValue
    getter value : String
    getter index : Int32

    def initialize(@value, @index)
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

    def not_in?(array : Enumerable(Value))
      not_in? array.map { |value| Any.new(value) }
    end

    def not_in?(array : Enumerable(Any))
      QueryExpression.new(value, "!=", "ALL($#{index})", [Any.new(array)])
    end

    {% for operator in %w[& | ^] %}
      # Bitwise operator
      def {{operator.id}}(other : Value)
        QueryExpression.new(value, {{operator}}, "$#{index}", [Any.new(other)])
      end
    {% end %}
  end
end
