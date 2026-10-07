require "./types"
require "./query_expression"

module Interro
  struct QueryValue
    getter value : String

    def initialize(@value)
    end

    def ==(other : Value)
      QueryExpression.new("#{value} = ", Any.new(other))
    end

    def ==(other : Nil)
      QueryExpression.new("#{value} IS NULL")
    end

    def <=(other : Value)
      QueryExpression.new("#{value} <= ", Any.new(other))
    end

    def >=(other : Value)
      QueryExpression.new("#{value} >= ", Any.new(other))
    end

    def <(other : Value)
      QueryExpression.new("#{value} < ", Any.new(other))
    end

    def >(other : Value)
      QueryExpression.new("#{value} > ", Any.new(other))
    end

    def !=(other : Value)
      QueryExpression.new("#{value} != ", Any.new(other))
    end

    def !=(other : Nil)
      QueryExpression.new("#{value} IS NOT NULL")
    end

    def in?(array : Enumerable(Value))
      in? array.map { |value| Any.new(value) }
    end

    def in?(array : Enumerable(Any))
      QueryExpression.new("#{value} = ANY(", Any.new(array), ")")
    end

    def not_in?(array : Enumerable(Value))
      not_in? array.map { |value| Any.new(value) }
    end

    def not_in?(array : Enumerable(Any))
      QueryExpression.new("#{value} != ALL(", Any.new(array), ")")
    end

    {% for operator in %w[& | ^] %}
      # Bitwise operator
      def {{operator.id}}(other : Value)
        QueryExpression.new("#{value} {{operator.id}} ", Any.new(other))
      end
    {% end %}
  end
end
