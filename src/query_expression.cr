require "./types" # for Interro::Value

module Interro
  struct QueryExpression
    getter expression : String
    getter values : Array(Any)

    def self.new(lhs, comparator, rhs, values : Array(Any))
      new("#{lhs} #{comparator} #{rhs}", values)
    end

    def initialize(@expression, @values)
    end

    def &(other : self) : self
      values = @values + other.values
      self.class.new("(#{to_sql})", "AND", "(#{other.to_sql})", values)
    end

    def |(other : self) : self
      values = @values + other.values
      self.class.new("(#{to_sql})", "OR", "(#{other.to_sql})", values)
    end

    def to_sql(io)
      io << expression
    end

    def to_sql
      expression
    end
  end
end
