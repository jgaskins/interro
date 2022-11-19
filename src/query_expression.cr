require "./types" # for Interro::Value

module Interro
  struct QueryExpression
    getter lhs : String
    getter comparator : String
    getter rhs : String
    getter values : Array(Any)

    def initialize(@lhs, @comparator, @rhs, @values : Array(Any))
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
      io << @lhs << ' ' << @comparator << ' ' << @rhs
    end

    def to_sql
      String.build { |str| to_sql str }
    end
  end
end
