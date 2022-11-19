require "./spec_helper"

require "../src/query_expression"

module Interro
  describe QueryExpression do
    it "generates an AND conjunction of two expressions" do
      lhs = QueryExpression.new("foo", ">", "$1", [Any.new(69)])
      rhs = QueryExpression.new("bar", "=", "$2", [Any.new(420)])
      (lhs & rhs).to_sql.should eq "(foo > $1) AND (bar = $2)"
    end

    it "generates an OR conjunction of two expressions" do
      lhs = QueryExpression.new("foo", ">", "$1", [Any.new(69)])
      rhs = QueryExpression.new("bar", "=", "$2", [Any.new(420)])
      (lhs | rhs).to_sql.should eq "(foo > $1) OR (bar = $2)"
    end
  end
end
