require "./spec_helper"

require "../src/query_expression"

module Interro
  describe QueryExpression do
    it "generates an AND conjunction of two expressions" do
      lhs = QueryExpression.parse("foo > $1", [Any.new(69)])
      rhs = QueryExpression.parse("bar = $1", [Any.new(420)])
      (lhs & rhs).to_sql.should eq "(foo > $1) AND (bar = $2)"
    end

    it "generates an OR conjunction of two expressions" do
      lhs = QueryExpression.parse("foo > $1", [Any.new(69)])
      rhs = QueryExpression.parse("bar = $1", [Any.new(420)])
      (lhs | rhs).to_sql.should eq "(foo > $1) OR (bar = $2)"
    end

    describe ".parse" do
      it "binds a repeated placeholder once per reference" do
        expression = QueryExpression.parse("a = $1 OR b = $1", [Any.new(1)])

        expression.to_sql.should eq "a = $1 OR b = $2"
        expression.values.should eq [Any.new(1), Any.new(1)]
      end

      it "binds values in reference order when placeholders appear out of order" do
        expression = QueryExpression.parse("b = $2 AND a = $1", [Any.new(1), Any.new(2)])

        expression.to_sql.should eq "b = $1 AND a = $2"
        expression.values.should eq [Any.new(2), Any.new(1)]
      end

      it "raises when a placeholder references a missing value" do
        expect_raises ArgumentError, "references $2" do
          QueryExpression.parse("a = $2", [Any.new(1)])
        end
      end

      it "rejects $0" do
        expect_raises ArgumentError, "references $0" do
          QueryExpression.parse("a = $0", [Any.new(1)])
        end
      end

      it "leaves $n inside string literals alone, including '' escapes" do
        expression = QueryExpression.parse("note = 'it''s $1' AND id = $1", [Any.new(1)])

        expression.to_sql.should eq "note = 'it''s $1' AND id = $1"
        expression.values.should eq [Any.new(1)]
      end
    end

    describe "#to_sql" do
      it "continues numbering across fragments rendered into one args array" do
        first = QueryExpression.parse("a = $1", [Any.new(1)])
        second = QueryExpression.parse("b = $1", [Any.new(2)])
        args = [] of Any

        sql = String.build do |str|
          first.to_sql str, args
          str << " AND "
          second.to_sql str, args
        end

        sql.should eq "a = $1 AND b = $2"
        args.should eq [Any.new(1), Any.new(2)]
      end
    end
  end
end
