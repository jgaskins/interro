require "./spec_helper"

require "../src/query_value"

module Interro
  describe QueryValue do
    value = QueryValue.new("my_value", 1)

    it "checks equal" do
      (value == 42).to_sql.should eq "my_value = $1"
    end

    {% for name, operator in {
                               "not equal":             "!=",
                               "less than":             "<",
                               "less than or equal":    "<=",
                               "greater than":          ">",
                               "greater than or equal": ">=",
                             } %}
      it "checks {{name}}" do
        (value {{operator.id}} 42).to_sql.should eq "my_value {{operator.id}} $1"
      end
    {% end %}
  end
end
