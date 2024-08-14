require "./spec_helper"

require "../src/join_clause"

module Interro
  describe JoinClause do
    it "does a thing" do
      join = JoinClause.new("my_table", as: "m", on: "m.foo_id = foo.id")

      join.to_sql.strip.should eq %{INNER JOIN "my_table" AS "m" ON m.foo_id = foo.id}
    end
  end
end
