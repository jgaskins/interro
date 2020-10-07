module Interro
  struct JoinClause
    getter other_table : String, relation : String?, condition : String

    # Represent a SQL JOIN with the given arguments.
    #
    # ```
    # JoinClause.new("my_table", as: "m", on: "m.foo_id = foo.id")
    # ```
    def initialize(@other_table, as @relation, on @condition)
    end

    # Output this JOIN clause to the given `IO` as SQL.
    def to_sql(io)
      io << " INNER JOIN " << other_table
      if relation
        io << " AS " << relation
      end
      io << " ON " << condition << ' '
    end

    # :nodoc:
    def to_sql
      String.build { |str| to_sql str }
    end
  end
end
