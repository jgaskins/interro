module Interro
  struct JoinClause
    getter other_table : String
    getter relation : String?
    getter condition : String
    getter join_type : String

    # Represent a SQL JOIN with the given arguments.
    #
    # ```
    # JoinClause.new("my_table", as: "m", on: "m.foo_id = foo.id")
    # ```
    def initialize(@other_table, as @relation, on @condition, @join_type = "INNER")
    end

    # Output this JOIN clause to the given `IO` as SQL.
    def to_sql(io)
      io << ' ' << @join_type << " JOIN "
      other_table.inspect io
      if relation
        io << " AS "
        relation.inspect io
      end
      io << " ON " << condition << ' '
    end

    # :nodoc:
    def to_sql
      String.build { |str| to_sql str }
    end
  end
end
