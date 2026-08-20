require "./types" # for Interro::Value

module Interro
  struct QueryExpression
    # Each part is either raw SQL text (`String`) or a value to bind (`Any`).
    alias Part = String | Any

    # The expression is just its parts: SQL text interleaved with the values to bind.
    # Placeholder numbers are assigned when the query is rendered in `#to_sql`.
    getter parts : Array(Part)

    def self.new(*parts : Part) : self
      array = Array(Part).new(parts.size)
      parts.each { |part| array << part }
      new array
    end

    def initialize(@parts)
    end

    # The values this fragment binds, in the order `to_sql` renders them.
    def values : Array(Any)
      @parts.compact_map(&.as?(Any))
    end

    def &(other : self) : self
      combine "AND", other
    end

    def |(other : self) : self
      combine "OR", other
    end

    def to_sql(io : IO, args : Array(Any)) : Nil
      @parts.each do |part|
        case part
        in String
          io << part
        in Any
          args << part
          io << '$' << args.size
        end
      end
    end

    # Render this expression, numbering placeholders from `$1`.
    # Intended for inspecting an expression; one that is about to executed should use `to_sql(io, args)` so that placeholders are numbered relative to the built args array.
    def to_sql(io : IO) : Nil
      to_sql io, [] of Any
    end

    def to_sql
      String.build { |str| to_sql str }
    end

    private def combine(operator : String, other : self) : self
      parts = Array(Part).new(@parts.size + other.parts.size + 3)
      parts << "("
      parts.concat @parts
      parts << ") #{operator} ("
      parts.concat other.parts
      parts << ")"
      self.class.new parts
    end

    # Matches either a $n placeholder, capturing n, or a whole single-quoted SQL string literal ('' being an escaped quote), with no capture.
    private PLACEHOLDER_OR_LITERAL = /\$(\d+)|'[^']*(?:''[^']*)*'/

    # Parses a raw SQL fragment, resolving each `$n` placeholder to the value it references: `$1` is `values[0]`, and so on.
    # Raises `ArgumentError` if a placeholder references no value.
    def self.parse(fragment : String, values : Array(Any)) : self
      parts = [] of Part
      cursor = 0

      # String literals are matched so that a $n inside one cannot match as a placeholder.
      fragment.scan(PLACEHOLDER_OR_LITERAL) do |match|
        # A literal match has no capture.
        next unless number = match[1]?

        index = number.to_i
        unless index.in?(1..values.size)
          raise ArgumentError.new("SQL fragment #{fragment.inspect} references $#{number}, but only #{values.size} values were provided")
        end

        prefix = fragment[cursor...match.begin]
        parts << prefix unless prefix.empty?
        parts << values[index - 1]
        cursor = match.end
      end

      suffix = fragment[cursor..]
      parts << suffix unless suffix.empty?

      new(parts)
    end
  end
end
