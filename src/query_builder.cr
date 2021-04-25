require "./join_clause"
require "./query_record"
require "./dynamic_query"

module Interro
  abstract struct QueryBuilder(T)
    include Enumerable(T)
    include Iterable(T)

    macro table(name)
      def sql_table_name
        {{name.id.stringify}}
      end
    end

    def self.[](transaction : ::DB::Transaction) : self
      new.with_transaction(transaction)
    end

    protected property? distinct : String? = nil
    protected property join_clause = [] of JoinClause
    protected property where_clause : QueryExpression?
    protected property order_by_clause : OrderBy?
    protected property limit_clause : Int32? = nil
    protected property offset_clause : Int32? = nil
    protected property transaction : DB::Transaction? = nil
    protected property args : Array(Value) = Array(Value).new

    def first
      first? || raise UnexpectedEmptyResultSet.new("#{self} returned no results")
    end

    def first?
      limit(1).each { |obj| return obj }
      nil
    end

    def each
      ResultSetIterator(T).new(connection(CONFIG.read_db), to_sql, @args)
    end

    def each(& : T ->)
      args = @args
      if offset = offset_clause
        args += [offset.as(Value)]
      end
      if limit = limit_clause
        args += [limit.as(Value)]
      end

      connection(Interro::CONFIG.read_db).query_each to_sql, args: args do |rs|
        {% begin %}
          {% if T < Tuple %}
            yield({ {% for type, index in T.type_vars %} rs.read({{type}}) {% if index < T.type_vars.size - 1 %},{% end %} {% end %} })
          {% else %}
            yield rs.read(T)
          {% end %}
        {% end %}
      end
    end

    def to_json(json : JSON::Builder) : Nil
      json.array do
        each(&.to_json(json))
      end
    end

    def to_sql
      String.build do |str|
        to_sql str
      end
    end

    def |(other : self) : CompoundQuery
      CompoundQuery.new(self, "UNION", other, connection(CONFIG.read_db))
    end

    def &(other : self) : CompoundQuery
      CompoundQuery.new(self, "INTERSECTION", other, connection(CONFIG.read_db))
    end

    def -(other : self) : CompoundQuery
      CompoundQuery.new(self, "EXCEPT", other, connection(CONFIG.read_db))
    end

    def count : Int64
      scalar "count(*)", as: Int64
    end

    protected def find(**params) : T?
      query = where(**params).limit(1)

      connection(CONFIG.read_db).query_one? query.to_sql, args: query.args + [1], as: T
    end

    protected def inner_join(other_table, on condition : String, as relation = nil)
      new = dup
      new.join_clause << JoinClause.new(other_table, relation, condition)
      new
    end

    protected def left_join(other_table, on condition : String, as relation = nil)
      new = dup
      new.join_clause << JoinClause.new(other_table, relation, condition, join_type: "LEFT")
      new
    end

    protected def where(**params) : self
      where_clause = nil
      args = Array(Value).new(initial_capacity: params.size)
      # pp where_clause: where_clause, args: @args
      params.each_with_index(@args.size + 1) do |key, value, index|
        case value
        when Nil
          new_clause = QueryExpression.new(key.to_s, "IS", "NULL", [] of Value)
        when Array
          args << value
          new_clause = QueryExpression.new(key.to_s, "=", "ANY($#{index})", [value.as(Value)])
        else
          args << value
          new_clause = QueryExpression.new(key.to_s, "=", "$#{index}", [value.as(Value)])
        end

        if where_clause
          where_clause &= new_clause
        else
          where_clause = new_clause
        end
        # pp key: key, value: value, where: where_clause
      end

      if where_clause && (current_where_clause = @where_clause)
        # pp current_where_clause: current_where_clause, where_clause: where_clause, new: current_where_clause & where_clause
        where_clause = current_where_clause & where_clause
      end

      new = dup
      if where_clause
        new.where_clause = where_clause
        if @args.any?
          new.args = @args + args
        else # If the current array is empty, we don't need to concatenate
          new.args = args
        end
      end
      new
    end

    protected def where(table = sql_table_name, &block : QueryRecord -> QueryExpression) : self
      index = @args.size
      where_clause = yield(QueryRecord.new(table) { index += 1 })
      values = where_clause.values

      if current_where_clause = @where_clause
        where_clause = current_where_clause & where_clause
      end

      new = dup
      new.where_clause = where_clause
      new.args = @args + values
      new
    end

    protected def order_by(**params) : self
      order_by_clause = OrderBy.new(initial_capacity: params.size)
      params.each { |key, value| order_by_clause[key.to_s] = value }

      if current_order_clause = @order_by_clause
        order_by_clause = current_order_clause.merge(order_by_clause)
      end

      new = dup
      new.order_by_clause = order_by_clause
      new
    end

    protected def order_by(expression, direction) : self
      order_by_clause = OrderBy { expression => direction }

      if current_order_clause = @order_by_clause
        order_by_clause = current_order_clause.merge(order_by_clause)
      end

      new = dup
      new.order_by_clause = order_by_clause
      new
    end

    protected def limit(count : Int) : self
      new = dup
      new.limit_clause = count
      new
    end

    protected def offset(count : Int) : self
      new = dup
      new.offset_clause = count
      new
    end

    protected def distinct(on expression = "") : self
      new = dup
      new.distinct = expression
      new
    end

    protected def with_transaction(transaction : DB::Transaction) : self
      new = dup
      new.transaction = transaction
      new
    end

    protected def scalar(select expression : String, as type : U.class) : U forall U
      sql = String.build do |str|
        to_sql str do
          expression.to_s str
        end
      end
      connection(CONFIG.read_db).scalar(sql, args: @args).as(U)
      # SelectOperation(U).new.call(
      #   select: expression,
      #   from: sql_table_name,
      #   where: where_clause,
      #   order_by: order_by_clause,
      #   limit: limit_clause,
      #   args: args,
      # )
    end

    def any? : Bool
      !!connection(CONFIG.read_db).query_one? <<-SQL, args: @args, as: Int32
        SELECT 1 AS one
        FROM #{sql_table_name}
        WHERE #{@where_clause.try(&.to_sql)}
        LIMIT 1
      SQL
    end

    protected def insert(**params) : T
      CreateOperation(T).new(connection(CONFIG.write_db))
        .call(self, params)
    end

    protected def update(**params) : Array(T)
      UpdateOperation(T).new(connection(CONFIG.write_db))
        .call self,
          set: params,
          where: @where_clause
    end

    protected def delete
      DeleteOperation.new(connection(CONFIG.write_db))
        .call sql_table_name,
          where: @where_clause
    end

    # How to determine which columns get selected in these queries, the default
    # is the instance variables for the model that are not ignored with a
    # `DB::Field` annotation with `ignore: true`. To change this, override this
    # method.
    #
    # ```
    # struct MyQuery < Interro::QueryBuilder(MyModel)
    #   # ...
    #   protected def select_columns(io)
    #     io << "id, name, lower(email) AS email, foo, bar, baz"
    #   end
    # end
    # ```
    protected def select_columns(io) : Nil
      {% begin %}
        # Don't try to select columns the model has explicitly asked not to be
        # populated.
        {%
          ivars = T.instance_vars.reject do |ivar|
            ann = ivar.annotation(::DB::Field)
            ann && ann[:ignore]
          end
        %}

        {% for ivar, index in ivars %}
          {% ann = ivar.annotation(::DB::Field) %}

            {% if ann && (key = ann[:key]) %}
              io << sql_table_name << ".{{key.id}}"
            {% else %}
              io << sql_table_name << ".{{ivar.name}}"
            {% end %}

          {% if index < ivars.size - 1 %}
            io << ", "
          {% end %}
        {% end %}
      {% end %}
    end

    private def to_sql(str) : Nil
      to_sql(str) { select_columns str }
    end

    private def to_sql(str, &) : Nil
      str << "SELECT "
      str << "DISTINCT " if distinct?
      yield str
      str << " FROM " << sql_table_name << ' '

      @join_clause.each do |join|
        join.to_sql str
      end

      if where = @where_clause
        str << "WHERE "
        where.to_sql str
        str << ' '
      end

      if order = @order_by_clause
        str << "ORDER BY "
        order.each_with_index(1) do |(key, direction), index|
          str << key << ' ' << direction.upcase
          if index < order.size
            str << ", "
          end
        end
        str << ' '
      end

      placeholder = @args.size

      if offset = @offset_clause
        str << "OFFSET $" << (placeholder += 1) << ' '
      end

      if limit = @limit_clause
        str << "LIMIT $" << (placeholder += 1) << ' '
      end
    end

    private def connection(db)
      if transaction = @transaction
        connection = transaction.connection
      end
      connection || db
    end

    class ResultSetIterator(T)
      include Iterator(T)

      @result_set : DB::ResultSet

      def initialize(db : DB::Database | DB::Connection, query : String, args : Array(U)) forall U
        @result_set = db.query(query, args: args)
      end

      def next
        if @result_set.move_next
          {% if T < Tuple %}
            {
              {% for type in T.type_vars %}
                @result_set.read({{type}}),
              {% end %}
            }
          {% else %}
            T.new(@result_set)
          {% end %}
        else
          @result_set.close
          stop
        end
      end

      def finalize
        @result_set.close
      end
    end

    struct CompoundQuery(T)
      include Enumerable(T)

      def initialize(
        @lhs : QueryBuilder(T),
        @combinator : String,
        @rhs : QueryBuilder(T),
        @connection : ::DB::Database | ::DB::Connection
      )
      end

      def each(& : T ->)
        # pp lhs: @lhs.args, rhs: @rhs.args
        @connection.query_each to_sql, args: @lhs.args + @rhs.args do |rs|
          yield T.new(rs)
        end
      end

      def to_sql
        lhs = @lhs.to_sql
        lhs_arg_count = @lhs.@args.size
        rhs = @rhs
          .to_sql
          .gsub(/\$(\d+)/) { |match| "$#{match[1].to_i + lhs_arg_count}" }

        <<-SQL
          #{lhs}

          #{@combinator}

          #{rhs}
        SQL
      end
    end
  end
end
