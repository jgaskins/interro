require "./join_clause"
require "./query_record"

module Interro
  abstract struct QueryBuilder(T)
    include Enumerable(T)
    include Iterable(T)

    macro table(name)
      TABLE_NAME = {{name.id.stringify}}

      def sql_table_name
        TABLE_NAME
      end
    end

    def self.[](transaction : ::DB::Transaction) : self
      new.with_transaction(transaction)
    end

    protected property? distinct : String? = nil
    protected property join_clause = [] of JoinClause
    protected property where_clause : QueryExpression?
    protected property order_by_clause : OrderBy?
    protected property limit_clause = false
    protected property transaction : DB::Transaction? = nil
    protected property args : Array(Value)

    def initialize(
      where @where_clause = nil,
      order_by @order_by_clause = nil,
      limit @limit_clause = false,
      @args = Array(Value).new
    )
    end

    def first?
      limit(1)
        .to_a
        .first?
    end

    def each
      ResultSetIterator(T).new(connection(CONFIG.read_db), to_sql, @args)
    end

    def each(&)
      connection(CONFIG.read_db).query_each to_sql, args: @args do |rs|
        yield T.new(rs)
      end
    end

    def to_a
      connection(CONFIG.read_db).query_all to_sql, args: @args, as: T
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

    protected def find(**params) : T?
      query = where(**params).limit(1)

      connection(CONFIG.read_db).query_one? query.to_sql, args: query.args, as: T
    end

    protected def inner_join(other_table, on condition : String, as relation = nil)
      new = dup
      new.join_clause << JoinClause.new(other_table, relation, condition)
      new
    end

    protected def where(**params : Value) : self
      where_clause = nil
      args = Array(Value).new(initial_capacity: params.size)
      params.each_with_index do |key, value|
        case value
        when Array
          args << value.as(Value)
          new_clause = QueryExpression.new(key.to_s, "=", "ANY($#{@args.size + args.size})", args)
        else
          args << value
          new_clause = QueryExpression.new(key.to_s, "=", "$#{@args.size + args.size}", args)
        end

        if where_clause
          where_clause &= new_clause
        else
          where_clause = new_clause
        end
      end

      if where_clause && (current_where_clause = @where_clause)
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

    protected def limit(count : Int) : self
      new = dup
      new.limit_clause = true
      new.args += [count.as(Value)].as(Array(Value))
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

    protected def insert(**params) : T
      CreateOperation(T).new(connection(CONFIG.write_db))
        .call(sql_table_name, params)
    end

    protected def update(**params) : Array(T)
      UpdateOperation(T).new(connection(CONFIG.write_db))
        .call sql_table_name,
          set: params,
          where: @where_clause
    end

    protected def delete
      DeleteOperation.new(connection(CONFIG.write_db))
        .call sql_table_name,
          where: @where_clause
    end

    # How to determine which methods get selected in these queries, the default
    # is the instance variables for the model that are not ignored with a
    # `DB::Field` annotation with `ignore: true`. To change this, override this
    # method.
    #
    # ```
    # struct MyQuery < Interro::QueryBuilder(MyModel)
    #   # ...
    #   private def select_columns(io)
    #     io << "id, name, lower(email) AS email, foo, bar, baz"
    #   end
    # end
    # ```
    private def select_columns(io) : Nil
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

      if limit = @limit_clause
        str << "LIMIT $#{@args.size}" << ' '
      end
    end

    private def connection(db)
      if transaction = @transaction
        connection = transaction.connection
      end
      connection || db
    end

    struct ResultSetIterator(T)
      include Iterator(T)

      @result_set : DB::ResultSet

      def initialize(db : DB::Database | DB::Connection, query : String, args : Array(U)) forall U
        @result_set = db.query(query, args: args)
      end

      def next
        if @result_set.move_next
          T.new(@result_set)
        else
          @result_set.close
          stop
        end
      end
    end
  end
end
