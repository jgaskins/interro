require "db"
require "./join_clause"
require "./query_record"
require "./dynamic_query"
require "./validations"

module Interro
  alias OrderBy = Hash(String, String)

  # Defining `QueryBuilder` objects is a way to create composable queries. For
  # example, if you have the following `Model` and `QueryBuilder`:
  #
  # ```
  # struct User
  #   include Interro::Model
  #
  #   getter id : UUID
  #   getter name : String
  #   getter email : String
  #   getter team_id : UUID?
  #   getter role : Role
  #
  #   enum Role
  #     Member
  #     TeamAdmin
  #     SiteAdmin
  #   end
  # end
  #
  # struct UserQuery < Interro::QueryBuilder(User)
  #   table "users"
  #
  #   def find(id : UUID)
  #     where(id: id).first?
  #   end
  #
  #   def on_team(team : Team)
  #     where team_id: team.id
  #   end
  #
  #   def with_role(role : User::Role)
  #     # We pass `role.value` to the SQL query since it's stored in `INTEGER`
  #     # column type.
  #     where role: role.value
  #   end
  #
  #   def sorted_by_name(direction : Interro::QueryBuilder::OrderByDirection = :asc)
  #     order_by name: direction
  #   end
  # end
  # ```
  #
  # Then you can find all team admins like this:
  #
  # ```
  # users = UserQuery.new
  #   .on_team(team)
  #   .with_role(:team_admin)
  #   .sorted_by_name
  # ```
  #
  # You can insert records with the `insert` method:
  #
  # ```
  # struct UserQuery < Interro::QueryBuilder(User)
  #   # ...
  #   def create(name : String, email : String, team : Team, role : User::Role)
  #     # This generates the following SQL:
  #     #   INSERT INTO users (name, email, team_id, role) VALUES ($1, $2, $3, $4)
  #     # And it passes these args to the parameterized query:
  #     #   [name, email, team.id, role.value]
  #     insert(
  #       name: name,
  #       email: email,
  #       team_id: team.id,
  #       role: role.value,
  #     )
  #   end
  # end
  # ```
  #
  # You can also use `Interro::Validations::Result` objects to validate the
  # inputs before saving them to the DB. Since `Interro::QueryBuilder` includes
  # the `Interro::Validations` mixin, you can simply refer to it as `Result`.
  #
  # ```
  # struct UserQuery < Interro::QueryBuilder(User)
  #   # ...
  #   def create(name : String, email : String, team : Team, role : User::Role)
  #     Result(User).new
  #       .validate_presence(name: name, email: email)
  #       .validate_uniqueness("email") { where(email: email).any? }
  #       .valid do
  #         insert(
  #           name: name,
  #           email: email,
  #           team_id: team.id,
  #           role: role.value,
  #         )
  #       end
  #   end
  # end
  # ```
  abstract struct QueryBuilder(T)
    include Enumerable(T)
    include Iterable(T)
    include Validations

    macro table(name, as table_alias = nil)
      def sql_table_name
        {{name}}
      end

      def sql_table_alias
        {{table_alias || name}}
      end

      def model_table_mappings
        { T => {{name}} }
      end
    end

    macro from(name, *joins)
      def sql_table_name
        {{name}}
      end

      def sql_table_alias
        sql_table_name
      end

      def model_table_mappings
        {
          {% for type_var, index in T.type_vars %}
            {% if index == 0 %}
              {{type_var}} => {{name}},
            {% else %}
              {% args = joins[index - 1].named_args %}

              {{type_var}} => {{(args.find { |arg| arg.name == "as".id } || args.first).value}},
            {% end %}
          {% end %}
        }
      end

      def self.new
        super
          {% for join in joins %}
            .{{join}}
          {% end %}
      end
    end

    struct InnerJoin
    end

    def self.[](transaction : ::DB::Transaction) : self
      new.with_transaction(transaction)
    end

    def self.new(transaction_owner : ::Interro::QueryBuilder)
      if txn = transaction_owner.transaction
        self[txn]
      else
        new
      end
    end

    protected property? distinct : Array(String)? = nil
    protected property join_clause = [] of JoinClause
    protected property where_clause : QueryExpression?
    protected property order_by_clause : OrderBy?
    protected property limit_clause : Int32? = nil
    protected property offset_clause : Int32? = nil
    protected property transaction : ::DB::Transaction? = nil
    protected property args : Array(Any) = Array(Any).new
    protected property? for_update = false
    protected property? skip_locked = false

    def first
      first? || raise UnexpectedEmptyResultSet.new("#{self} returned no results")
    end

    def first(count : Int)
      limit count
    end

    def first?
      limit(1).each { |obj| return obj }
      nil
    end

    def each
      ResultSetIterator(T).new(
        db: connection(CONFIG.read_db),
        query: to_sql,
        args: @args,
      )
    end

    def each(& : T ->)
      args = @args
      if offset = offset_clause
        args += [offset] of Interro::Value
      end
      if limit = limit_clause
        args += [limit] of Interro::Value
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

    def merge(other : QueryBuilder) : self
      new = dup
      new.join_clause += other.join_clause
      if (my_where = new.where_clause) && (their_where = other.where_clause)
        new.where_clause = my_where & their_where
      else
        new.where_clause ||= other.where_clause
      end
      new.args += other.args
      if (my_order = new.order_by_clause) && (their_order = other.order_by_clause)
        new.order_by_clause = my_order.merge(their_order)
      else
        new.order_by_clause ||= other.order_by_clause
      end
      new
    end

    def to_json(json : JSON::Builder) : Nil
      json.array do
        each(&.to_json(json))
      end
    end

    def to_sql : String
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
      new.join_clause = join_clause.dup
      new.join_clause << JoinClause.new(other_table, relation, condition)
      new
    end

    protected def left_join(other_table, on condition : String, as relation = nil)
      new = dup
      new.join_clause = join_clause.dup
      new.join_clause << JoinClause.new(other_table, relation, condition, join_type: "LEFT")
      new
    end

    protected def where(**params : Value | Any | Array) : self
      where_clause = nil
      args = Array(Any).new(initial_capacity: params.size)
      params.each_with_index(@args.size + 1) do |key, value, index|
        case value
        when Nil
          new_clause = QueryExpression.new(key.to_s, "IS", "NULL", [] of Any)
        when Array
          any = Any.new(value)
          args << any
          new_clause = QueryExpression.new(key.to_s, "=", "ANY($#{index})", [any])
        else
          args << Any.new(value)
          new_clause = QueryExpression.new(key.to_s, "=", "$#{index}", [Any.new(value)])
        end

        if where_clause
          where_clause &= new_clause
        else
          where_clause = new_clause
        end
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

    protected def where(table = sql_table_alias, &block : QueryRecord -> QueryExpression) : self
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

    protected def where(lhs : String, comparator : String, rhs : String, values : Array(Value) = [] of Value) : self
      # Must upcast all values in the array to Interro::Value objects
      values = values.map { |value| Any.new(value) }

      # Translate $1, $2, ... $n to the numbers they should be.
      arg_count = @args.size
      lhs = lhs.gsub /\$(\d+)/ do |match|
        index = match[1].to_i
        "$#{arg_count + index}"
      end
      rhs = rhs.gsub /\$(\d+)/ do |match|
        index = match[1].to_i
        "$#{arg_count + index}"
      end

      where_clause = Interro::QueryExpression.new(lhs, comparator, rhs, values)

      if current_where_clause = @where_clause
        where_clause = current_where_clause & where_clause
      end

      new = dup
      new.where_clause = where_clause
      if @args.any?
        new.args = @args + values
      else # If the current array is empty, we don't need to concatenate
        new.args = values
      end
      new
    end

    protected def where(expression : String, values : Array(Value) = [] of Value) : self
      # Must upcast all values in the array to Interro::Value objects
      values = values.map { |value| Any.new(value) }

      # Translate $1, $2, ... $n to the numbers they should be.
      arg_count = @args.size
      expression = expression.gsub /\$(\d+)/ do |match|
        index = match[1].to_i
        "$#{arg_count + index}"
      end
      where_clause = Interro::QueryExpression.new(expression, values)

      if current_where_clause = @where_clause
        where_clause = current_where_clause & where_clause
      end

      new = dup
      new.where_clause = where_clause
      if @args.any?
        new.args = @args + values
      else # If the current array is empty, we don't need to concatenate
        new.args = values
      end
      new
    end

    protected def order_by(**params : OrderByDirection) : self
      order_by(**params.transform_values(&.to_s))
    end

    enum OrderByDirection
      ASC
      DESC
      ASC_NULLS_FIRST
      ASC_NULLS_LAST
      DESC_NULLS_FIRST
      DESC_NULLS_LAST

      def to_s
        {% for member in @type.constants %}
          if value == {{@type.constant(member)}}
            return {{member.stringify.tr("_", " ")}}
          end

          value.to_s
        {% end %}
      end
    end

    protected def order_by(**params : String) : self
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
      order_by_clause = OrderBy{expression => direction}

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

    protected def distinct(on expressions : Enumerable(String)) : self
      new = dup
      new.distinct = expressions.to_a
      new
    end

    protected def distinct(on expression : String = "*") : self
      distinct({expression})
    end

    protected def with_transaction(transaction : DB::Transaction) : self
      new = dup
      new.transaction = transaction
      new
    end

    protected def scalar(select expression : String, as type : U.class) : U forall U
      args = @args
      if offset = offset_clause
        args += [offset] of Interro::Value
      end
      if limit = limit_clause
        args += [limit] of Interro::Value
      end

      sql = String.build do |str|
        to_sql str do
          expression.to_s str
        end
      end
      connection(CONFIG.read_db).scalar(sql, args: @args).as(U)
    end

    protected def for_update
      new = dup
      new.for_update = true
      new
    end

    protected def skip_locked
      new = dup
      new.skip_locked = true
      new
    end

    def any? : Bool
      !none?
    end

    def none? : Bool
      sql = String.build do |str|
        str << "SELECT 1 AS one"
        str << " FROM " << sql_table_name
        if join = join_clause
          join.each(&.to_sql(str))
        end

        str << " WHERE " << @where_clause.try(&.to_sql)
        str << " LIMIT 1"
      end

      !connection(CONFIG.read_db).query_one? sql, args: @args, as: Int32
    end

    protected def insert(**values) : T
      insert values: values, on_conflict: nil
    end

    protected def insert(values : NamedTuple) : T
      insert values: values, on_conflict: nil
    end

    protected def insert(values : NamedTuple, on_conflict : ConflictHandler?) : T
      CreateOperation(T).new(connection(CONFIG.write_db))
        .call(self, values, on_conflict: on_conflict)
    end

    protected def update(**params) : Array(T)
      update params
    end

    protected def update(*expressions) : Array(T)
      UpdateOperation(T).new(connection(CONFIG.write_db))
        .call self,
          set: expressions.join(", "),
          where: @where_clause
    end

    protected def update(set clause : String, args : Array)
      clause = clause.gsub(/\$(\d+)\b/) do |match|
        "$#{$1.to_i + @args.size - 1}"
      end

      UpdateOperation(T).new(connection(CONFIG.write_db))
        .call self,
          set: clause,
          args: args,
          where: @where_clause
    end

    protected def update(params : NamedTuple) : Array(T)
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

    protected def write_db
      connection(CONFIG.write_db)
    end

    protected def read_db
      connection(CONFIG.read_db)
    end

    protected def transaction(&)
      Interro.transaction do |txn|
        old_txn = @transaction
        @transaction = txn
        yield txn
      ensure
        @transaction = old_txn
      end
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
    protected def select_columns(io : IO) : Nil
      {% if T < Tuple %}
        {% for type, index in T.type_vars %}
          select_columns_for_model {{type}}, io
          {% if index < T.type_vars.size - 1 %}
            io << ", "
          {% end %}
        {% end %}
      {% else %}
        select_columns_for_model T, io
      {% end %}
    end

    protected def select_columns_for_model(model : U.class, io) : Nil forall U
      model_table_mappings = self.model_table_mappings

      {% begin %}
        # Don't try to select columns the model has explicitly asked not to be
        # populated.
        {%
          ivars = U.instance_vars.reject do |ivar|
            ann = ivar.annotation(::Interro::Field) || ivar.annotation(::DB::Field)
            ann && ann[:ignore]
          end
        %}

        {% for ivar, index in ivars %}
          {% ann = ivar.annotation(::Interro::Field) || ivar.annotation(::DB::Field) %}

            {% if ann && (key = ann[:key]) %}
              io << model_table_mappings[model] << ".{{key.id}}"
            {% elsif ann && ann[:select] %}
              io << {{ann[:select]}} << " AS {{(ann[:as] || ivar).id}}"
            {% else %}
              io << model_table_mappings[model] << ".{{ivar.name}}"
            {% end %}

          {% if index < ivars.size - 1 %}
            io << ", "
          {% end %}
        {% end %}
      {% end %}
    end

    protected def select_columns
      String.build { |str| select_columns str }
    end

    protected def select_columns(relation_name : String? = nil)
      relation_name ||= model_table_mappings[T]
      String.build { |str| select_columns str, relation_name }
    end

    protected def select_columns(io : IO, relation_name = model_table_mappings[T]) : Nil
      {% if T < Tuple %}
        {% for type, index in T.type_vars %}
          select_columns_for_model {{type}}, io, relation_name
          {% if index < T.type_vars.size - 1 %}
            io << ", "
          {% end %}
        {% end %}
      {% else %}
        select_columns_for_model T, io, relation_name
      {% end %}
    end

    protected def select_columns_for_model(model : U.class, io : IO, relation_name = model_table_mappings[model]) : Nil forall U
      {% begin %}
        # Don't try to select columns the model has explicitly asked not to be
        # populated.
        {%
          ivars = U.instance_vars.reject do |ivar|
            ann = ivar.annotation(::Interro::Field) || ivar.annotation(::DB::Field)
            ann && ann[:ignore]
          end
        %}

        {% for ivar, index in ivars %}
          {% ann = ivar.annotation(::Interro::Field) || ivar.annotation(::DB::Field) %}

          {% if ann && (key = ann[:key]) %}
            relation_name.inspect io
            io << ".{{key.id}}"
          {% elsif ann && ann[:select] %}
            {{ann[:select]}}.to_s io
            io << " AS {{(ann[:as] || ivar).id}}"
          {% else %}
            relation_name.inspect io
            io << %{."{{ivar.name}}"}
          {% end %}

          {% if index < ivars.size - 1 %}
            io << ", "
          {% end %}
        {% end %}
      {% end %}
    end

    protected def to_sql(str) : Nil
      to_sql(str) { select_columns str }
    end

    private def to_sql(str, &) : Nil
      str << "SELECT "
      if distinct_subclause = self.distinct?
        # If you provide DISTINCT and an ORDER BY, the ORDER BY clause must also
        # appear in the DISTINCT subclause.
        if order_by = @order_by_clause
          distinct_subclause += order_by.keys
        end

        str << "DISTINCT "
        unless distinct_subclause.empty?
          str << "ON ("
          distinct_subclause.each_with_index 1 do |expression, index|
            str << expression
            if index < distinct_subclause.size
              str << ", "
            end
          end
          str << ") "
        end
      end

      # SELECT columns
      yield str

      str << " FROM " << sql_table_name
      if sql_table_name != sql_table_alias
        str << " AS " << sql_table_alias
      end

      @join_clause.each do |join|
        join.to_sql str
      end

      if where = @where_clause
        str << " WHERE "
        where.to_sql str
      end

      if order = @order_by_clause
        str << " ORDER BY "
        order.each_with_index(1) do |(key, direction), index|
          str << key << ' ' << direction.upcase
          if index < order.size
            str << ", "
          end
        end
      end

      placeholder = @args.size

      if offset = @offset_clause
        str << " OFFSET $" << (placeholder += 1)
      end

      if limit = @limit_clause
        str << " LIMIT $" << (placeholder += 1)
      end

      if for_update?
        str << " FOR UPDATE"
      end

      if skip_locked?
        str << " SKIP LOCKED"
      end
    end

    private def connection(db)
      @transaction.try(&.connection) || db
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

      protected property limit : Int64? = nil

      def initialize(
        @lhs : QueryBuilder(T),
        @combinator : String,
        @rhs : QueryBuilder(T),
        @connection : ::DB::Database | ::DB::Connection,
      )
      end

      def each(& : T ->)
        args = @lhs.args + @rhs.args
        if limit
          args << Any.new(limit)
        end

        @connection.query_each to_sql, args: args do |rs|
          yield T.new(rs)
        end
      end

      def first(count : Int)
        new = dup
        new.limit = count
        new
      end

      def to_sql
        lhs = @lhs.to_sql
        lhs_arg_count = @lhs.@args.size
        rhs = @rhs
          .to_sql
          .gsub(/\$(\d+)/) { |match| "$#{match[1].to_i + lhs_arg_count}" }

        arg_count = lhs_arg_count + @rhs.@args.size

        String.build do |str|
          str << lhs
          str << ' ' << @combinator << ' '
          str << rhs
          if @limit
            str << "LIMIT $" << (arg_count += 1)
          end
        end
      end
    end
  end
end

struct NamedTuple
  def transform_values(&)
    {% begin %}
      {
        {% for key, value in T %}
          {{key.stringify}}: yield(self[:{{key.stringify}}]),
        {% end %}
      }
    {% end %}
  end
end
