require "./query_builder"

module Interro
  abstract struct QueryBuilder(T)
    protected def fetch(columns : String, as type : U, delegate : V = self) forall U, V
      {% begin %}
        DynamicQuery(
          {% if U < Tuple %}
            { {{U.type_vars.map(&.instance).join(", ").id}} },
          {% else %}
            {{U.instance}},
          {% end %}
          V
        ).new(
          select: columns,
          from: sql_table_name,
          join: @join_clause,
          where: @where_clause,
          order_by: @order_by_clause,
          offset: @offset_clause,
          limit: @limit_clause,
          args: @args,
          transaction: transaction,
          delegate: delegate,
        )
      {% end %}
    end
  end

  struct DynamicQuery(T, U) < QueryBuilder(T)
    protected property select_columns : String
    getter sql_table_name : String

    def initialize(
      select @select_columns,
      from @sql_table_name,
      join @join_clause,
      where @where_clause,
      order_by @order_by_clause,
      offset @offset_clause,
      limit @limit_clause,
      @args,
      @transaction,
      @delegate : U
    )
    end

    delegate sql_table_alias, to: @delegate

    macro method_missing(call)
      @delegate.join_clause = @join_clause
      @delegate.where_clause = @where_clause
      @delegate.order_by_clause = @order_by_clause
      @delegate.offset_clause = @offset_clause
      @delegate.limit_clause = @limit_clause
      @delegate.args = @args
      @delegate.transaction = @transaction
      %new_query = @delegate.{{call}}
      
      {{@type.id}}.new(
        @select_columns,
        from: sql_table_name,
        join: %new_query.join_clause,
        where: %new_query.where_clause,
        order_by: %new_query.order_by_clause,
        offset: %new_query.offset_clause,
        limit: %new_query.limit_clause,
        args: %new_query.args,
        transaction: %new_query.transaction,
        delegate: @delegate,
      )
    end

    protected def select_columns(io) : Nil
      io << @select_columns
    end
  end
end
