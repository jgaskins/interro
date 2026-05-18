require "db/transaction"

module Interro
  class Transaction < DB::Transaction
    private getter original : DB::Transaction
    private getter after_commit : Array(Proc(Nil)) { [] of Proc(Nil) }
    private getter after_rollback : Array(Proc(Nil)) { [] of Proc(Nil) }
    private getter state : State = :open

    def initialize(@original)
    end

    def after_commit(&block)
      after_commit << block
    end

    def after_rollback(&block)
      after_rollback << block
    end

    def commit
      ensure_open!

      @original.commit
      @state = :committed
      run_after_commit!
    end

    def rollback
      ensure_open!

      @original.rollback
      @state = :rolled_back
      run_after_rollback!
    end

    # :nodoc:
    def begin_transaction : DB::Transaction
      @original
    end

    # :nodoc:
    def connection : DB::Connection
      @original.connection
    end

    # :nodoc:
    def do_close
      @original.do_close
    end

    # :nodoc:
    def release_from_nested_transaction
      @original.release_from_nested_transaction
    end

    private def run_after_commit!
      @after_commit.try &.each &.call
    end

    private def run_after_rollback!
      @after_rollback.try &.each &.call
    end

    private def ensure_open!
      if state.closed?
        raise AlreadyClosed.new("Transaction is already closed")
      end
    end

    class AlreadyClosed < Error
    end

    private enum State
      Open
      Committed
      RolledBack

      def closed?
        committed? || rolled_back?
      end
    end
  end
end
