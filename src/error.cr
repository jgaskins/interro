module Interro
  class Error < ::Exception
  end

  class NotFound < Error
  end

  class UnexpectedEmptyResultSet < Error
  end
end
